from __future__ import annotations

import json
import logging
import mimetypes
import re
from datetime import datetime
import threading
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ml_toolbox.protocol.decorators import NODE_REGISTRY
from ml_toolbox.config import DATA_DIR
from ml_toolbox.services import file_store, store
from ml_toolbox.services.executor import (
    PipelineExecutor,
    get_active_executor,
    remove_active_executor,
    try_set_active_executor,
)
from ml_toolbox.services.file_store import _validate_path_id
from ml_toolbox.routers.ws import broadcast_sync

router = APIRouter(prefix="/api/pipelines")


# ── Request / Response Models ────────────────────────────────────


class CreatePipelineRequest(BaseModel):
    name: str


class PipelineSummary(BaseModel):
    id: str
    name: str


class SettingsUpdate(BaseModel):
    model_config = {"extra": "allow"}


class Position(BaseModel):
    x: float
    y: float


class AddNodeRequest(BaseModel):
    type: str
    position: Position
    params: dict[str, Any] | None = None
    code: str | None = None
    name: str | None = None


class UpdateNodeRequest(BaseModel):
    params: dict[str, Any] | None = None
    code: str | None = None
    position: Position | None = None
    name: str | None = None


class AddEdgeRequest(BaseModel):
    source: str
    source_port: str
    target: str
    target_port: str
    condition: str | None = None


class UpdateEdgeRequest(BaseModel):
    condition: str | None = None


# ── Cycle Detection ──────────────────────────────────────────────


def would_create_cycle(
    pipeline_data: dict, source_id: str, target_id: str
) -> bool:
    """Return True if adding an edge source→target would create a cycle.

    DFS from target: if we can reach source through downstream edges,
    adding this edge creates a cycle.
    """
    if source_id == target_id:
        return True

    # Build adjacency list from existing edges
    adj: dict[str, list[str]] = defaultdict(list)
    for edge in pipeline_data.get("edges", []):
        adj[edge["source"]].append(edge["target"])

    # DFS from target node — can we reach source?
    visited: set[str] = set()
    stack = [target_id]
    while stack:
        node = stack.pop()
        if node == source_id:
            return True
        if node in visited:
            continue
        visited.add(node)
        stack.extend(adj[node])

    return False


@router.post("", status_code=201)
async def create_pipeline(body: CreatePipelineRequest) -> PipelineSummary:
    pipeline_id = uuid.uuid4().hex
    data = {
        "id": pipeline_id,
        "name": body.name,
        "settings": {"keep_outputs": True},
        "nodes": [],
        "edges": [],
    }
    store.save(pipeline_id, data)
    return PipelineSummary(id=pipeline_id, name=body.name)


@router.get("")
async def list_pipelines() -> list[dict]:
    pipelines = store.list_all()
    return [
        {
            "id": p["id"],
            "name": p["name"],
            "node_count": len(p.get("nodes", [])),
        }
        for p in pipelines
    ]


@router.get("/{pipeline_id}")
async def get_pipeline(pipeline_id: str) -> dict:
    try:
        data = store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Backfill seq numbers for existing nodes that lack them
    before = data.get("_next_node_seq")
    _ensure_node_seqs(data)
    if data.get("_next_node_seq") != before:
        store.save(pipeline_id, data)

    return data


@router.put("/{pipeline_id}")
async def update_pipeline(pipeline_id: str, body: dict) -> dict:
    if not store.exists(pipeline_id):
        raise HTTPException(status_code=404, detail="Pipeline not found")
    body["id"] = pipeline_id

    # Backfill node fields that frontend may not include (code, inputs, outputs).
    # First try existing saved data, then fall back to NODE_REGISTRY defaults.
    existing = store.load(pipeline_id)
    existing_nodes = {n["id"]: n for n in existing.get("nodes", [])}
    for node in body.get("nodes", []):
        prev = existing_nodes.get(node["id"], {})
        node_type = node.get("type", prev.get("type", ""))
        template = NODE_REGISTRY.get(node_type, {})
        for field, registry_key in [
            ("code", "default_code"),
            ("inputs", "inputs"),
            ("outputs", "outputs"),
        ]:
            if field not in node:
                node[field] = prev.get(field) or template.get(registry_key, [])

    _ensure_node_seqs(body)
    store.save(pipeline_id, body)
    return body


@router.delete("/{pipeline_id}", status_code=204)
async def delete_pipeline(pipeline_id: str) -> None:
    if not store.exists(pipeline_id):
        raise HTTPException(status_code=404, detail="Pipeline not found")
    store.delete(pipeline_id)


@router.post("/{pipeline_id}/duplicate", status_code=201)
async def duplicate_pipeline(pipeline_id: str) -> PipelineSummary:
    try:
        original = store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    new_id = uuid.uuid4().hex
    new_name = original["name"] + " (copy)"
    clone = {**original, "id": new_id, "name": new_name}
    store.save(new_id, clone)
    return PipelineSummary(id=new_id, name=new_name)


@router.patch("/{pipeline_id}/settings")
async def update_settings(pipeline_id: str, body: SettingsUpdate) -> dict:
    try:
        data = store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    data.setdefault("settings", {}).update(body.model_dump())
    store.save(pipeline_id, data)
    return data["settings"]


# ── Helpers ──────────────────────────────────────────────────────


def _ensure_node_seqs(data: dict) -> None:
    """Backfill sequential numbers for nodes that don't have one."""
    next_seq = data.get("_next_node_seq", 1)
    for node in data.get("nodes", []):
        if "seq" not in node:
            node["seq"] = next_seq
            next_seq += 1
    data["_next_node_seq"] = next_seq


def _load_pipeline(pipeline_id: str) -> dict:
    try:
        return store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")


# ── Node Operations ──────────────────────────────────────────────


@router.post("/{pipeline_id}/nodes", status_code=201)
async def add_node(pipeline_id: str, body: AddNodeRequest) -> dict:
    data = _load_pipeline(pipeline_id)

    if body.type not in NODE_REGISTRY:
        raise HTTPException(status_code=400, detail=f"Unknown node type: {body.type}")

    template = NODE_REGISTRY[body.type]
    node_id = uuid.uuid4().hex

    # Assign sequential number
    _ensure_node_seqs(data)
    next_seq = data.get("_next_node_seq", 1)

    node: dict[str, Any] = {
        "id": node_id,
        "seq": next_seq,
        "type": body.type,
        "position": body.position.model_dump(),
        "params": body.params if body.params is not None else list(template["params"]),
        "code": body.code if body.code is not None else template["default_code"],
        "inputs": list(template["inputs"]),
        "outputs": list(template["outputs"]),
    }
    if body.name:
        node["name"] = body.name

    data["_next_node_seq"] = next_seq + 1
    data["nodes"].append(node)
    store.save(pipeline_id, data)
    return node


@router.delete("/{pipeline_id}/nodes/{node_id}", status_code=204)
async def delete_node(pipeline_id: str, node_id: str) -> None:
    data = _load_pipeline(pipeline_id)

    original_count = len(data["nodes"])
    data["nodes"] = [n for n in data["nodes"] if n["id"] != node_id]

    if len(data["nodes"]) == original_count:
        raise HTTPException(status_code=404, detail="Node not found")

    # Remove all edges connected to this node
    data["edges"] = [
        e
        for e in data["edges"]
        if e["source"] != node_id and e["target"] != node_id
    ]

    store.save(pipeline_id, data)


@router.patch("/{pipeline_id}/nodes/{node_id}")
async def update_node(pipeline_id: str, node_id: str, body: UpdateNodeRequest) -> dict:
    data = _load_pipeline(pipeline_id)

    node = next((n for n in data["nodes"] if n["id"] == node_id), None)
    if node is None:
        raise HTTPException(status_code=404, detail="Node not found")

    if body.params is not None:
        # Merge user values into the existing ParamDefinition[] array
        # so the array structure is preserved for the frontend.
        existing_params = node.get("params", [])
        if isinstance(existing_params, list) and existing_params:
            for param_def in existing_params:
                if isinstance(param_def, dict) and param_def.get("name") in body.params:
                    param_def["default"] = body.params[param_def["name"]]
            node["params"] = existing_params
        else:
            # Legacy dict or empty — recover array from NODE_REGISTRY template
            template = NODE_REGISTRY.get(node.get("type", ""), {})
            template_params = list(template.get("params", []))
            if template_params:
                import copy

                template_params = copy.deepcopy(template_params)
                merged: dict[str, Any] = {}
                if isinstance(existing_params, dict):
                    merged.update(existing_params)
                merged.update(body.params)  # new values take precedence
                for param_def in template_params:
                    if param_def["name"] in merged:
                        param_def["default"] = merged[param_def["name"]]
                node["params"] = template_params
            else:
                # No template available — store dict as last resort
                node["params"] = body.params
    if body.code is not None:
        node["code"] = body.code
    if body.position is not None:
        node["position"] = body.position.model_dump()
    if body.name is not None:
        # Empty string clears the custom name (reverts to definition label)
        node["name"] = body.name if body.name else None

    store.save(pipeline_id, data)

    # Notify frontend to refetch pipeline (e.g. when CC patches params)
    broadcast_sync(pipeline_id, {
        'type': 'pipeline_updated',
        'node_id': node_id,
    })

    return node


# ── Edge Operations ──────────────────────────────────────────────

# When a user connects a "train" port, automatically create companion
# edges for "val" and "test" (if both source and target have those ports).
_COMPANION_PORTS: dict[str, list[str]] = {"train": ["val", "test"]}


def _create_companion_edges(
    data: dict,
    source_node: dict,
    target_node: dict,
    body: AddEdgeRequest,
) -> list[dict]:
    """Return companion edges to auto-create alongside a primary edge."""
    companions = _COMPANION_PORTS.get(body.source_port, [])
    if not companions:
        return []

    source_outputs = {o["name"] for o in source_node.get("outputs", [])}
    target_inputs = {i["name"] for i in target_node.get("inputs", [])}
    occupied_target_ports = {
        e["target_port"]
        for e in data["edges"]
        if e["target"] == body.target
    }

    result: list[dict] = []
    for port in companions:
        if port in source_outputs and port in target_inputs and port not in occupied_target_ports:
            result.append({
                "id": uuid.uuid4().hex,
                "source": body.source,
                "source_port": port,
                "target": body.target,
                "target_port": port,
                "condition": None,
            })
    return result


def _node_label(node: dict) -> str:
    """Return the display label for a pipeline node."""
    return node.get("name") or node.get("type", "unknown").split(".")[-1].replace("_", " ").title()


@router.post("/{pipeline_id}/edges", status_code=201)
async def add_edge(pipeline_id: str, body: AddEdgeRequest) -> dict:
    data = _load_pipeline(pipeline_id)

    # 0. Self-loop check
    if body.source == body.target:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "self_loop",
                "message": "A node cannot connect to itself",
            },
        )

    # 1. Validate source and target nodes exist
    source_node = next(
        (n for n in data["nodes"] if n["id"] == body.source), None
    )
    if source_node is None:
        raise HTTPException(status_code=400, detail="Source node not found")

    target_node = next(
        (n for n in data["nodes"] if n["id"] == body.target), None
    )
    if target_node is None:
        raise HTTPException(status_code=400, detail="Target node not found")

    source_label = _node_label(source_node)
    target_label = _node_label(target_node)

    # 2. Validate port names exist
    source_port = next(
        (p for p in source_node.get("outputs", []) if p["name"] == body.source_port),
        None,
    )
    if source_port is None:
        raise HTTPException(
            status_code=400,
            detail=f"Source port '{body.source_port}' not found",
        )

    target_port = next(
        (p for p in target_node.get("inputs", []) if p["name"] == body.target_port),
        None,
    )
    if target_port is None:
        raise HTTPException(
            status_code=400,
            detail=f"Target port '{body.target_port}' not found",
        )

    # 3. Check if target port is already occupied
    existing_edge = next(
        (e for e in data["edges"]
         if e["target"] == body.target and e["target_port"] == body.target_port),
        None,
    )
    if existing_edge is not None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "port_occupied",
                "message": f"Input port '{body.target_port}' on {target_label} already has a connection. Remove the existing connection first or add another {target_label} node.",
                "target_port": body.target_port,
                "existing_source": existing_edge["source"],
            },
        )

    # 4. Validate port types match
    if source_port["type"] != target_port["type"]:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "type_mismatch",
                "message": f"Port type mismatch: {source_port['type']} output cannot connect to {target_port['type']} input",
                "source_port_type": source_port["type"],
                "target_port_type": target_port["type"],
            },
        )

    # 5. Check allowed_upstream constraint (per-port)
    source_type = source_node.get("type", "")
    target_type = target_node.get("type", "")
    source_template = NODE_REGISTRY.get(source_type, {})
    target_template = NODE_REGISTRY.get(target_type, {})
    source_fn = source_type.rsplit(".", 1)[-1] if "." in source_type else source_type
    target_fn = target_type.rsplit(".", 1)[-1] if "." in target_type else target_type
    allowed_map = target_template.get("allowed_upstream", {})

    if isinstance(allowed_map, dict) and body.target_port in allowed_map:
        allowed_for_port = allowed_map[body.target_port]
        if allowed_for_port and source_fn not in allowed_for_port:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "invalid_upstream",
                    "message": f'Port "{body.target_port}" on {target_label} cannot accept connections from {source_fn}. Allowed: {allowed_for_port}',
                    "source_node": source_fn,
                    "target_node": target_fn,
                    "target_port": body.target_port,
                    "allowed_upstream": allowed_for_port,
                },
            )
    elif isinstance(allowed_map, list):
        # Legacy list format (backward compat)
        if allowed_map and source_fn not in allowed_map:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "invalid_upstream",
                    "message": f"{source_label} ({source_fn}) cannot connect to {target_label}. Allowed: {allowed_map}",
                    "source_node": source_fn,
                    "target_node": target_fn,
                    "allowed_upstream": allowed_map,
                },
            )

    # 6. Check for cycles
    if would_create_cycle(data, body.source, body.target):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "cycle",
                "message": f"Cannot connect {source_label} to {target_label}: would create a cycle",
            },
        )

    edge_id = uuid.uuid4().hex
    edge = {
        "id": edge_id,
        "source": body.source,
        "source_port": body.source_port,
        "target": body.target,
        "target_port": body.target_port,
        "condition": body.condition,
    }

    data["edges"].append(edge)

    # Auto-connect companion ports (train → val, test)
    companion_edges = _create_companion_edges(data, source_node, target_node, body)
    data["edges"].extend(companion_edges)

    store.save(pipeline_id, data)

    # Auto-configure target node params from upstream metadata (instant, no LLM)
    _auto_configure_node(pipeline_id, body.target)

    if companion_edges:
        broadcast_sync(pipeline_id, {"type": "pipeline_updated", "node_id": body.target})

    return edge


@router.delete("/{pipeline_id}/edges/{edge_id}", status_code=204)
async def delete_edge(pipeline_id: str, edge_id: str) -> None:
    data = _load_pipeline(pipeline_id)

    deleted_edge = next((e for e in data["edges"] if e["id"] == edge_id), None)
    if deleted_edge is None:
        raise HTTPException(status_code=404, detail="Edge not found")

    # Collect IDs to remove: the requested edge + any companion edges
    ids_to_remove = {edge_id}
    companion_ports = _COMPANION_PORTS.get(deleted_edge.get("source_port", ""), [])
    if companion_ports:
        for e in data["edges"]:
            if (
                e["source"] == deleted_edge["source"]
                and e["target"] == deleted_edge["target"]
                and e["source_port"] in companion_ports
                and e["target_port"] in companion_ports
            ):
                ids_to_remove.add(e["id"])

    data["edges"] = [e for e in data["edges"] if e["id"] not in ids_to_remove]

    store.save(pipeline_id, data)
    if len(ids_to_remove) > 1:
        broadcast_sync(
            pipeline_id,
            {"type": "pipeline_updated", "node_id": deleted_edge["target"]},
        )


@router.patch("/{pipeline_id}/edges/{edge_id}")
async def update_edge(pipeline_id: str, edge_id: str, body: UpdateEdgeRequest) -> dict:
    data = _load_pipeline(pipeline_id)

    edge = next((e for e in data["edges"] if e["id"] == edge_id), None)
    if edge is None:
        raise HTTPException(status_code=404, detail="Edge not found")

    edge["condition"] = body.condition

    store.save(pipeline_id, data)
    return edge


# ── Execution API ────────────────────────────────────────────────


def _run_pipeline(pipeline_id: str, pipeline: dict, node_id: str | None = None) -> str:
    """Spawn executor in a background thread. Returns run_id immediately.

    Uses executor.run_all() or executor.run_from() — the public API —
    rather than calling internal methods directly.

    Raises HTTPException(409) if a run is already active (atomic check-and-set).
    """
    executor = PipelineExecutor(broadcast=broadcast_sync)

    # Atomic check-and-set prevents TOCTOU race condition
    if not try_set_active_executor(pipeline_id, executor):
        raise HTTPException(status_code=409, detail="Pipeline is already running")

    run_id = uuid.uuid4().hex

    def _target() -> None:
        try:
            if node_id is not None:
                executor.run_from(node_id, pipeline, run_id=run_id)
            else:
                executor.run_all(pipeline, run_id=run_id)
        except Exception as exc:
            import logging
            import traceback

            logging.getLogger(__name__).error(
                "Pipeline %s run %s failed: %s", pipeline_id, run_id, exc
            )
            broadcast_sync(pipeline_id, {
                "status": "error",
                "run_id": run_id,
                "traceback": traceback.format_exc(),
            })
        finally:
            remove_active_executor(pipeline_id)

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    return run_id


@router.post("/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str) -> dict:
    data = _load_pipeline(pipeline_id)
    run_id = _run_pipeline(pipeline_id, data)
    return {"run_id": run_id}


@router.post("/{pipeline_id}/run/{node_id}")
async def run_from_node(pipeline_id: str, node_id: str) -> dict:
    data = _load_pipeline(pipeline_id)

    # Validate node exists
    if not any(n["id"] == node_id for n in data.get("nodes", [])):
        raise HTTPException(status_code=404, detail="Node not found")

    run_id = _run_pipeline(pipeline_id, data, node_id=node_id)
    return {"run_id": run_id}


@router.post("/{pipeline_id}/cancel")
async def cancel_pipeline(pipeline_id: str) -> dict:
    _load_pipeline(pipeline_id)  # 404 if missing
    executor = get_active_executor(pipeline_id)
    if executor is not None:
        executor.cancel()
    return {"status": "ok"}


@router.get("/{pipeline_id}/status")
async def pipeline_status(pipeline_id: str) -> dict:
    _load_pipeline(pipeline_id)  # 404 if missing
    is_running = get_active_executor(pipeline_id) is not None
    last_run_id = file_store.get_latest_run_id(pipeline_id)

    # Try to determine current node from status file
    current_node_id = None
    if is_running and last_run_id:
        runs_dir = file_store._runs_dir(pipeline_id)
        # Check latest run dir for status
        for run_dir in sorted(runs_dir.iterdir(), key=lambda d: d.stat().st_mtime, reverse=True):
            status_file = run_dir / "_status.json"
            if status_file.exists():
                try:
                    status_data = json.loads(status_file.read_text())
                    current_node_id = status_data.get("current_node_id")
                except Exception:
                    pass
                break

    return {
        "is_running": is_running,
        "current_node_id": current_node_id,
        "last_run_id": last_run_id,
    }


# ── Run History API ──────────────────────────────────────────────


@router.get("/{pipeline_id}/runs")
async def list_runs(pipeline_id: str) -> list[dict]:
    _load_pipeline(pipeline_id)  # 404 if missing
    runs = file_store.list_runs(pipeline_id)

    # Enrich with status, completed_at, duration from _status.json
    for run in runs:
        run_dir = file_store._runs_dir(pipeline_id) / run["id"]
        status_file = run_dir / "_status.json"
        if status_file.exists():
            try:
                status_data = json.loads(status_file.read_text())
                run["status"] = status_data.get("status", "unknown")
                run["completed_at"] = status_data.get("completed_at")
                # Prefer timestamps from _status.json for started_at
                if "started_at" in status_data:
                    run["started_at"] = status_data["started_at"]
                # Compute duration if both timestamps are available
                sa = status_data.get("started_at")
                ca = status_data.get("completed_at")
                if sa and ca:
                    t0 = datetime.fromisoformat(sa)
                    t1 = datetime.fromisoformat(ca)
                    run["duration"] = round((t1 - t0).total_seconds(), 1)
                else:
                    run["duration"] = None
            except Exception:
                run["status"] = "unknown"
                run["completed_at"] = None
                run["duration"] = None
        else:
            run["status"] = "unknown"
            run["completed_at"] = None
            run["duration"] = None

    return runs


@router.delete("/{pipeline_id}/runs/{run_id}", status_code=204)
async def delete_run(pipeline_id: str, run_id: str) -> None:
    _load_pipeline(pipeline_id)
    try:
        _validate_path_id(run_id, "run_id")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    run_dir = file_store._runs_dir(pipeline_id) / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="Run not found")
    file_store.delete_run(pipeline_id, run_id)


# ── Output API ───────────────────────────────────────────────────


def _resolve_run_dir(pipeline_id: str, run_id: str | None) -> tuple[str, Path]:
    """Resolve run_id (defaulting to latest) and return (run_id, run_dir)."""
    if run_id is None:
        run_id = file_store.get_latest_run_id(pipeline_id)
    if run_id is None:
        raise HTTPException(status_code=404, detail="No runs found")
    try:
        _validate_path_id(run_id, "run_id")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    run_dir = file_store._runs_dir(pipeline_id) / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="Run not found")
    return run_id, run_dir


def _find_output_file(run_dir: Path, node_id: str) -> Path | None:
    """Find the primary output file for a node (excludes metadata files)."""
    candidates = [
        f
        for f in run_dir.glob(f"{node_id}_*")
        if not _is_internal_file(f)
    ]
    if candidates:
        return candidates[0]
    # Also check for direct node_id.* files
    candidates = [
        f
        for f in run_dir.glob(f"{node_id}.*")
        if not _is_internal_file(f)
    ]
    return candidates[0] if candidates else None


def _file_metadata(output_file: Path) -> dict[str, Any]:
    """Build metadata dict for a single output file."""
    ext = output_file.suffix.lower()
    size = output_file.stat().st_size
    meta: dict[str, Any] = {
        "file": output_file.name,
        "type": ext.lstrip("."),
        "size": size,
    }

    if ext == ".parquet":
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(output_file)
            df = table.to_pandas()
            meta["preview"] = {
                "columns": list(df.columns),
                "rows": df.head(10).values.tolist(),
                "total_rows": len(df),
                "dtypes": {col: str(df[col].dtype) for col in df.columns},
            }
        except Exception:
            pass
    elif ext == ".csv":
        try:
            import pandas as pd

            df = pd.read_csv(output_file, nrows=10)
            meta["preview"] = {
                "columns": list(df.columns),
                "rows": df.values.tolist(),
                "total_rows": -1,
                "dtypes": {col: str(df[col].dtype) for col in df.columns},
            }
        except Exception:
            pass
    elif ext == ".json":
        try:
            data = json.loads(output_file.read_text())
            meta["preview"] = data
        except Exception:
            pass
    elif ext == ".joblib":
        # MODEL output: show file size only.
        # We intentionally do NOT call joblib.load() here because joblib
        # uses pickle internally and deserializing untrusted data from
        # the sandbox would be an arbitrary-code-execution vulnerability.
        meta["preview"] = {
            "format": "joblib",
            "file_size": meta["size"],
        }
    elif ext == ".npy":
        try:
            import numpy as np

            arr = np.load(output_file, mmap_mode="r")
            meta["preview"] = {
                "shape": list(arr.shape),
                "dtype": str(arr.dtype),
                "values": arr.flatten()[:20].tolist(),
                "total_elements": int(arr.size),
            }
        except Exception:
            pass
    elif ext == ".pt":
        meta["preview"] = {
            "format": "pytorch",
            "file_size": meta["size"],
        }

    return meta


def _is_internal_file(f: Path) -> bool:
    """Return True for internal metadata files that are not node outputs."""
    name = f.name
    if name.endswith((".hash", ".txt", ".meta.json", ".analysis.json", "_transform_summary.json")):
        return True
    # Exclude internal manifest/result/error JSON files but keep
    # legitimate node output JSON (e.g. metrics.json).
    _INTERNAL_SUFFIXES = ("_manifest.json", "_manifest_result.json", "_manifest_error.json")
    return any(name.endswith(s) for s in _INTERNAL_SUFFIXES)


def _find_meta_json(run_dir: Path, node_id: str) -> Path | None:
    """Find the .meta.json sidecar for a node's output.

    Looks for {node_id}_df.meta.json first, then any {node_id}*.meta.json.
    """
    preferred = run_dir / f"{node_id}_df.meta.json"
    if preferred.exists():
        return preferred
    candidates = list(run_dir.glob(f"{node_id}*.meta.json"))
    return candidates[0] if candidates else None


def _output_metadata(run_dir: Path, node_id: str) -> dict:
    """Build output metadata for a node.

    Returns metadata for all output files produced by the node.
    Single-output nodes return a flat dict (backwards-compatible).
    Multi-output nodes include an ``outputs`` list.
    """
    output_files = [
        f
        for f in run_dir.glob(f"{node_id}_*")
        if not _is_internal_file(f)
    ]
    if not output_files:
        # Also check for direct node_id.* files
        output_files = [
            f
            for f in run_dir.glob(f"{node_id}.*")
            if not _is_internal_file(f)
        ]

    # Check for error and container logs regardless of output files
    error_path = run_dir / f"{node_id}_manifest_error.json"
    logs_file = run_dir / f"{node_id}_logs.txt"

    if not output_files:
        # No output files — return error/logs metadata if available
        if error_path.exists() or logs_file.exists():
            meta: dict[str, Any] = {
                "node_id": node_id,
                "type": "ERROR",
                "file": "",
                "size": 0,
                "preview": None,
            }
            if error_path.exists():
                try:
                    err = json.loads(error_path.read_text())
                    meta["error"] = err.get("error")
                except Exception:
                    pass
            if logs_file.exists():
                meta["logs"] = logs_file.read_text()
            return meta
        raise HTTPException(status_code=404, detail="Output not found")

    primary = output_files[0]
    meta: dict[str, Any] = {
        "node_id": node_id,
        **_file_metadata(primary),
    }

    # Include .meta.json sidecar content if present
    meta_json_path = _find_meta_json(run_dir, node_id)
    if meta_json_path is not None:
        try:
            meta["column_metadata"] = json.loads(meta_json_path.read_text())
        except Exception:
            pass

    # Include transform_summary.json sidecar if present
    transform_summary_path = run_dir / f"{node_id}_transform_summary.json"
    if transform_summary_path.exists():
        try:
            meta["transform_summary"] = json.loads(transform_summary_path.read_text())
        except Exception:
            pass

    if len(output_files) > 1:
        outputs_list = []
        for f in output_files:
            fmeta = _file_metadata(f)
            # Extract port name: remove node_id prefix from stem
            port_name = f.stem
            if port_name.startswith(node_id + "_"):
                port_name = port_name[len(node_id) + 1 :]
            fmeta["port"] = port_name
            outputs_list.append(fmeta)
        meta["outputs"] = outputs_list

    if error_path.exists():
        try:
            err = json.loads(error_path.read_text())
            meta["error"] = err.get("error")
        except Exception:
            pass

    if logs_file.exists():
        meta["logs"] = logs_file.read_text()

    return meta


def _parquet_to_csv_response(output_file: Path) -> StreamingResponse:
    """Convert a parquet file to CSV and return as a streaming response."""
    import io

    import pandas as pd

    df = pd.read_parquet(output_file)
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    filename = output_file.stem + ".csv"
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/{pipeline_id}/outputs/{node_id}")
async def get_output(
    pipeline_id: str,
    node_id: str,
    run_id: str | None = Query(default=None),
) -> dict:
    _load_pipeline(pipeline_id)
    resolved_run_id, run_dir = _resolve_run_dir(pipeline_id, run_id)
    return _output_metadata(run_dir, node_id)


def _resolve_output_file(
    run_dir: Path, node_id: str, port: str | None
) -> Path:
    """Find the output file for a node, optionally filtered by port name.

    Raises HTTPException(400) if port contains invalid characters,
    or HTTPException(404) if no matching file is found.
    """
    if port:
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", port):
            raise HTTPException(
                status_code=400, detail="Invalid port name"
            )
        candidates = list(run_dir.glob(f"{node_id}_{port}.*"))
        candidates = [c for c in candidates if not _is_internal_file(c)]
        if not candidates:
            raise HTTPException(
                status_code=404, detail=f"Output port '{port}' not found"
            )
        return candidates[0]
    output_file = _find_output_file(run_dir, node_id)
    if output_file is None:
        raise HTTPException(status_code=404, detail="Output not found")
    return output_file


@router.get("/{pipeline_id}/outputs/{node_id}/download")
async def download_output(
    pipeline_id: str,
    node_id: str,
    run_id: str | None = Query(default=None),
    format: str | None = Query(default=None),
    port: str | None = Query(default=None),
) -> StreamingResponse:
    _load_pipeline(pipeline_id)
    resolved_run_id, run_dir = _resolve_run_dir(pipeline_id, run_id)
    output_file = _resolve_output_file(run_dir, node_id, port)

    if format == "csv" and output_file.suffix == ".parquet":
        return _parquet_to_csv_response(output_file)

    media_type = mimetypes.guess_type(output_file.name)[0] or "application/octet-stream"

    def _iter_file():
        with open(output_file, "rb") as f:
            while chunk := f.read(64 * 1024):
                yield chunk

    return StreamingResponse(
        _iter_file(),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={output_file.name}"},
    )


@router.get("/{pipeline_id}/runs/{run_id}/outputs/{node_id}")
async def get_run_output(pipeline_id: str, run_id: str, node_id: str) -> dict:
    _load_pipeline(pipeline_id)
    _, run_dir = _resolve_run_dir(pipeline_id, run_id)
    return _output_metadata(run_dir, node_id)


@router.get("/{pipeline_id}/runs/{run_id}/outputs/{node_id}/download")
async def download_run_output(
    pipeline_id: str,
    run_id: str,
    node_id: str,
    format: str | None = Query(default=None),
    port: str | None = Query(default=None),
) -> StreamingResponse:
    _load_pipeline(pipeline_id)
    _, run_dir = _resolve_run_dir(pipeline_id, run_id)
    output_file = _resolve_output_file(run_dir, node_id, port)

    if format == "csv" and output_file.suffix == ".parquet":
        return _parquet_to_csv_response(output_file)

    media_type = mimetypes.guess_type(output_file.name)[0] or "application/octet-stream"

    def _iter_file():
        with open(output_file, "rb") as f:
            while chunk := f.read(64 * 1024):
                yield chunk

    return StreamingResponse(
        _iter_file(),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={output_file.name}"},
    )


# ── Metadata Sidecar API ─────────────────────────────────────────


@router.get("/{pipeline_id}/outputs/{node_id}/metadata")
async def get_metadata(
    pipeline_id: str,
    node_id: str,
    run_id: str | None = Query(default=None),
) -> dict:
    """Read .meta.json sidecar for a node's output."""
    _load_pipeline(pipeline_id)
    _, run_dir = _resolve_run_dir(pipeline_id, run_id)
    meta_path = _find_meta_json(run_dir, node_id)
    if meta_path is None:
        return {"metadata": None}
    try:
        return {"metadata": json.loads(meta_path.read_text())}
    except Exception:
        return {"metadata": None}


@router.put("/{pipeline_id}/outputs/{node_id}/metadata")
async def put_metadata(
    pipeline_id: str,
    node_id: str,
    body: dict,
    run_id: str | None = Query(default=None),
) -> dict:
    """Save user-edited metadata to .meta.json alongside the output file."""
    _load_pipeline(pipeline_id)
    _, run_dir = _resolve_run_dir(pipeline_id, run_id)
    output_file = _find_output_file(run_dir, node_id)

    meta_path = run_dir / f"{node_id}_df.meta.json"
    meta_path.write_text(json.dumps(body, indent=2))
    broadcast_sync(pipeline_id, {"type": "metadata_updated", "node_id": node_id})

    # Re-cast parquet in background when output file exists.
    # Always re-read from the original source file (CSV/parquet uploaded by
    # the user) so that columns previously dropped via role=ignore/identifier
    # can be recovered when the user changes the role back to "feature".
    if output_file is not None:
        def _recast() -> None:
            try:
                import pandas as pd

                from ml_toolbox.llm.metadata import cast_by_metadata

                # Prefer reading from original source to recover dropped columns
                source_path = body.get("source_path")
                # Validate source_path is within the allowed data directory
                # to prevent path-traversal via client-supplied metadata.
                if source_path:
                    resolved = Path(source_path).resolve()
                    if not resolved.is_relative_to(DATA_DIR.resolve()):
                        source_path = None
                if source_path and Path(source_path).is_file():
                    src = Path(source_path)
                    if src.suffix.lower() == ".csv":
                        df = pd.read_csv(src)
                    else:
                        df = pd.read_parquet(src)
                else:
                    df = pd.read_parquet(output_file)

                df, cast_results = cast_by_metadata(df, body)

                # Update cast_status in metadata
                for col_name, result in cast_results.items():
                    if col_name in body.get("columns", {}):
                        body["columns"][col_name]["cast_status"] = result.get("status")
                        if result.get("reason"):
                            body["columns"][col_name]["cast_reason"] = result["reason"]

                meta_path.write_text(json.dumps(body, indent=2, ensure_ascii=False))
                df.to_parquet(output_file, index=False)
                broadcast_sync(
                    pipeline_id, {"type": "metadata_updated", "node_id": node_id},
                )
            except Exception as e:
                logger.warning("Re-cast failed for %s: %s", node_id, e)

        threading.Thread(target=_recast, daemon=True).start()

    # Re-configure downstream nodes after metadata edit
    def _propagate() -> None:
        try:
            pipeline_data = store.load(pipeline_id)
            downstream = _get_downstream_nodes(node_id, pipeline_data)
            if not downstream:
                return

            # Re-run auto-configure on each downstream node
            # (they read metadata lazily via DAG traversal)
            for ds_node_id in downstream:
                _auto_configure_node(pipeline_id, ds_node_id)

            broadcast_sync(pipeline_id, {
                "type": "metadata_propagated",
                "source_node_id": node_id,
                "updated_nodes": downstream,
            })
        except Exception as e:
            logger.warning("Metadata propagation failed for %s: %s", node_id, e)

    threading.Thread(target=_propagate, daemon=True).start()

    return {"status": "saved"}


# ── Analysis Sidecar API ─────────────────────────────────────────


@router.get("/{pipeline_id}/outputs/{node_id}/analysis")
async def get_analysis(
    pipeline_id: str,
    node_id: str,
    run_id: str | None = Query(default=None),
) -> dict:
    """Read .analysis.json sidecar produced by subprocess CC output analysis."""
    _load_pipeline(pipeline_id)
    _, run_dir = _resolve_run_dir(pipeline_id, run_id)
    analysis_files = list(run_dir.glob(f"{node_id}*.analysis.json"))
    if not analysis_files:
        return {"analysis": None}
    try:
        return {"analysis": json.loads(analysis_files[0].read_text())}
    except Exception:
        return {"analysis": None}


@router.get("/{pipeline_id}/outputs/{node_id}/eda-context")
async def get_eda_context(
    pipeline_id: str,
    node_id: str,
    run_id: str | None = Query(default=None),
) -> dict:
    """Lazy-read EDA context by traversing the DAG upward from this node,
    finding all EDA sibling nodes, and combining their report outputs."""
    pipeline = _load_pipeline(pipeline_id)
    _, run_dir = _resolve_run_dir(pipeline_id, run_id)

    result = _collect_eda_context_from_dag(pipeline, node_id, run_dir)
    return {"eda_context": result}


@router.put("/{pipeline_id}/selection")
async def update_selection(pipeline_id: str, body: dict) -> dict:
    """Persist the set of node IDs the user currently has selected on the canvas."""
    _validate_path_id(pipeline_id, "pipeline_id")
    selection_file = file_store.PROJECTS_DIR / pipeline_id / ".selection.json"
    selection_file.write_text(json.dumps(body))
    return {"status": "saved"}


@router.get("/{pipeline_id}/selection")
async def get_selection(pipeline_id: str) -> dict:
    """Return the current canvas selection for a pipeline."""
    _validate_path_id(pipeline_id, "pipeline_id")
    selection_file = file_store.PROJECTS_DIR / pipeline_id / ".selection.json"
    if selection_file.exists():
        return json.loads(selection_file.read_text())
    return {"selected_nodes": []}


@router.post("/{pipeline_id}/outputs/{node_id}/metadata-notify")
async def notify_metadata_updated(pipeline_id: str, node_id: str) -> dict:
    """Broadcast a metadata_updated event over WebSocket.

    Called by Pipeline CC after writing .meta.json directly to disk.
    """
    broadcast_sync(pipeline_id, {"type": "metadata_updated", "node_id": node_id})
    return {"status": "notified"}


# ── Auto-configure node params via deterministic rule engine ─────


def _get_downstream_nodes(node_id: str, pipeline_data: dict) -> list[str]:
    """Return all transitive downstream node IDs from *node_id*."""
    edges = pipeline_data.get("edges", [])
    downstream: list[str] = []
    queue = [node_id]
    visited: set[str] = {node_id}
    while queue:
        current = queue.pop(0)
        for edge in edges:
            if edge["source"] == current:
                target = edge["target"]
                if target not in visited:
                    visited.add(target)
                    downstream.append(target)
                    queue.append(target)
    return downstream


def _read_upstream_metadata(
    pipeline_id: str, target_node_id: str, pipeline_data: dict,
) -> dict | None:
    """Traverse DAG upward from target_node_id to find the nearest .meta.json.

    Skips EDA nodes (they produce METRICS, not TABLE) and keeps going
    until a node with .meta.json is found.
    """
    edges = pipeline_data.get("edges", [])
    nodes = {n["id"]: n for n in pipeline_data.get("nodes", [])}
    try:
        latest_run = file_store.get_latest_run_id(pipeline_id)
        if not latest_run:
            return None
        run_dir = file_store.PROJECTS_DIR / pipeline_id / "runs" / latest_run
    except Exception:
        return None

    # BFS upward
    visited: set[str] = set()
    queue = [target_node_id]

    while queue:
        current = queue.pop(0)
        parent_ids = [e["source"] for e in edges if e["target"] == current]

        for parent_id in parent_ids:
            if parent_id in visited:
                continue
            visited.add(parent_id)

            # Skip EDA nodes — they don't produce .meta.json
            parent_node = nodes.get(parent_id)
            if parent_node and ".eda." in parent_node.get("type", "").lower():
                queue.append(parent_id)
                continue

            meta_files = list(run_dir.glob(f"{parent_id}*.meta.json"))
            if meta_files:
                try:
                    return json.loads(meta_files[0].read_text())
                except Exception:
                    pass

            # No .meta.json at this level, keep going up
            queue.append(parent_id)

    return None


def _extract_eda_section_from_report(report: dict) -> dict:
    """Extract a context section from an EDA node's report JSON.

    Maps report_type to a {section_key: section_data} dict that mirrors
    the old .eda-context.json format consumed by auto-configure rules.
    """
    report_type = report.get("report_type", "")

    if report_type == "correlation_matrix":
        high_pairs = [
            [p["a"], p["b"], p["r"]]
            for p in report.get("top_pairs", [])
            if p.get("abs_r", 0) > 0.8
        ]
        ctx: dict = {"high_pairs": high_pairs}
        if "target_correlations" in report:
            ctx["target_correlations"] = [
                {"feature": tc["feature"], "r": tc["r"]}
                for tc in report["target_correlations"]
            ]
        return {"correlation": ctx}

    if report_type == "distribution_profile":
        dist: dict = {}
        for col_info in report.get("columns", []):
            stats = col_info.get("stats")
            if stats and "skewness" in stats:
                dist[col_info["name"]] = {
                    "skewness": stats["skewness"],
                    "kurtosis": stats["kurtosis"],
                    "mean": stats["mean"],
                    "std": stats["std"],
                }
        return {"distribution": dist} if dist else {}

    if report_type == "missing_analysis":
        missing: dict = {}
        for col_info in report.get("columns", []):
            missing[col_info["name"]] = {
                "missing_pct": col_info["missing_pct"],
                "severity": col_info["severity"],
            }
        return {"missing": missing} if missing else {}

    if report_type == "outlier_detection":
        outliers: dict = {}
        method = report.get("params", {}).get("method", "both")
        for col_info in report.get("columns", []):
            if col_info.get("outlier_count", 0) > 0:
                entry: dict = {
                    "method": method,
                    "outlier_pct": col_info["outlier_pct"],
                }
                if "z_max" in col_info:
                    entry["z_max"] = round(col_info["z_max"], 4)
                if "upper_fence" in col_info:
                    entry["upper_fence"] = col_info["upper_fence"]
                outliers[col_info["name"]] = entry
        return {"outliers": outliers} if outliers else {}

    return {}


def _collect_eda_context_from_dag(
    pipeline_data: dict, target_node_id: str, run_dir: Path,
) -> dict | None:
    """Traverse DAG upward from target_node_id, find EDA sibling nodes,
    and extract context from their report outputs.

    Returns a combined dict like:
        {"distribution": {...}, "outliers": {...}, "correlation": {...}, "missing": {...}}
    or None if no EDA results found.
    """
    edges = pipeline_data.get("edges", [])
    nodes = {n["id"]: n for n in pipeline_data.get("nodes", [])}
    combined: dict = {}

    # BFS upward through the DAG
    visited_ancestors: set[str] = set()
    queue = [target_node_id]

    while queue:
        current = queue.pop(0)

        # Find parents of current node
        parent_ids = [e["source"] for e in edges if e["target"] == current]

        for parent_id in parent_ids:
            if parent_id in visited_ancestors:
                continue
            visited_ancestors.add(parent_id)

            # Find all children of this parent that are EDA nodes
            children = [e["target"] for e in edges if e["source"] == parent_id]
            for child_id in children:
                child_node = nodes.get(child_id)
                if not child_node:
                    continue
                child_type = child_node.get("type", "")
                if ".eda." not in child_type.lower():
                    continue

                # Read this EDA node's report output
                report_files = list(run_dir.glob(f"{child_id}_report.json"))
                if not report_files:
                    continue
                try:
                    report = json.loads(report_files[0].read_text())
                    section = _extract_eda_section_from_report(report)
                    combined.update(section)
                except Exception:
                    continue

            # Continue traversing upward
            queue.append(parent_id)

    return combined or None


def _read_upstream_eda_context(
    pipeline_id: str, target_node_id: str, pipeline_data: dict,
) -> dict | None:
    """Lazy-read EDA context by traversing the DAG upward and extracting
    from EDA nodes' report outputs."""
    try:
        latest_run = file_store.get_latest_run_id(pipeline_id)
        if not latest_run:
            return None
        run_dir = file_store.PROJECTS_DIR / pipeline_id / "runs" / latest_run
        return _collect_eda_context_from_dag(pipeline_data, target_node_id, run_dir)
    except Exception:
        return None


def _get_params_for_node(
    node_fn: str,
    continuous: list[str],
    target_col: str,
    identifiers: list[str],
    categoricals: list[str],
    columns_meta: dict,
    eda_context: dict | None = None,
) -> dict[str, Any]:
    """Return param updates based on node type and column metadata."""
    if node_fn == "outlier_detection":
        analysis_cols = [
            c for c in continuous if c != target_col and c not in identifiers
        ]
        return {"columns": ", ".join(analysis_cols)}

    if node_fn == "correlation_matrix":
        return {"target_column": target_col}

    if node_fn == "distribution_profile":
        return {"target_column": target_col}

    if node_fn == "missing_analysis":
        return {}

    if node_fn == "random_holdout":
        return {"stratify_column": target_col}

    # ── EDA-context-aware rules ─────────────────────────────────

    if node_fn == "log_transform":
        result: dict[str, Any] = {}
        if target_col:
            result["target_column"] = target_col
        if eda_context:
            dist = eda_context.get("distribution", {})
            outliers = eda_context.get("outliers", {})
            cols: set[str] = set()
            for col, stats in dist.items():
                if col in continuous and abs(stats.get("skewness", 0)) > 1:
                    cols.add(col)
            for col, stats in outliers.items():
                if col in continuous and stats.get("outlier_pct", 0) > 0.05:
                    cols.add(col)
            sorted_cols = sorted(c for c in cols if c != target_col)
            if sorted_cols:
                result["columns"] = ", ".join(sorted_cols)
                return result
        # Fallback: all continuous non-target columns
        fallback = [c for c in continuous if c != target_col]
        if fallback:
            result["columns"] = ", ".join(fallback)
        return result

    if node_fn == "interaction_creator":
        result = {}
        if target_col:
            result["target_column"] = target_col
        if eda_context:
            pairs = eda_context.get("correlation", {}).get("high_pairs", [])
            pair_strs = [f"{a}:{b}" for a, b, r in pairs if abs(r) > 0.5]
            if pair_strs:
                result["pairs"] = ", ".join(pair_strs)
        return result

    if node_fn == "datetime_encoder":
        result = {}
        if target_col:
            result["target_column"] = target_col
        dt_cols = [
            name for name, m in columns_meta.items()
            if m.get("semantic_type") == "datetime"
        ]
        if dt_cols:
            result["column"] = dt_cols[0]
        return result

    if node_fn == "column_dropper":
        result = {}
        if target_col:
            result["target_column"] = target_col
        drop = [
            name for name, m in columns_meta.items()
            if m.get("role") == "identifier"
        ]
        if drop:
            result["columns_to_drop"] = ", ".join(drop)
        return result

    if node_fn == "missing_imputer":
        result = {}
        if target_col:
            result["target_column"] = target_col
        if eda_context:
            missing = eda_context.get("missing", {})
            high_missing = [
                col for col, m in missing.items()
                if m.get("missing_pct", 0) > 0.3
            ]
            if high_missing:
                result["strategy"] = "constant"
                result["constant_value"] = "0"
        return result

    if node_fn == "feature_selector":
        result: dict[str, Any] = {}
        if eda_context:
            corr = eda_context.get("correlation", {})
            target_corrs = corr.get("target_correlations", [])
            if target_corrs:
                result = {"method": "correlation_with_target", "threshold": "0.05"}
        if target_col:
            result["target_column"] = target_col
        return result

    if node_fn == "category_encoder":
        cat_cols = [
            name for name, m in columns_meta.items()
            if m.get("semantic_type") in ("categorical", "ordinal", "binary")
            and m.get("role") != "target" and m.get("role") != "identifier"
        ]
        result = {}
        if cat_cols:
            result["columns"] = ", ".join(cat_cols)
        if target_col:
            result["target_column"] = target_col
        return result

    if node_fn == "scaler_transform":
        scale_cols = [
            c for c in continuous if c != target_col and c not in identifiers
        ]
        result = {}
        if scale_cols:
            result["columns"] = ", ".join(scale_cols)
        if target_col:
            result["target_column"] = target_col
        return result

    if node_fn == "stratified_holdout":
        if target_col:
            return {"target_column": target_col}
        return {}

    # ── Universal target_column pass-through ─────────────────────
    # Training, evaluation, and other nodes that need target_column
    # in sandbox code. Only effective if the node defines a
    # target_column param in its @node decorator.
    if target_col:
        return {"target_column": target_col}

    return {}


def _auto_configure_node(
    pipeline_id: str, target_node_id: str,
) -> None:
    """Auto-configure node params based on upstream .meta.json.

    Pure deterministic rules — no LLM calls.  Failures are logged and
    silently ignored.
    """
    try:
        pipeline_data = store.load(pipeline_id)
    except FileNotFoundError:
        return

    target_node = next(
        (n for n in pipeline_data["nodes"] if n["id"] == target_node_id), None
    )
    if not target_node:
        return

    node_type = target_node.get("type", "")
    node_fn = node_type.rsplit(".", 1)[-1]

    metadata = _read_upstream_metadata(pipeline_id, target_node_id, pipeline_data)
    eda_context = _read_upstream_eda_context(
        pipeline_id, target_node_id, pipeline_data,
    )

    if not metadata or "columns" not in metadata:
        # Even without metadata, some EDA-only rules could apply,
        # but all current rules need at least columns_meta for target_col.
        return

    columns_meta: dict = metadata["columns"]

    # Classify columns by semantic type / role
    continuous = [
        name for name, m in columns_meta.items()
        if m.get("semantic_type") == "continuous"
    ]
    target_col = next(
        (name for name, m in columns_meta.items() if m.get("role") == "target"), ""
    )
    identifiers = [
        name for name, m in columns_meta.items() if m.get("role") == "identifier"
    ]
    categoricals = [
        name for name, m in columns_meta.items()
        if m.get("semantic_type") in ("categorical", "ordinal", "binary")
    ]

    params_update = _get_params_for_node(
        node_fn, continuous, target_col, identifiers, categoricals, columns_meta,
        eda_context,
    )
    if not params_update:
        return

    # Apply updates to matching param definitions
    changed = False
    for param_def in target_node.get("params", []):
        if isinstance(param_def, dict) and param_def.get("name") in params_update:
            param_def["default"] = params_update[param_def["name"]]
            changed = True

    if not changed:
        return

    store.save(pipeline_id, pipeline_data)
    broadcast_sync(pipeline_id, {
        "type": "pipeline_updated",
        "node_id": target_node_id,
    })
    logger.info(
        "Auto-configured params for node %s: %s",
        target_node_id, list(params_update.keys()),
    )
