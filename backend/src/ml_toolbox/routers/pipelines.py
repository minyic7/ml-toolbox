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
        return store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")


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

    node: dict[str, Any] = {
        "id": node_id,
        "type": body.type,
        "position": body.position.model_dump(),
        "params": body.params if body.params is not None else list(template["params"]),
        "code": body.code if body.code is not None else template["default_code"],
        "inputs": list(template["inputs"]),
        "outputs": list(template["outputs"]),
    }
    if body.name:
        node["name"] = body.name

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

    # 5. Check allowed_upstream constraint
    source_type = source_node.get("type", "")
    target_type = target_node.get("type", "")
    source_template = NODE_REGISTRY.get(source_type, {})
    target_template = NODE_REGISTRY.get(target_type, {})
    source_category = source_template.get("category", "")
    allowed = target_template.get("allowed_upstream", [])
    if allowed and source_category not in allowed:
        source_fn = source_type.split(".")[-1] if "." in source_type else source_type
        target_fn = target_type.split(".")[-1] if "." in target_type else target_type
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_upstream",
                "message": f"{source_label} ({source_fn}) cannot connect to {target_label}. Allowed upstream categories: {', '.join(allowed)}",
                "source_node": source_fn,
                "target_node": target_fn,
                "allowed_upstream": allowed,
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
    store.save(pipeline_id, data)
    return edge


@router.delete("/{pipeline_id}/edges/{edge_id}", status_code=204)
async def delete_edge(pipeline_id: str, edge_id: str) -> None:
    data = _load_pipeline(pipeline_id)

    original_count = len(data["edges"])
    data["edges"] = [e for e in data["edges"] if e["id"] != edge_id]

    if len(data["edges"]) == original_count:
        raise HTTPException(status_code=404, detail="Edge not found")

    store.save(pipeline_id, data)


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
    if name.endswith((".hash", ".txt", ".meta.json")):
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

    # Re-cast parquet in background when output file exists
    if output_file is not None:
        def _recast() -> None:
            try:
                import pandas as pd

                from ml_toolbox.llm.metadata import cast_by_metadata

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

    return {"status": "saved"}


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
