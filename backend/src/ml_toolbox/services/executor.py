"""DAG execution engine — runs pipeline nodes in Docker sandbox containers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
import logging
import os
import threading
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

import docker
import requests.exceptions
from docker.errors import ContainerError, ImageNotFound

from ml_toolbox.config import DATA_DIR
from ml_toolbox.services import file_store

logger = logging.getLogger(__name__)

SANDBOX_IMAGE = os.environ.get("ML_TOOLBOX_SANDBOX_IMAGE", "ghcr.io/minyic7/ml-toolbox/sandbox:latest")

# Docker volume name used to share data between backend and sandbox containers.
# When running in Docker-in-Docker (backend creates sandbox containers via host
# docker.sock), bind mounts with container-internal paths don't work because the
# Docker daemon resolves paths on the host. Using a named volume solves this.
DOCKER_VOLUME_NAME = os.environ.get("ML_TOOLBOX_DOCKER_VOLUME", "ml-toolbox_ml_data")

# Broadcast callback type: (pipeline_id, message_dict) -> None
BroadcastFn = Callable[[str, dict[str, Any]], None]


def _translate_params_for_sandbox(
    params: dict[str, Any],
    data_dir: str,
    sandbox_root: str,
) -> dict[str, Any]:
    """Translate host DATA_DIR paths in param values to sandbox container paths.

    Uploaded files are stored under DATA_DIR on the host, but the sandbox
    container mounts the data volume at *sandbox_root* (``/ml_data``).  Any
    string param value that starts with *data_dir* is rewritten so the
    sandbox can find the file.
    """
    translated: dict[str, Any] = {}
    for key, value in params.items():
        if isinstance(value, str) and value.startswith(data_dir):
            translated[key] = sandbox_root + value[len(data_dir):]
        else:
            translated[key] = value
    return translated


_SIDECAR_SUFFIXES = (
    ".meta.json", ".eda-context.json", "_manifest_error.json", "_logs.txt",
    ".hash", "_manifest.json", "_result.json", ".analysis.json",
)


def _is_sidecar_file(path: Path) -> bool:
    """True for metadata sidecars and internal files, not actual data outputs."""
    return any(path.name.endswith(suffix) for suffix in _SIDECAR_SUFFIXES)


class CycleError(Exception):
    """Raised when the pipeline graph contains a cycle."""


class PipelineExecutor:
    """Executes a pipeline DAG in topological order using Docker sandboxes."""

    def __init__(self, broadcast: BroadcastFn | None = None) -> None:
        self._broadcast = broadcast or (lambda _pid, _msg: None)
        self._cancelled = threading.Event()
        self._current_container: Any = None
        self._lock = threading.Lock()
        self._docker: docker.DockerClient | None = None

    # ── Public API ───────────────────────────────────────────────

    def run_all(self, pipeline: dict, run_id: str | None = None) -> str:
        """Execute every node in the pipeline. Returns run_id."""
        run_id = run_id or uuid.uuid4().hex
        pipeline_id = pipeline["id"]
        run_dir = file_store.make_run_dir(pipeline_id, run_id)

        order = self._topological_sort(pipeline)
        self._execute_ordered(order, pipeline, run_dir, run_id)
        return run_id

    def run_from(self, node_id: str, pipeline: dict, run_id: str | None = None) -> str:
        """Re-run *node_id* and all downstream nodes. Returns run_id.

        Upstream nodes that are unchanged get hard-linked from the most
        recent previous run (zero additional disk cost).
        """
        run_id = run_id or uuid.uuid4().hex
        pipeline_id = pipeline["id"]
        run_dir = file_store.make_run_dir(pipeline_id, run_id)

        order = self._topological_sort(pipeline)
        downstream = self._downstream_set(node_id, pipeline)
        downstream.add(node_id)

        # Hard-link cached upstream outputs
        for nid in order:
            if nid not in downstream:
                self._hardlink_cached(nid, pipeline, run_dir)

        # Execute only the downstream portion
        to_run = [nid for nid in order if nid in downstream]
        self._execute_ordered(to_run, pipeline, run_dir, run_id)
        return run_id

    def cancel(self) -> None:
        """Signal cancellation and stop the currently running container."""
        self._cancelled.set()
        with self._lock:
            if self._current_container is not None:
                try:
                    self._current_container.stop(timeout=5)
                except Exception:
                    pass

    # ── Topological Sort (Kahn's algorithm) ──────────────────────

    @staticmethod
    def _topological_sort(pipeline: dict) -> list[str]:
        """Return node IDs in topological order using Kahn's algorithm.

        Raises CycleError if the graph contains a cycle.
        """
        nodes = {n["id"] for n in pipeline.get("nodes", [])}
        edges = pipeline.get("edges", [])

        adj: dict[str, list[str]] = defaultdict(list)
        in_degree: dict[str, int] = {nid: 0 for nid in nodes}

        for edge in edges:
            src, tgt = edge["source"], edge["target"]
            adj[src].append(tgt)
            in_degree[tgt] = in_degree.get(tgt, 0) + 1

        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        order: list[str] = []

        while queue:
            nid = queue.pop(0)
            order.append(nid)
            for neighbor in adj[nid]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(nodes):
            raise CycleError("Pipeline graph contains a cycle")

        return order

    # ── Caching ──────────────────────────────────────────────────

    @staticmethod
    def _params_hash(node: dict) -> str:
        """SHA-256 of the node's params + code for cache invalidation."""
        payload = json.dumps(
            {"params": node.get("params", {}), "code": node.get("code", "")},
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _is_cached(node_id: str, node: dict, run_dir: Path) -> bool:
        """Check whether a valid cached output exists for this node."""
        hash_file = run_dir / f"{node_id}.hash"
        if not hash_file.exists():
            return False
        stored_hash = hash_file.read_text().strip()
        return stored_hash == PipelineExecutor._params_hash(node)

    # ── Hard-linking ─────────────────────────────────────────────

    @staticmethod
    def _hardlink_cached(
        node_id: str, pipeline: dict, run_dir: Path
    ) -> None:
        """Hard-link outputs from the latest previous run into *run_dir*."""
        pipeline_id = pipeline["id"]
        prev_run_id = file_store.get_latest_run_id(
            pipeline_id, exclude=run_dir.name
        )
        if prev_run_id is None:
            return
        prev_dir = file_store.make_run_dir(pipeline_id, prev_run_id)
        for f in prev_dir.glob(f"{node_id}*"):
            dest = run_dir / f.name
            if not dest.exists():
                try:
                    os.link(f, dest)
                except OSError:
                    # Fallback: copy if hard-link fails (cross-device, etc.)
                    import shutil

                    shutil.copy2(f, dest)

    # ── Condition helpers ────────────────────────────────────────

    @staticmethod
    def _has_conditions(node_id: str, pipeline: dict) -> bool:
        """Check whether any incoming edge to *node_id* carries a condition."""
        edges = pipeline.get("edges", [])
        return any(
            e.get("condition") for e in edges if e["target"] == node_id
        )

    @staticmethod
    def _gather_conditions(
        node_id: str, pipeline: dict
    ) -> list[dict]:
        """Return incoming edge conditions for inclusion in the sandbox manifest.

        Conditions are opaque strings — they are evaluated inside the
        sandbox container, never on the backend host.
        """
        edges = pipeline.get("edges", [])
        conditions: list[dict] = []
        for edge in edges:
            if edge["target"] != node_id:
                continue
            cond = edge.get("condition")
            if cond:
                conditions.append({
                    "source_id": edge["source"],
                    "condition": cond,
                })
        return conditions

    # ── Downstream set ───────────────────────────────────────────

    @staticmethod
    def _downstream_set(node_id: str, pipeline: dict) -> set[str]:
        """Return the set of all nodes reachable downstream from *node_id*."""
        adj: dict[str, list[str]] = defaultdict(list)
        for edge in pipeline.get("edges", []):
            adj[edge["source"]].append(edge["target"])

        visited: set[str] = set()
        stack = list(adj.get(node_id, []))
        while stack:
            nid = stack.pop()
            if nid in visited:
                continue
            visited.add(nid)
            stack.extend(adj.get(nid, []))
        return visited

    # ── Node execution ───────────────────────────────────────────

    def _get_docker(self) -> docker.DockerClient:
        if self._docker is None:
            self._docker = docker.from_env()
        return self._docker

    def _build_inputs(
        self, node_id: str, pipeline: dict, run_dir: Path
    ) -> dict[str, str]:
        """Resolve input port paths from upstream node outputs."""
        inputs: dict[str, str] = {}
        node_map = {n["id"]: n for n in pipeline["nodes"]}
        edges = pipeline.get("edges", [])

        for edge in edges:
            if edge["target"] != node_id:
                continue
            source_id = edge["source"]
            source_port = edge.get("source_port", "output")
            target_port = edge.get("target_port", "input")

            # Look for any file matching source output pattern
            pattern = f"{source_id}_{source_port}*"
            matches = [
                f for f in run_dir.glob(pattern)
                if not _is_sidecar_file(f)
            ]
            if not matches:
                # Try simple node_id pattern
                pattern = f"{source_id}_output*"
                matches = [
                    f for f in run_dir.glob(pattern)
                    if not _is_sidecar_file(f)
                ]

            if matches:
                # Use relative path — the sandbox runner resolves from manifest_path.parent
                # which is the run_dir inside the sandbox container.
                host_path = matches[0]
                sandbox_vol_root = Path("/ml_data")
                rel_run = run_dir.relative_to(DATA_DIR)
                sandbox_run_dir = sandbox_vol_root / rel_run
                container_path = str(sandbox_run_dir / host_path.name)
                inputs[target_port] = container_path

        return inputs

    @staticmethod
    def _ensure_sandbox_permissions(
        run_dir: Path, *files: Path
    ) -> None:
        """Make run_dir writable and parent dirs traversable for the sandbox.

        The sandbox container may run as a different UID than the host
        process (especially with bind mounts in tests).  Walk from run_dir
        up to DATA_DIR setting 0o755, set run_dir to 0o777 (container
        writes results here), and individual files to 0o644.
        """
        # run_dir needs to be world-writable so the container can create result files
        run_dir.chmod(0o777)

        # Walk up to DATA_DIR making each directory world-traversable
        current = run_dir.parent
        data_dir_resolved = DATA_DIR.resolve()
        while current.resolve() != data_dir_resolved and len(str(current)) > 1:
            try:
                current.chmod(0o755)
            except OSError:
                break
            current = current.parent
        # DATA_DIR itself
        try:
            DATA_DIR.chmod(0o755)
        except OSError:
            pass

        for f in files:
            f.chmod(0o644)

    def _execute_node(
        self,
        node_id: str,
        pipeline: dict,
        run_dir: Path,
    ) -> str:
        """Write manifest and run a single node in a Docker container."""
        node = next(n for n in pipeline["nodes"] if n["id"] == node_id)

        inputs = self._build_inputs(node_id, pipeline, run_dir)

        conditions = self._gather_conditions(node_id, pipeline)

        # Params can be either a dict of values {"rows": 100} (after frontend save)
        # or a list of param definitions [{"name": "rows", "default": 100, ...}]
        # (right after add_node before frontend auto-save). Normalize to a values dict.
        raw_params = node.get("params", {})
        if isinstance(raw_params, list):
            params = {p["name"]: p.get("default") for p in raw_params if "name" in p}
        else:
            params = raw_params

        # Translate host DATA_DIR paths in param values to sandbox container paths.
        # Uploaded files get stored under DATA_DIR with host paths, but the sandbox
        # mounts the data volume at /ml_data/.
        data_dir_str = str(DATA_DIR)
        sandbox_data_root = "/ml_data"
        params = _translate_params_for_sandbox(params, data_dir_str, sandbox_data_root)

        # Extract entry function name from node type (e.g. "ml_toolbox.nodes.demo.clean_data" -> "clean_data")
        entry_fn = node.get("type", "run").rsplit(".", 1)[-1]

        # Build output type map so the runner can auto-serialize (e.g. MODEL → joblib)
        output_types: dict[str, str] = {}
        for port in node.get("outputs", []):
            output_types[port["name"]] = port.get("type", "VALUE")

        manifest = {
            "node_id": node_id,
            "code": node.get("code", ""),
            "entry_fn": entry_fn,
            "inputs": inputs,
            "params": params,
            "conditions": conditions,
            "output_types": output_types,
        }

        manifest_path = run_dir / f"{node_id}_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

        # Write params hash for caching
        hash_file = run_dir / f"{node_id}.hash"
        hash_file.write_text(self._params_hash(node))

        # Ensure the sandbox container can read the manifest and write results.
        # When using bind mounts (e.g. in tests), the container process may run
        # as a different UID, so we make directories traversable and the run_dir
        # writable, and files world-readable.
        self._ensure_sandbox_permissions(run_dir, manifest_path, hash_file)

        client = self._get_docker()

        # Docker-in-Docker volume mounting:
        # The backend container's DATA_DIR is backed by a named Docker volume.
        # We mount that entire volume into the sandbox and compute correct paths.
        sandbox_vol_root = Path("/ml_data")
        rel_run = run_dir.relative_to(DATA_DIR)
        sandbox_run_dir = sandbox_vol_root / rel_run
        container_manifest = str(sandbox_run_dir / manifest_path.name)

        try:
            container = client.containers.run(
                image=SANDBOX_IMAGE,
                command=["python", "/sandbox/runner.py", container_manifest],
                volumes={DOCKER_VOLUME_NAME: {"bind": str(sandbox_vol_root), "mode": "rw"}},
                network_disabled=True,
                mem_limit="1g",
                nano_cpus=1_000_000_000,
                pids_limit=64,
                read_only=True,
                tmpfs={"/tmp": "size=128m"},
                cap_drop=["ALL"],
                security_opt=["no-new-privileges"],
                detach=True,
            )
        except ImageNotFound:
            raise RuntimeError(
                f"Sandbox image '{SANDBOX_IMAGE}' not found. "
                "Build it with: docker build -t ml-toolbox-sandbox backend/sandbox/"
            )

        with self._lock:
            self._current_container = container

        try:
            result = container.wait(timeout=300)
            exit_code = result.get("StatusCode", -1)
        except requests.exceptions.Timeout as exc:
            try:
                container.stop(timeout=5)
            except Exception:
                pass
            raise RuntimeError(
                "Node execution timed out after 5 minutes. "
                "Check your code for infinite loops or reduce the input data size."
            ) from exc
        except Exception:
            try:
                container.stop(timeout=5)
            except Exception:
                pass
            raise
        finally:
            # Capture logs before removing
            try:
                logs = container.logs().decode("utf-8", errors="replace")
                if logs.strip():
                    log_path = run_dir / f"{node_id}_logs.txt"
                    log_path.write_text(logs)
            except Exception:
                pass
            try:
                container.remove(force=True)
            except Exception:
                pass
            with self._lock:
                self._current_container = None

        if exit_code == 137:
            raise RuntimeError(
                "Node was killed — likely out of memory (1GB limit). "
                "Try reducing the input data size or simplifying the computation."
            )

        if exit_code != 0:
            error_path = run_dir / f"{node_id}_manifest_error.json"
            error_msg = "Node execution failed"
            if error_path.exists():
                err = json.loads(error_path.read_text())
                error_msg = err.get("error", error_msg)
            raise RuntimeError(error_msg)

        # Check if the sandbox decided to skip (condition not met)
        result_path = run_dir / f"{node_id}_manifest_result.json"
        if result_path.exists():
            try:
                result_data = json.loads(result_path.read_text())
                if result_data.get("skipped"):
                    return "skipped"
            except (json.JSONDecodeError, OSError):
                pass

        return "done"

    # ── Orchestration ────────────────────────────────────────────

    def _execute_ordered(
        self,
        order: list[str],
        pipeline: dict,
        run_dir: Path,
        run_id: str,
    ) -> None:
        """Execute nodes in the given order, broadcasting status updates."""
        pipeline_id = pipeline["id"]
        node_map = {n["id"]: n for n in pipeline["nodes"]}

        # Write run status
        status_path = run_dir / "_status.json"
        started_at = datetime.now(timezone.utc).isoformat()
        status_path.write_text(json.dumps({
            "status": "running",
            "run_id": run_id,
            "started_at": started_at,
        }))

        had_error = False
        final_status = "done"
        try:
            for node_id in order:
                if self._cancelled.is_set():
                    self._broadcast(pipeline_id, {
                        "node_id": node_id,
                        "status": "skipped",
                        "run_id": run_id,
                    })
                    continue

                node = node_map.get(node_id)
                if node is None:
                    continue

                # Check cache (only if node has no conditions — conditioned
                # nodes always run in the sandbox so the condition can be
                # evaluated securely).
                if not self._has_conditions(node_id, pipeline) and self._is_cached(node_id, node, run_dir):
                    self._broadcast(pipeline_id, {
                        "node_id": node_id,
                        "status": "done",
                        "run_id": run_id,
                        "cached": True,
                    })
                    continue

                # Update current node in status file
                status_path.write_text(json.dumps({
                    "status": "running",
                    "run_id": run_id,
                    "current_node_id": node_id,
                }))

                self._broadcast(pipeline_id, {
                    "node_id": node_id,
                    "status": "running",
                    "run_id": run_id,
                })

                try:
                    node_result = self._execute_node(node_id, pipeline, run_dir)

                    if node_result == "skipped":
                        # Sandbox evaluated conditions and decided to skip
                        self._broadcast(pipeline_id, {
                            "node_id": node_id,
                            "status": "skipped",
                            "run_id": run_id,
                        })
                        continue

                    # Collect output files
                    outputs = [
                        f.name
                        for f in run_dir.glob(f"{node_id}_*")
                        if not f.name.endswith((".json", ".hash", ".txt"))
                    ]

                    self._broadcast(pipeline_id, {
                        "node_id": node_id,
                        "status": "done",
                        "run_id": run_id,
                        "outputs": outputs,
                    })

                    # Auto-infer schema for ingest nodes + EDA context propagation
                    _post_execution_hook(
                        pipeline_id=pipeline_id,
                        node_id=node_id,
                        node_type=node.get("type", ""),
                        run_dir=run_dir,
                        broadcast=self._broadcast,
                        pipeline_data=pipeline,
                    )
                except Exception as exc:
                    had_error = True
                    tb = str(exc)
                    self._broadcast(pipeline_id, {
                        "node_id": node_id,
                        "status": "error",
                        "run_id": run_id,
                        "traceback": tb,
                    })
                    # Stop executing further nodes on error
                    break

            if self._cancelled.is_set():
                final_status = "cancelled"
            elif had_error:
                final_status = "error"
            else:
                final_status = "done"
        except Exception:
            final_status = "error"
            raise
        finally:
            completed_at = datetime.now(timezone.utc).isoformat()
            status_path.write_text(json.dumps({
                "status": final_status,
                "run_id": run_id,
                "started_at": started_at,
                "completed_at": completed_at,
            }))


# ── Module-level singleton state ─────────────────────────────────

_active_executors: dict[str, PipelineExecutor] = {}
_executors_lock = threading.Lock()


def get_active_executor(pipeline_id: str) -> PipelineExecutor | None:
    with _executors_lock:
        return _active_executors.get(pipeline_id)


def try_set_active_executor(
    pipeline_id: str, executor: PipelineExecutor
) -> bool:
    """Atomically set the active executor only if none is currently running.

    Returns True if the executor was set, False if one is already active.
    This prevents the TOCTOU race between checking and setting.
    """
    with _executors_lock:
        if pipeline_id in _active_executors:
            return False
        _active_executors[pipeline_id] = executor
        return True


def set_active_executor(pipeline_id: str, executor: PipelineExecutor) -> None:
    with _executors_lock:
        _active_executors[pipeline_id] = executor


def remove_active_executor(pipeline_id: str) -> None:
    with _executors_lock:
        _active_executors.pop(pipeline_id, None)


# ── Post-execution hook: auto schema inference ───────────────────────


def _post_execution_hook(
    pipeline_id: str,
    node_id: str,
    node_type: str,
    run_dir: Path,
    broadcast: Callable[[str, dict], None],
    pipeline_data: dict | None = None,
) -> None:
    """Trigger background tasks after node execution."""
    # Schema inference for ingest nodes
    if ".ingest." in node_type.lower():
        threading.Thread(
            target=_infer_schema_background,
            args=(pipeline_id, node_id, run_dir, broadcast),
            daemon=True,
        ).start()

    # EDA context propagation: after an EDA node runs, propagate .eda-context.json
    # to all sibling downstream nodes of the parent, then re-auto-configure them.
    if ".eda." in node_type.lower() and pipeline_data is not None:
        threading.Thread(
            target=_propagate_eda_context,
            args=(pipeline_id, node_id, run_dir, pipeline_data, broadcast),
            daemon=True,
        ).start()

    # Output analysis via subprocess Claude Code for all nodes
    threading.Thread(
        target=_analyze_output_background,
        args=(pipeline_id, node_id, node_type, run_dir, broadcast),
        daemon=True,
    ).start()


def _propagate_eda_context(
    pipeline_id: str,
    eda_node_id: str,
    run_dir: Path,
    pipeline_data: dict,
    broadcast: Callable[[str, dict], None],
) -> None:
    """After an EDA node runs, propagate .eda-context.json to sibling downstream nodes.

    EDA nodes write .eda-context.json alongside their input file (the parent's
    output).  But that sidecar never reaches sibling downstream nodes because
    the sandbox runner only propagates sidecars during node execution, and the
    parent has already finished by the time EDA writes.

    This function copies the context file to every downstream sibling's expected
    input location, walks deeper into the DAG, and re-triggers auto-configure
    on each affected node.
    """
    import shutil

    try:
        edges = pipeline_data.get("edges", [])

        # 1. Find parent node: who feeds into this EDA node?
        eda_input_edges = [e for e in edges if e["target"] == eda_node_id]
        if not eda_input_edges:
            return
        parent_id = eda_input_edges[0]["source"]
        parent_port = eda_input_edges[0].get("source_port", "output")

        # 2. Find the .eda-context.json that EDA just wrote (alongside parent's output)
        eda_context_files = list(
            run_dir.glob(f"{parent_id}_{parent_port}*.eda-context.json")
        )
        if not eda_context_files:
            eda_context_files = list(
                run_dir.glob(f"{parent_id}*.eda-context.json")
            )
        if not eda_context_files:
            return
        eda_context_file = eda_context_files[0]

        # 3. Find all downstream edges from parent (excluding this EDA node)
        parent_downstream = [
            e for e in edges
            if e["source"] == parent_id and e["target"] != eda_node_id
        ]

        # 4. Copy .eda-context.json alongside each sibling's input parquet
        for edge in parent_downstream:
            source_port = edge.get("source_port", "output")
            dest_pattern = f"{parent_id}_{source_port}"
            dest_candidates = list(run_dir.glob(f"{dest_pattern}*.parquet"))
            if dest_candidates:
                dest = dest_candidates[0].with_suffix(".eda-context.json")
                shutil.copy2(str(eda_context_file), str(dest))

        # 5. Walk deeper into the DAG — propagate to grandchildren and beyond
        visited: set[str] = set()
        queue = [e["target"] for e in parent_downstream]
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            # Copy alongside this node's output parquets
            for output_file in run_dir.glob(f"{current}*.parquet"):
                if _is_sidecar_file(output_file):
                    continue
                dest = output_file.with_suffix(".eda-context.json")
                if not dest.exists():
                    shutil.copy2(str(eda_context_file), str(dest))
            # Enqueue this node's children
            for e in edges:
                if e["source"] == current:
                    queue.append(e["target"])

        # 6. Broadcast so the frontend knows context was updated
        broadcast(pipeline_id, {
            "type": "eda_context_updated",
            "source_node_id": eda_node_id,
        })

        # 7. Re-trigger auto-configure on each affected downstream node
        #    Import lazily to avoid circular dependency (pipelines → executor).
        from ml_toolbox.routers.pipelines import _auto_configure_node

        for target_id in visited:
            _auto_configure_node(pipeline_id, target_id)

    except Exception:
        logger.exception(
            "Failed to propagate EDA context from node %s", eda_node_id,
        )


def _infer_schema_background(
    pipeline_id: str,
    node_id: str,
    run_dir: Path,
    broadcast: Callable[[str, dict], None],
) -> None:
    """Background schema inference — heuristic analysis → cast → .meta.json."""
    import pandas as pd

    from ml_toolbox.llm.metadata import (
        build_metadata_from_heuristics,
        cast_by_metadata,
        heuristic_profile,
    )

    # Find the output parquet file(s) for this node
    output_files = list(run_dir.glob(f"{node_id}*.parquet"))
    if not output_files:
        return

    output_file = output_files[0]

    try:
        df = pd.read_parquet(output_file)
        profiles = heuristic_profile(df)
        metadata = build_metadata_from_heuristics(
            profiles, row_count=len(df), node_id=node_id,
        )

        # Store original source path so re-cast can recover dropped columns
        manifest_path = run_dir / f"{node_id}_manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text())
                source = manifest.get("params", {}).get("path", "")
                if source:
                    # Translate sandbox path back to host path
                    sandbox_root = "/ml_data"
                    if source.startswith(sandbox_root):
                        source = str(DATA_DIR) + source[len(sandbox_root):]
                    metadata["source_path"] = source
            except Exception:
                pass

        # Cast columns based on inferred types
        df, cast_results = cast_by_metadata(df, metadata)

        # Write cast_status into metadata
        for col_name, result in cast_results.items():
            if col_name in metadata.get("columns", {}):
                metadata["columns"][col_name]["cast_status"] = result.get("status")
                if result.get("reason"):
                    metadata["columns"][col_name]["cast_reason"] = result["reason"]

        # Write .meta.json alongside the parquet file
        meta_path = output_file.with_suffix(".meta.json")
        meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

        # Rewrite parquet with correct types
        df.to_parquet(output_file, index=False)

        # Notify frontend via WebSocket (instant heuristic results)
        broadcast(pipeline_id, {
            "type": "metadata_updated",
            "node_id": node_id,
        })
        logger.info("Auto schema inference completed for node %s", node_id)

        # Pass 2: LLM refinement (few seconds, non-blocking for initial display)
        if _refine_metadata_with_llm(meta_path, pipeline_id):
            # Re-cast if LLM changed semantic types
            updated_metadata = json.loads(meta_path.read_text())
            df_reread = pd.read_parquet(output_file)
            df_reread, _ = cast_by_metadata(df_reread, updated_metadata)
            df_reread.to_parquet(output_file, index=False)

            broadcast(pipeline_id, {
                "type": "metadata_updated",
                "node_id": node_id,
            })
            logger.info("LLM-refined metadata for node %s", node_id)
    except Exception as e:
        logger.warning("Schema inference failed for %s: %s", node_id, e)


def _refine_metadata_with_llm(meta_path: Path, pipeline_id: str) -> bool:
    """Refine heuristic metadata using ``claude -p``.

    Returns True if any column was updated, False otherwise.
    """
    import shutil
    import subprocess

    if shutil.which("claude") is None:
        return False

    try:
        metadata = json.loads(meta_path.read_text())
    except Exception:
        return False

    columns_summary: list[str] = []
    for name, meta in metadata.get("columns", {}).items():
        stats_str = (
            f"unique={meta.get('unique_count', '?')}, "
            f"unique_ratio={meta.get('unique_ratio', '?')}, "
            f"null_pct={meta.get('null_pct', '?')}"
        )
        sample = meta.get("sample_values", [])
        columns_summary.append(
            f"{name}: dtype={meta['dtype']}, semantic_type={meta.get('semantic_type', '?')}, "
            f"role={meta.get('role', '?')}, {stats_str}, samples={sample}"
        )

    prompt = f"""Review these column classifications and fix any errors.

Columns:
{chr(10).join(columns_summary)}

Rules:
- Column named 'ID', 'id', 'index' with unique_ratio near 1.0 → role=identifier
- Column with name containing 'target', 'label', 'default', 'churn', 'survived' and binary values → role=target
- Integer columns with <=20 unique values → likely categorical or ordinal, not continuous
- Columns with very low unique count relative to rows → not suitable for continuous analysis

Return ONLY a JSON object with corrections. Only include columns that need changes:
{{"column_name": {{"semantic_type": "...", "role": "..."}}, ...}}
Return {{}} if all classifications are correct."""

    try:
        project_dir = DATA_DIR / "projects" / pipeline_id
        result = subprocess.run(
            ["claude", "-p", prompt, "--output-format", "json"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(project_dir),
        )
        if result.returncode != 0:
            logger.warning("LLM metadata refinement failed (rc=%s)", result.returncode)
            return False

        raw = result.stdout.strip()
        if not raw:
            return False

        # Parse envelope from --output-format json
        corrections: dict[str, Any] | None = None
        try:
            envelope = json.loads(raw)
            if isinstance(envelope, dict) and "result" in envelope:
                inner = envelope["result"]
            else:
                inner = raw
        except (json.JSONDecodeError, TypeError):
            inner = raw

        # Strip markdown fences if present
        if isinstance(inner, str):
            stripped = inner.strip()
            if stripped.startswith("```"):
                lines = stripped.split("\n")
                lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                stripped = "\n".join(lines)
            corrections = json.loads(stripped)
        else:
            corrections = inner

        if not corrections or not isinstance(corrections, dict):
            return False

        updated = False
        for col_name, fixes in corrections.items():
            if col_name in metadata["columns"] and isinstance(fixes, dict):
                for key in ("semantic_type", "role"):
                    if key in fixes:
                        metadata["columns"][col_name][key] = fixes[key]
                        updated = True
                metadata["columns"][col_name]["refined_by"] = "llm"

        if updated:
            metadata["generated_by"] = "auto-heuristic+llm"
            meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

        return updated
    except Exception as e:
        logger.warning("LLM metadata refinement failed: %s", e)
        return False


def _analyze_output_background(
    pipeline_id: str,
    node_id: str,
    node_type: str,
    run_dir: Path,
    broadcast: Callable[[str, dict], None],
) -> None:
    """Spawn a short-lived Claude Code subprocess to analyze node output."""
    import shutil
    import subprocess

    # Check if claude CLI is available
    if shutil.which("claude") is None:
        logger.debug("claude CLI not found, skipping output analysis for %s", node_id)
        return

    # Find output files (exclude sidecars)
    output_files = [
        f for f in run_dir.glob(f"{node_id}*")
        if not _is_sidecar_file(f)
    ]
    if not output_files:
        return

    output_file = output_files[0]

    # Read output content based on file type
    try:
        if output_file.suffix == ".json":
            output_content = output_file.read_text()[:5000]
        elif output_file.suffix == ".parquet":
            import pandas as pd

            df = pd.read_parquet(output_file)
            output_content = (
                f"Parquet: {len(df)} rows, {len(df.columns)} cols.\n"
                f"Columns: {list(df.columns)}\n"
                f"Dtypes:\n{df.dtypes.to_string()}\n"
                f"Head:\n{df.head(5).to_string()}"
            )[:5000]
        else:
            output_content = f"File: {output_file.name}, size: {output_file.stat().st_size}"
    except Exception as e:
        logger.warning("Failed to read output for analysis %s: %s", node_id, e)
        return

    # Read .meta.json if available
    meta_content = ""
    meta_files = list(run_dir.glob(f"{node_id}*.meta.json"))
    if meta_files:
        try:
            meta_content = meta_files[0].read_text()[:2000]
        except Exception:
            pass

    prompt = f"""You are analyzing a pipeline node's output. Use the project context from CLAUDE.md.

Node type: {node_type}
Node function: {node_type.rsplit('.', 1)[-1]}
Output file: {output_file.name}

## Output Report
{output_content}

## Dataset Metadata
{meta_content if meta_content else 'No metadata available — check runs/ directory for .meta.json files'}

## Your Task
Analyze this output and provide a concise summary for the user.

1. **Key Findings** — 2-3 most important observations from this output
2. **Warnings** — anything the user should pay attention to (data quality issues, unexpected patterns, potential problems for downstream modeling)
3. **Suggested Next Steps** — what should the user do after seeing this output

Rules:
- Each finding must be ONE sentence, max 20 words
- Each warning message must be ONE sentence, max 25 words
- Summary must be ONE sentence, max 30 words
- Suggestions must be actionable and specific, ONE sentence each
- Do NOT repeat information that is already visible in the data table
- Focus on insights the user cannot easily see themselves

Be specific — reference actual column names, values, and statistics from the output.

Return ONLY valid JSON (no markdown fences, no extra text):
{{"summary": "One-sentence overall summary", "findings": ["Finding 1 with specific details", "Finding 2"], "warnings": [{{"type": "high|medium|low", "column": "col_name or null", "message": "Specific warning"}}], "suggestions": ["Specific actionable suggestion 1", "Suggestion 2"]}}"""

    try:
        result = subprocess.run(
            ["claude", "-p", prompt, "--output-format", "json"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(DATA_DIR / "projects" / pipeline_id),
        )
        if result.returncode != 0:
            stderr = result.stderr.strip() if result.stderr else ""
            if "auth" in stderr.lower() or "login" in stderr.lower():
                logger.warning(
                    "claude -p auth failed for %s — run 'claude login' in the backend container",
                    node_id,
                )
            else:
                logger.warning(
                    "claude -p failed for %s (rc=%s): %s",
                    node_id, result.returncode, stderr[:200],
                )
            return

        if result.stdout.strip():
            raw = result.stdout.strip()

            # The claude --output-format json wraps the response in a JSON
            # envelope with a "result" field. Extract the inner text.
            try:
                envelope = json.loads(raw)
                if isinstance(envelope, dict) and "result" in envelope:
                    inner = envelope["result"]
                else:
                    inner = raw
            except (json.JSONDecodeError, TypeError):
                inner = raw

            # The inner text may contain markdown fences — strip them.
            if isinstance(inner, str):
                stripped = inner.strip()
                if stripped.startswith("```"):
                    lines = stripped.split("\n")
                    # Remove first line (```json) and last line (```)
                    lines = lines[1:]
                    if lines and lines[-1].strip() == "```":
                        lines = lines[:-1]
                    stripped = "\n".join(lines)
                try:
                    analysis = json.loads(stripped)
                except json.JSONDecodeError:
                    logger.warning(
                        "Output analysis returned invalid JSON for %s", node_id,
                    )
                    return
            else:
                analysis = inner

            # Validate structure
            if not isinstance(analysis, dict):
                logger.warning("Output analysis not a dict for %s", node_id)
                return

            # Write .analysis.json alongside the output file
            analysis_path = output_file.with_suffix(".analysis.json")
            analysis_path.write_text(json.dumps(analysis, indent=2))

            broadcast(pipeline_id, {
                "type": "analysis_updated",
                "node_id": node_id,
            })
            logger.info("Output analysis completed for node %s", node_id)
    except subprocess.TimeoutExpired:
        logger.warning("Output analysis timed out for %s", node_id)
    except Exception as e:
        logger.warning("Output analysis failed for %s: %s", node_id, e)
