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
            matches = list(run_dir.glob(pattern))
            if not matches:
                # Try simple node_id pattern
                pattern = f"{source_id}_output*"
                matches = list(run_dir.glob(pattern))

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
