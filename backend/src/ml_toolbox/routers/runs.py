"""Global runs endpoint – returns runs across all pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

from ml_toolbox.services import file_store, store

router = APIRouter(prefix="/api/runs")

# File extensions we surface as artifacts
_ARTIFACT_EXTENSIONS = frozenset({
    ".parquet", ".pkl", ".json", ".npy", ".png", ".svg",
})
# Extensions that are metadata, not artifacts
_META_EXTENSIONS = frozenset({".hash", ".txt"})

# Chart-type artifacts that may carry bars metadata
_CHART_TYPES = frozenset({"png", "svg"})


def _node_status_from_run_dir(run_dir: Path, node_id: str) -> str:
    """Infer a node's status from files present in the run directory."""
    error_file = run_dir / f"{node_id}_manifest_error.json"
    if error_file.exists():
        return "error"
    # Check for any output file produced by this node
    outputs = [
        f for f in run_dir.iterdir()
        if f.name.startswith(f"{node_id}_") or f.name.startswith(f"{node_id}.")
        if not f.name.endswith((".json", ".hash", ".txt"))
    ]
    if outputs:
        return "done"
    return "pending"


def _build_dag_snapshot(pipeline_data: dict, run_dir: Path) -> list[dict]:
    """Build a snapshot of node states from pipeline definition + run outputs."""
    snapshot = []
    for node in pipeline_data.get("nodes", []):
        node_id = node["id"]
        # Derive a short name: custom name > type's last segment
        node_name = node.get("name") or node.get("type", "").rsplit(".", 1)[-1]
        node_type = node.get("type", "")
        status = _node_status_from_run_dir(run_dir, node_id)
        snapshot.append({
            "node_id": node_id,
            "node_name": node_name,
            "node_type": node_type,
            "status": status,
        })
    return snapshot


def _build_artifacts(pipeline_data: dict, run_dir: Path) -> list[dict]:
    """Collect output artifacts from a run directory."""
    node_map = {n["id"]: n for n in pipeline_data.get("nodes", [])}
    artifacts: list[dict[str, Any]] = []

    for f in sorted(run_dir.iterdir()):
        if not f.is_file():
            continue
        ext = f.suffix.lower()
        if ext in _META_EXTENSIONS or f.name.startswith("_"):
            continue
        if ext not in _ARTIFACT_EXTENSIONS:
            continue
        # Skip manifest / error sidecar JSONs (e.g. n1_manifest.json,
        # n1_manifest_error.json, n1_bars.json)
        if ext == ".json" and any(
            tag in f.stem for tag in ("manifest", "_bars", "_error")
        ):
            continue

        # Determine which node produced this file
        # File naming: {node_id}_output.ext or {node_id}.ext
        stem = f.stem  # e.g. "abc123_output" or "abc123"
        node_id = None
        for nid in node_map:
            if stem == nid or stem.startswith(f"{nid}_"):
                node_id = nid
                break
        if node_id is None:
            continue

        node = node_map[node_id]
        node_name = node.get("name") or node.get("type", "").rsplit(".", 1)[-1]
        file_type = ext.lstrip(".")

        artifact: dict[str, Any] = {
            "node_id": node_id,
            "node_name": node_name,
            "filename": f.name,
            "type": file_type,
            "size": f.stat().st_size,
        }

        # For chart types, check for a bars sidecar JSON
        if file_type in _CHART_TYPES:
            bars_file = run_dir / f"{f.stem}_bars.json"
            if bars_file.exists():
                try:
                    artifact["bars"] = json.loads(bars_file.read_text())
                except Exception:
                    pass

        artifacts.append(artifact)

    return artifacts


def _read_status(run_dir: Path) -> dict:
    """Read _status.json from a run directory, returning {} on failure."""
    status_file = run_dir / "_status.json"
    if not status_file.exists():
        return {}
    try:
        return json.loads(status_file.read_text())
    except Exception:
        return {}


@router.get("")
async def list_all_runs(
    pipeline_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """Return runs across all pipelines, sorted by created_at desc."""
    pipelines = store.list_all()

    # If filtering by pipeline_id, only load that pipeline
    if pipeline_id is not None:
        pipelines = [p for p in pipelines if p.get("id") == pipeline_id]

    # Collect all runs with metadata
    all_runs: list[tuple[float, dict]] = []

    for pipeline_data in pipelines:
        pid = pipeline_data["id"]
        pipeline_name = pipeline_data.get("name", "")
        runs_dir = file_store._runs_dir(pid)
        if not runs_dir.exists():
            continue

        for run_dir in runs_dir.iterdir():
            if not run_dir.is_dir() or run_dir.name.startswith("_"):
                continue

            run_id = run_dir.name
            stat = run_dir.stat()
            started_at = stat.st_mtime

            # Apply search filter (run_id prefix match)
            if search and not run_id.startswith(search):
                continue

            status_data = _read_status(run_dir)
            run_status = status_data.get("status", "unknown")

            # Apply status filter
            if status and run_status != status:
                continue

            # Compute duration and completed_at from status file timestamps
            completed_at = None
            duration = None
            status_file = run_dir / "_status.json"
            if run_status in ("done", "error", "cancelled") and status_file.exists():
                end_time = status_file.stat().st_mtime
                completed_at = end_time
                duration = round(end_time - started_at, 2)

            dag_snapshot = _build_dag_snapshot(pipeline_data, run_dir)
            artifacts = _build_artifacts(pipeline_data, run_dir)

            from datetime import datetime, timezone

            run_record: dict[str, Any] = {
                "id": run_id,
                "pipeline_id": pid,
                "pipeline_name": pipeline_name,
                "status": run_status,
                "started_at": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
                "completed_at": (
                    datetime.fromtimestamp(completed_at, tz=timezone.utc).isoformat()
                    if completed_at is not None
                    else None
                ),
                "duration": duration,
                "dag_snapshot": dag_snapshot,
                "artifacts": artifacts,
            }
            all_runs.append((started_at, run_record))

    # Sort by created_at descending
    all_runs.sort(key=lambda x: x[0], reverse=True)

    # Apply pagination
    page = all_runs[offset : offset + limit]
    return [r[1] for r in page]
