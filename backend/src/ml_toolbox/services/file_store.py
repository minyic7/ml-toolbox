"""File store for pipeline run outputs."""

from __future__ import annotations

import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from ml_toolbox.config import DATA_DIR

PROJECTS_DIR = DATA_DIR / "projects"

# Allowed characters for IDs used in path construction
_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_path_id(value: str, label: str = "ID") -> None:
    """Reject IDs that could escape the expected directory."""
    if not value or not _SAFE_ID_RE.match(value):
        raise ValueError(
            f"Invalid {label}: must be non-empty and contain only "
            f"alphanumeric characters, hyphens, or underscores"
        )


def _runs_dir(pipeline_id: str) -> Path:
    _validate_path_id(pipeline_id, "pipeline_id")
    return PROJECTS_DIR / pipeline_id / "runs"


def make_run_dir(pipeline_id: str, run_id: str) -> Path:
    _validate_path_id(pipeline_id, "pipeline_id")
    _validate_path_id(run_id, "run_id")
    run_dir = _runs_dir(pipeline_id) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def list_runs(pipeline_id: str) -> list[dict]:
    runs_dir = _runs_dir(pipeline_id)
    if not runs_dir.exists():
        return []
    runs: list[tuple[float, dict]] = []
    for d in runs_dir.iterdir():
        if d.is_dir():
            ctime = d.stat().st_mtime
            runs.append((ctime, {
                "run_id": d.name,
                "created_at": datetime.fromtimestamp(ctime, tz=timezone.utc).isoformat(),
            }))
    runs.sort(key=lambda x: x[0], reverse=True)
    return [r[1] for r in runs]


def delete_run(pipeline_id: str, run_id: str) -> None:
    _validate_path_id(run_id, "run_id")
    run_dir = _runs_dir(pipeline_id) / run_id
    # Extra safety: ensure resolved path is within the runs directory
    if not run_dir.resolve().is_relative_to(_runs_dir(pipeline_id).resolve()):
        raise ValueError("Invalid run_id: path traversal detected")
    shutil.rmtree(run_dir)


def get_output_path(pipeline_id: str, run_id: str, node_id: str, ext: str) -> Path:
    return _runs_dir(pipeline_id) / run_id / f"{node_id}.{ext}"


def output_exists(pipeline_id: str, run_id: str, node_id: str) -> bool:
    run_dir = _runs_dir(pipeline_id) / run_id
    if not run_dir.exists():
        return False
    return any(run_dir.glob(f"{node_id}.*"))


def get_latest_run_id(
    pipeline_id: str, exclude: str | None = None
) -> str | None:
    runs_dir = _runs_dir(pipeline_id)
    if not runs_dir.exists():
        return None
    dirs = [
        d for d in runs_dir.iterdir()
        if d.is_dir() and (exclude is None or d.name != exclude)
    ]
    if not dirs:
        return None
    latest = max(dirs, key=lambda d: d.stat().st_mtime)
    return latest.name


def cleanup_run_dir(pipeline_id: str, run_id: str) -> None:
    _validate_path_id(run_id, "run_id")
    run_dir = _runs_dir(pipeline_id) / run_id
    if run_dir.exists():
        shutil.rmtree(run_dir)
