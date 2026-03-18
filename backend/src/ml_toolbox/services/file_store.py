"""File store for pipeline run outputs."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from ml_toolbox.config import DATA_DIR

PROJECTS_DIR = DATA_DIR / "projects"


def _runs_dir(pipeline_id: str) -> Path:
    return PROJECTS_DIR / pipeline_id / "runs"


def make_run_dir(pipeline_id: str, run_id: str) -> Path:
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
    run_dir = _runs_dir(pipeline_id) / run_id
    shutil.rmtree(run_dir)


def get_output_path(pipeline_id: str, run_id: str, node_id: str, ext: str) -> Path:
    return _runs_dir(pipeline_id) / run_id / f"{node_id}.{ext}"


def output_exists(pipeline_id: str, run_id: str, node_id: str) -> bool:
    run_dir = _runs_dir(pipeline_id) / run_id
    if not run_dir.exists():
        return False
    return any(run_dir.glob(f"{node_id}.*"))


def get_latest_run_id(pipeline_id: str) -> str | None:
    runs_dir = _runs_dir(pipeline_id)
    if not runs_dir.exists():
        return None
    dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not dirs:
        return None
    latest = max(dirs, key=lambda d: d.stat().st_mtime)
    return latest.name


def cleanup_run_dir(pipeline_id: str, run_id: str) -> None:
    run_dir = _runs_dir(pipeline_id) / run_id
    if run_dir.exists():
        shutil.rmtree(run_dir)
