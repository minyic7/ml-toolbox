"""JSON file-based pipeline store."""

from __future__ import annotations

import json
import shutil
import threading
from collections import defaultdict
from pathlib import Path

from ml_toolbox.config import DATA_DIR

PROJECTS_DIR = DATA_DIR / "projects"

_pipeline_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)


def pipeline_lock(pipeline_id: str) -> threading.Lock:
    """Return a per-pipeline lock for safe concurrent read-modify-write."""
    return _pipeline_locks[pipeline_id]


def _pipeline_path(pipeline_id: str) -> Path:
    return PROJECTS_DIR / pipeline_id / "pipeline.json"


def save(pipeline_id: str, data: dict) -> None:
    path = _pipeline_path(pipeline_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def load(pipeline_id: str) -> dict:
    path = _pipeline_path(pipeline_id)
    if not path.exists():
        raise FileNotFoundError(f"Pipeline {pipeline_id} not found")
    data = json.loads(path.read_text())
    data.setdefault("id", pipeline_id)
    return data


def list_all() -> list[dict]:
    if not PROJECTS_DIR.exists():
        return []
    pipelines: list[tuple[float, dict]] = []
    for pipeline_file in PROJECTS_DIR.glob("*/pipeline.json"):
        try:
            data = json.loads(pipeline_file.read_text())
            data.setdefault("id", pipeline_file.parent.name)
            mtime = pipeline_file.stat().st_mtime
            pipelines.append((mtime, data))
        except (json.JSONDecodeError, OSError):
            continue
    pipelines.sort(key=lambda x: x[0], reverse=True)
    return [p[1] for p in pipelines]


def delete(pipeline_id: str) -> None:
    project_dir = PROJECTS_DIR / pipeline_id
    shutil.rmtree(project_dir)


def exists(pipeline_id: str) -> bool:
    return _pipeline_path(pipeline_id).exists()
