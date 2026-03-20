from __future__ import annotations

import uuid
from pathlib import PurePosixPath

from fastapi import APIRouter, UploadFile, HTTPException

from ml_toolbox.config import DATA_DIR

router = APIRouter(prefix="/api", tags=["uploads"])

MAX_UPLOAD_BYTES = 100 * 1024 * 1024  # 100 MB


@router.post("/upload")
async def upload_file(file: UploadFile) -> dict[str, str | int]:
    """Upload a file and return its server-side path."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    upload_dir = DATA_DIR / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize: extract just the filename component (no directory traversal)
    safe_name = PurePosixPath(file.filename).name
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Read at most MAX_UPLOAD_BYTES + 1 to detect oversized files
    # without buffering the entire upload
    content = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large (max {MAX_UPLOAD_BYTES // (1024 * 1024)} MB)",
        )

    # Prefix with a short UUID to avoid silent overwrites
    unique_name = f"{uuid.uuid4().hex[:8]}_{safe_name}"
    dest = upload_dir / unique_name
    dest.write_bytes(content)

    return {"path": str(dest), "filename": unique_name, "size": len(content)}
