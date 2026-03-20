from __future__ import annotations

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

    # Sanitize filename: strip path separators and parent-dir traversal
    safe_name = file.filename.replace("..", "").replace("/", "").replace("\\", "")
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename")

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large (max {MAX_UPLOAD_BYTES // (1024 * 1024)} MB)",
        )

    dest = upload_dir / safe_name
    dest.write_bytes(content)

    return {"path": str(dest), "filename": safe_name, "size": len(content)}
