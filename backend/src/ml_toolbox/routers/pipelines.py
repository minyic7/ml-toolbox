from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ml_toolbox.services import store

router = APIRouter(prefix="/api/pipelines")


class CreatePipelineRequest(BaseModel):
    name: str


class PipelineSummary(BaseModel):
    id: str
    name: str


class SettingsUpdate(BaseModel):
    model_config = {"extra": "allow"}


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
