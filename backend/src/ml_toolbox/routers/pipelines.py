from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ml_toolbox.protocol.decorators import NODE_REGISTRY
from ml_toolbox.services import store

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


class UpdateNodeRequest(BaseModel):
    params: dict[str, Any] | None = None
    code: str | None = None
    position: Position | None = None


class AddEdgeRequest(BaseModel):
    source: str
    source_port: str
    target: str
    target_port: str


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

    node = {
        "id": node_id,
        "type": body.type,
        "position": body.position.model_dump(),
        "params": body.params if body.params is not None else list(template["params"]),
        "code": template["code"],
        "inputs": list(template["inputs"]),
        "outputs": list(template["outputs"]),
    }

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
        node["params"] = body.params
    if body.code is not None:
        node["code"] = body.code
    if body.position is not None:
        node["position"] = body.position.model_dump()

    store.save(pipeline_id, data)
    return node


# ── Edge Operations ──────────────────────────────────────────────


@router.post("/{pipeline_id}/edges", status_code=201)
async def add_edge(pipeline_id: str, body: AddEdgeRequest) -> dict:
    data = _load_pipeline(pipeline_id)

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

    # 3. Validate port types match
    if source_port["type"] != target_port["type"]:
        raise HTTPException(
            status_code=400,
            detail=f"Port type mismatch: {source_port['type']} != {target_port['type']}",
        )

    # 4. Check for cycles
    if would_create_cycle(data, body.source, body.target):
        raise HTTPException(status_code=400, detail="Edge would create a cycle")

    edge_id = uuid.uuid4().hex
    edge = {
        "id": edge_id,
        "source": body.source,
        "source_port": body.source_port,
        "target": body.target,
        "target_port": body.target_port,
        "condition": None,
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
