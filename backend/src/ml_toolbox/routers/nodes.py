from fastapi import APIRouter, HTTPException

from ml_toolbox.protocol import NODE_REGISTRY

# Ensure demo nodes are registered at import time
import ml_toolbox.nodes  # noqa: F401

router = APIRouter(prefix="/api")


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/nodes")
async def list_nodes() -> list[dict]:
    return list(NODE_REGISTRY.values())


@router.get("/nodes/{node_type}")
async def get_node(node_type: str) -> dict:
    if node_type not in NODE_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Node type '{node_type}' not found")
    return NODE_REGISTRY[node_type]
