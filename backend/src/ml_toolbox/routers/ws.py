"""WebSocket router for real-time pipeline execution updates."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/ws")
logger = logging.getLogger(__name__)


class ConnectionManager:
    """Track active WebSocket connections per pipeline_id."""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, pipeline_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.setdefault(pipeline_id, []).append(ws)

    def disconnect(self, pipeline_id: str, ws: WebSocket) -> None:
        conns = self._connections.get(pipeline_id, [])
        if ws in conns:
            conns.remove(ws)
        if not conns:
            self._connections.pop(pipeline_id, None)

    async def broadcast(self, pipeline_id: str, message: dict[str, Any]) -> None:
        """Send a JSON message to all clients watching *pipeline_id*."""
        conns = list(self._connections.get(pipeline_id, []))
        for ws in conns:
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(pipeline_id, ws)


# Singleton instance
manager = ConnectionManager()

# Captured at startup so background threads can schedule coroutines on it.
_main_loop: asyncio.AbstractEventLoop | None = None


def set_main_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Store a reference to the main async event loop."""
    global _main_loop
    _main_loop = loop


def broadcast_sync(pipeline_id: str, message: dict[str, Any]) -> None:
    """Thread-safe bridge: schedule an async broadcast from a sync context.

    Called by the executor running in a background thread.
    """
    if _main_loop is None or _main_loop.is_closed():
        logger.warning("broadcast_sync: no event loop available, dropping message")
        return
    asyncio.run_coroutine_threadsafe(
        manager.broadcast(pipeline_id, message), _main_loop
    )


@router.websocket("/pipelines/{pipeline_id}")
async def pipeline_ws(websocket: WebSocket, pipeline_id: str) -> None:
    await manager.connect(pipeline_id, websocket)
    try:
        while True:
            # Keep connection alive; we only send, but must read to detect disconnect
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(pipeline_id, websocket)
