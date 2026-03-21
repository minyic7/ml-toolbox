"""Claude Code terminal WebSocket and REST endpoints."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import pty
import signal
import struct
import termios

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from ml_toolbox.services.pipeline_cc import PipelineCCManager

router = APIRouter()
cc_manager = PipelineCCManager()


# ------------------------------------------------------------------
# WebSocket — terminal streaming
# ------------------------------------------------------------------


def _set_pty_size(fd: int, cols: int, rows: int) -> None:
    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)


@router.websocket("/ws/pipelines/{pipeline_id}/terminal")
async def pipeline_terminal(websocket: WebSocket, pipeline_id: str) -> None:
    if not cc_manager.is_alive(pipeline_id):
        cc_manager.start(pipeline_id)
        await asyncio.sleep(1)

    session_name = cc_manager._session_name(pipeline_id)
    await websocket.accept()

    master_fd, slave_fd = pty.openpty()
    _set_pty_size(master_fd, 80, 24)

    def _child_setup() -> None:
        os.setsid()
        fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)

    env = {**os.environ, "TERM": "xterm-256color"}
    process = await asyncio.create_subprocess_exec(
        "tmux",
        "attach-session",
        "-t",
        session_name,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        preexec_fn=_child_setup,
        env=env,
    )
    os.close(slave_fd)

    loop = asyncio.get_event_loop()
    child_pid = process.pid

    async def _read_pty() -> None:
        while True:
            try:
                data = await loop.run_in_executor(
                    None, lambda: os.read(master_fd, 4096)
                )
                if not data:
                    break
                await websocket.send_bytes(data)
            except OSError:
                break

    async def _write_pty() -> None:
        while True:
            try:
                msg = await websocket.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                if "bytes" in msg:
                    data: bytes = msg["bytes"]
                    if data and data[0:1] == b"\x01":
                        resize = json.loads(data[1:])
                        _set_pty_size(
                            master_fd,
                            resize.get("cols", 80),
                            resize.get("rows", 24),
                        )
                        if child_pid:
                            os.kill(child_pid, signal.SIGWINCH)
                    else:
                        os.write(master_fd, data)
                elif "text" in msg:
                    os.write(master_fd, msg["text"].encode())
            except (WebSocketDisconnect, Exception):
                break

    read_task = asyncio.create_task(_read_pty())
    write_task = asyncio.create_task(_write_pty())

    try:
        await asyncio.wait(
            [read_task, write_task], return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for t in [read_task, write_task]:
            t.cancel()
        try:
            os.close(master_fd)
        except OSError:
            pass
        process.terminate()


# ------------------------------------------------------------------
# REST endpoints — session management
# ------------------------------------------------------------------


class MessageBody(BaseModel):
    message: str


@router.post("/api/cc/pipelines/{pipeline_id}/start")
async def start_session(pipeline_id: str) -> dict[str, str]:
    name = cc_manager.start(pipeline_id)
    return {"session": name, "status": "started"}


@router.post("/api/cc/pipelines/{pipeline_id}/restart")
async def restart_session(pipeline_id: str) -> dict[str, str]:
    name = cc_manager.restart(pipeline_id)
    return {"session": name, "status": "restarted"}


@router.delete("/api/cc/pipelines/{pipeline_id}")
async def stop_session(pipeline_id: str) -> dict[str, str]:
    cc_manager.stop(pipeline_id)
    return {"status": "stopped"}


@router.get("/api/cc/pipelines/{pipeline_id}/status")
async def session_status(pipeline_id: str) -> dict[str, object]:
    alive = cc_manager.is_alive(pipeline_id)
    return {
        "pipeline_id": pipeline_id,
        "session": cc_manager._session_name(pipeline_id),
        "alive": alive,
    }


@router.post("/api/cc/pipelines/{pipeline_id}/message")
async def send_message(pipeline_id: str, body: MessageBody) -> dict[str, str]:
    cc_manager.send_message(pipeline_id, body.message)
    return {"status": "sent"}
