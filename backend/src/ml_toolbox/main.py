import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ml_toolbox.config import DATA_DIR
from ml_toolbox.routers import nodes, pipelines, runs, uploads, ws
from ml_toolbox.routers.ws import set_main_loop


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    set_main_loop(asyncio.get_running_loop())
    yield


app = FastAPI(title="ML Toolbox", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(nodes.router)
app.include_router(pipelines.router)
app.include_router(runs.router)
app.include_router(uploads.router)
app.include_router(ws.router)
