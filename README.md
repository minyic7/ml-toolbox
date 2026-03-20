# ML Toolbox

Visual ML pipeline builder — a node-based canvas editor where Python nodes execute in Docker sandboxes as a DAG. Built as a personal project (single-user, Mac Mini). Phase 1 (foundation) and Phase 2 (ML nodes) are complete.

**Stack:** React 19 + TypeScript + Vite (frontend), FastAPI + Python 3.13 + uv (backend), Docker sandbox execution.

## Prerequisites

- **Docker** — required for sandbox node execution
- **Node.js 22+** and **pnpm** — frontend
- **Python 3.13+** and **uv** — backend

## Quick Start — Local Development

**Backend:**

```bash
cd backend
uv sync --frozen
uv run uvicorn ml_toolbox.main:app --reload --port 8000
```

**Frontend:**

```bash
cd frontend
pnpm install --frozen-lockfile
pnpm dev
# Opens at http://localhost:5173/ml-toolbox/
```

**Sandbox image (required for node execution):**

```bash
cd backend
docker build -t ghcr.io/minyic7/ml-toolbox/sandbox-base:latest -f sandbox/Dockerfile.base sandbox/
docker build -t ml-toolbox-sandbox sandbox/
```

## Production — Docker Compose

```bash
docker compose up -d
# Caddy serves frontend + proxies API on port 8910
```

See `docker-compose.yml` — runs Caddy (port 8910), backend, and sandbox.

## Running Tests

**Backend unit + API tests:**

```bash
cd backend
uv run pytest tests/ -x -q --ignore=tests/integration
```

**Backend integration tests (requires Docker):**

```bash
cd backend
docker build -t ghcr.io/minyic7/ml-toolbox/sandbox-base:latest -f sandbox/Dockerfile.base sandbox/
docker build -t ml-toolbox-sandbox sandbox/
uv run pytest tests/integration/ -x -v
```

**Frontend unit tests:**

```bash
cd frontend
pnpm test
```

**Frontend E2E tests (Playwright):**

```bash
cd frontend
pnpm exec playwright install --with-deps chromium
pnpm test:e2e
# Auto-starts dev server, runs 18 tests (canvas smoke, home screen runs, WebSocket disconnect)
```

See `frontend/playwright.config.ts` — uses `baseURL: http://localhost:5173/ml-toolbox/`, auto-starts `pnpm dev`.

## Adding New Nodes

Nodes are auto-discovered — create a `.py` file in `backend/src/ml_toolbox/nodes/`. The `@node` decorator registers it via `pkgutil.iter_modules` in `nodes/__init__.py`. No imports to update.

```python
from ml_toolbox.protocol import PortType, node

@node(
    inputs={"df": PortType.TABLE},
    outputs={"df": PortType.TABLE},
)
def my_node(inputs: dict, params: dict) -> dict:
    import polars as pl
    df = pl.read_parquet(inputs["df"])
    # ... transform ...
    out = _get_output_path("df")
    df.write_parquet(out)
    return {"df": str(out)}
```

Currently 15 nodes across 6 files — see `backend/src/ml_toolbox/nodes/`.

## Architecture

```
frontend/     React 19 + Vite + React Flow canvas + Monaco editor
backend/      FastAPI + uv + pipeline execution + WebSocket
  nodes/      @node-decorated Python files (auto-discovered)
  sandbox/    Dockerfile + runner.py (isolated Docker execution)
```
