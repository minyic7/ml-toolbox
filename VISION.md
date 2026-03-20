# ML-Toolbox — Vision

> **Personal project.** Single-user, runs on a Mac Mini (local network only).
> No auth, no multi-tenancy, no SLA. Build it right, not enterprise-ready.

---

## Status

**Phase 1 — COMPLETE.** Foundation: node protocol, canvas UI, execution engine, sandbox infrastructure, CI/CD.

**Phase 2 — COMPLETE.** 15 ML nodes implemented across 6 categories (Ingest, Transform, Train, Evaluate, Export, Demo). Auto-discovery via `pkgutil` — drop a `.py` file in `nodes/` and it registers automatically.

**Current focus:** UX polish, manual testing, and iterating on the workflow experience.

---

## Stack

```
Frontend:  React 19, TypeScript 5, Vite 6, Tailwind v4, shadcn/ui,
           @xyflow/react 12, @monaco-editor/react, React Query, Zustand, pnpm

Backend:   Python 3.13, FastAPI, uv, Polars, pandas, pyarrow, pydantic v2, docker-py

Deploy:    Docker Compose on Mac Mini (linux/arm64), Caddy reverse proxy,
           GHA CI (pyright + pytest + eslint + tsc + Playwright) + CD (Tailscale SSH)
           Port 8910, GHCR images, GitHub repo: https://github.com/minyic7/ml-toolbox
```

---

## Architecture: Core Design Decisions

### Data Transfer Between Nodes

Node outputs are written to **structured files on disk**. Nodes receive **file paths**, not data objects.

```
~/.ml-toolbox/projects/{project_id}/runs/{run_id}/
  {node_id}.parquet    ← TABLE (Polars DataFrame)
  {node_id}.joblib     ← MODEL
  {node_id}.json       ← METRICS / VALUE
  {node_id}.npy        ← ARRAY
  {node_id}.hash       ← params + code SHA-256
```

### Pipeline Execution

DAG — directed, no cycles. Kahn's topological sort.

```
Run All  → new run_id, start from all root nodes
Run From → new run_id, re-run node + downstream, hardlink cached upstream
Skip     → only if output file exists AND params hash matches
```

### Node Execution Sandbox

Every node runs in an **isolated Docker container** — never in the FastAPI process.

```
Host (FastAPI)                     Sandbox Container
──────────────                     ─────────────────
PipelineExecutor
  ├─ write manifest.json ─mount──▶  /data/{run_id}/
  │   --network none, --memory 1g, --read-only, --cap-drop ALL
  │   --pids-limit 64, timeout 300s
  └─ read result manifest ◀───────  container exits
```

Two-image split: `sandbox/Dockerfile.base` (heavy deps, cached) + `sandbox/Dockerfile` (runner.py only).

### Node Protocol

```python
@node(
    inputs={"df": PortType.TABLE},
    outputs={"df": PortType.TABLE},
    params={"strategy": Select(["mean", "median", "drop"], default="mean",
                                description="How to handle nulls")},
)
def my_node(inputs: dict, params: dict) -> dict:
    df = pd.read_parquet(inputs["df"])
    out = _get_output_path("df")
    df.to_parquet(out)
    return {"df": str(out)}
```

6 PortTypes: TABLE, MODEL, METRICS, ARRAY, VALUE, TENSOR
4 Param types: Select, Slider, Text, Toggle (each supports description + placeholder)

---

## Backend API

28 REST endpoints + 1 WebSocket:

| Area | Endpoints |
|------|-----------|
| Pipeline CRUD | POST/GET/PUT/DELETE /api/pipelines, /duplicate, /settings |
| Node/Edge CRUD | POST/DELETE/PATCH for nodes and edges |
| Execution | POST /run, /run/{node_id}, /cancel; GET /status |
| Runs & Outputs | GET /api/runs (global), GET /outputs/{node_id}/download |
| WebSocket | WS /ws/pipelines/{id} — real-time status broadcasts |

---

## UI Layout

```
┌──────────────────────────────────────────────────────────────────┐
│  TOPBAR  44px  (logo, pipeline name, auto-save, Run All, Cancel) │
├──────────────────────────────────────────────────────────────────┤
│  TOOLBAR  46px  (horizontal node icon-chips by category)         │
├──────────────────────────────┬───────────────────────────────────┤
│                              │                                   │
│   CANVAS (flex-fill)         │  CODE PANE (resizable, right)     │
│   React Flow — pan/zoom      │  PARAMS/CODE/OUTPUT tabs          │
│                              │                                   │
├──────────────────────────────┴───────────────────────────────────┤
│  BOTTOM DRAWER (floating, transparent, ~220px)                   │
│  Params tab / simplified Output tab                              │
└──────────────────────────────────────────────────────────────────┘
```

**Home Screen:** Run dashboard (default) + Pipelines grid (secondary tab).

---

## Nodes (15 registered)

| Category | Nodes |
|----------|-------|
| Ingest | CSV Reader, Parquet Reader |
| Transform | Clean Data, Feature Engineering, Train/Test Split, Compute Stats |
| Train | Train sklearn Model (10 estimators), Train XGBoost |
| Evaluate | Classification Metrics, Regression Metrics, Feature Importance |
| Export | Export Table, Export Model |
| Demo | Generate Data, Summarize Data |

---

## Testing

- **Backend:** 204 tests (executor, API, protocol, all nodes, sandbox runner)
- **Frontend:** 123 unit tests + 18 Playwright E2E tests
- **Integration:** Tests A-G with real Docker sandbox
- **CI:** pyright + pytest + eslint + tsc + vitest + Playwright
- **CD:** docker build arm64 → Tailscale SSH → docker compose up

---

## What's Next

- Layout redesign: floating bottom drawer, resizable code panel (#102)
- File upload for path params (#103)
- More nodes as needed for real ML workflows
- `sql_input` deferred (needs sandbox networking redesign)
