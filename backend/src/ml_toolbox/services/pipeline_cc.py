"""Per-pipeline Claude Code tmux session manager."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from ml_toolbox.config import DATA_DIR
from ml_toolbox.services import store


class PipelineCCManager:
    """Manages one tmux session per pipeline for Claude Code."""

    def __init__(self) -> None:
        self.data_dir = DATA_DIR

    def _session_name(self, pipeline_id: str) -> str:
        return f"ml-toolbox-cc-{pipeline_id[:12]}"

    def _work_dir(self, pipeline_id: str) -> Path:
        return self.data_dir / "projects" / pipeline_id / "cc"

    def is_alive(self, pipeline_id: str) -> bool:
        name = self._session_name(pipeline_id)
        result = subprocess.run(
            ["tmux", "has-session", "-t", name],
            capture_output=True,
        )
        return result.returncode == 0

    def start(self, pipeline_id: str) -> str:
        name = self._session_name(pipeline_id)
        if self.is_alive(pipeline_id):
            return name

        work_dir = self._work_dir(pipeline_id)
        work_dir.mkdir(parents=True, exist_ok=True)

        self._write_claude_md(work_dir, pipeline_id)
        self._install_skills(work_dir, pipeline_id)

        subprocess.run(
            ["tmux", "new-session", "-d", "-s", name, "-x", "200", "-y", "50"],
            cwd=str(work_dir),
            check=True,
        )
        subprocess.run(
            ["tmux", "set-option", "-t", name, "remain-on-exit", "on"],
            check=True,
        )
        subprocess.run(
            ["tmux", "set-option", "-t", name, "history-limit", "200000"],
            check=True,
        )

        claude_cmd = "while true; do claude --verbose; sleep 2; done"
        subprocess.run(
            ["tmux", "send-keys", "-t", name, claude_cmd, "Enter"],
            check=True,
        )

        return name

    def restart(self, pipeline_id: str) -> str:
        self.stop(pipeline_id)
        return self.start(pipeline_id)

    def stop(self, pipeline_id: str) -> None:
        name = self._session_name(pipeline_id)
        if self.is_alive(pipeline_id):
            subprocess.run(
                ["tmux", "kill-session", "-t", name],
                capture_output=True,
            )

    def send_message(self, pipeline_id: str, message: str) -> None:
        name = self._session_name(pipeline_id)
        if not self.is_alive(pipeline_id):
            self.start(pipeline_id)
        subprocess.run(
            ["tmux", "send-keys", "-t", name, message, "Enter"],
            check=True,
        )

    # ------------------------------------------------------------------
    # CLAUDE.md generation
    # ------------------------------------------------------------------

    def _write_claude_md(self, work_dir: Path, pipeline_id: str) -> None:
        pipeline = self._load_pipeline_safe(pipeline_id)
        name = pipeline.get("name", pipeline_id) if pipeline else pipeline_id

        nodes_section = ""
        if pipeline and pipeline.get("nodes"):
            lines: list[str] = []
            for n in pipeline["nodes"]:
                node_type = n.get("type", "unknown")
                label = n.get("name") or node_type.rsplit(".", 1)[-1]
                lines.append(f"- **{label}** (`{n.get('id', '?')}`): type=`{node_type}`")
            nodes_section = "### Nodes\n" + "\n".join(lines)

        edges_section = ""
        if pipeline and pipeline.get("edges"):
            lines = []
            for e in pipeline["edges"]:
                lines.append(
                    f"- `{e.get('source', '?')}:{e.get('source_port', '?')}` → "
                    f"`{e.get('target', '?')}:{e.get('target_port', '?')}`"
                )
            edges_section = "### Edges\n" + "\n".join(lines)

        project_dir = self.data_dir / "projects" / pipeline_id
        runs_dir = project_dir / "runs"

        content = f"""\
# ML-Toolbox — Claude Code Context

## Pipeline
- **Name:** {name}
- **ID:** `{pipeline_id}`

{nodes_section}

{edges_section}

## Data Locations
- Project dir: `{project_dir}`
- Runs dir: `{runs_dir}`
- Pipeline JSON: `{project_dir / 'pipeline.json'}`

## Output Format
Node outputs are written to structured files:
```
~/.ml-toolbox/projects/{{project_id}}/runs/{{run_id}}/
  {{node_id}}.parquet    # TABLE
  {{node_id}}.joblib     # MODEL
  {{node_id}}.json       # METRICS / VALUE
  {{node_id}}.npy        # ARRAY
  {{node_id}}.hash       # params + code SHA-256
```

## ML Toolbox API (localhost:8000)
- `GET  /api/pipelines` — list pipelines
- `GET  /api/pipelines/{{id}}` — get pipeline
- `PUT  /api/pipelines/{{id}}` — update pipeline
- `POST /api/pipelines/{{id}}/run` — run full pipeline
- `POST /api/pipelines/{{id}}/run/{{node_id}}` — run from node
- `GET  /api/pipelines/{{id}}/status` — execution status
- `GET  /api/nodes` — list available node types
- `GET  /api/runs` — list all runs

## Available Skills
- **infer-schema** — Analyze data files and infer column types
- **suggest-dag** — Suggest pipeline DAG improvements
- **explain-output** — Explain node output files
- **configure-node** — Help configure node parameters

## Guidelines
- This is a single-user personal project (no auth, no multi-tenancy).
- Nodes run in Docker sandbox containers, not in the FastAPI process.
- Data transfer between nodes uses file paths, not data objects.
- DAG execution uses Kahn's topological sort.
"""
        (work_dir / "CLAUDE.md").write_text(content)

    # ------------------------------------------------------------------
    # Skills installation
    # ------------------------------------------------------------------

    def _install_skills(self, work_dir: Path, pipeline_id: str) -> None:
        skills_dir = work_dir / ".claude" / "skills"
        skills_dir.mkdir(parents=True, exist_ok=True)

        template_dir = Path(__file__).parent.parent / "cc_skills"
        if template_dir.is_dir():
            for skill_file in template_dir.iterdir():
                if skill_file.is_file():
                    shutil.copy2(skill_file, skills_dir / skill_file.name)
            return

        # Fallback: generate default skill stubs
        self._write_default_skills(skills_dir, pipeline_id)

    def _write_default_skills(self, skills_dir: Path, pipeline_id: str) -> None:
        project_dir = self.data_dir / "projects" / pipeline_id

        skills: dict[str, str] = {
            "infer-schema.md": f"""\
---
name: infer-schema
description: Analyze data files and infer column types/statistics
---

# Infer Schema

Analyze parquet/CSV files in the project and report column names, types,
null counts, and basic statistics.

## Usage
Look in `{project_dir}/runs/` for output files and analyze their schema.
""",
            "suggest-dag.md": f"""\
---
name: suggest-dag
description: Suggest improvements to the pipeline DAG
---

# Suggest DAG

Review the pipeline definition at `{project_dir}/pipeline.json`
and suggest improvements to the node graph.
""",
            "explain-output.md": f"""\
---
name: explain-output
description: Explain the contents of node output files
---

# Explain Output

Read node output files (parquet, JSON, joblib) from
`{project_dir}/runs/` and explain what they contain.
""",
            "configure-node.md": f"""\
---
name: configure-node
description: Help configure node parameters
---

# Configure Node

Help the user choose appropriate parameter values for pipeline nodes.
Read the pipeline JSON at `{project_dir}/pipeline.json` to see
current node configurations and available parameter types.
""",
        }

        for filename, content in skills.items():
            (skills_dir / filename).write_text(content)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_pipeline_safe(self, pipeline_id: str) -> dict | None:
        try:
            return store.load(pipeline_id)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
