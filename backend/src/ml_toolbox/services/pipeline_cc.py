"""Per-pipeline Claude Code tmux session manager."""

from __future__ import annotations

import json
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
        return self.data_dir / "projects" / pipeline_id

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
    # Template directory
    # ------------------------------------------------------------------

    _TEMPLATE_DIR = Path(__file__).parent / "cc_templates"

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
        api_base = "http://localhost:8000"

        placeholders = {
            "{{pipeline_name}}": name,
            "{{pipeline_id}}": pipeline_id,
            "{{project_dir}}": str(project_dir),
            "{{runs_dir}}": str(runs_dir),
            "{{api_base}}": api_base,
            "{{nodes_section}}": nodes_section,
            "{{edges_section}}": edges_section,
        }

        template_path = self._TEMPLATE_DIR / "CLAUDE.md"
        content = template_path.read_text()
        for key, value in placeholders.items():
            content = content.replace(key, value)

        (work_dir / "CLAUDE.md").write_text(content)

    # ------------------------------------------------------------------
    # Skills installation
    # ------------------------------------------------------------------

    def _install_skills(self, work_dir: Path, pipeline_id: str) -> None:
        skills_dir = work_dir / ".claude" / "skills"
        skills_dir.mkdir(parents=True, exist_ok=True)

        project_dir = self.data_dir / "projects" / pipeline_id
        runs_dir = project_dir / "runs"
        api_base = "http://localhost:8000"

        placeholders = {
            "{{pipeline_id}}": pipeline_id,
            "{{project_dir}}": str(project_dir),
            "{{runs_dir}}": str(runs_dir),
            "{{api_base}}": api_base,
        }

        template_skills_dir = self._TEMPLATE_DIR / "skills"
        for skill_file in template_skills_dir.iterdir():
            if not skill_file.is_file():
                continue
            content = skill_file.read_text()
            for key, value in placeholders.items():
                content = content.replace(key, value)
            # Claude Code expects skills/{name}/SKILL.md directory structure
            skill_name = skill_file.stem  # e.g. "configure-node"
            skill_dir = skills_dir / skill_name
            skill_dir.mkdir(parents=True, exist_ok=True)
            (skill_dir / "SKILL.md").write_text(content)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_pipeline_safe(self, pipeline_id: str) -> dict | None:
        try:
            return store.load(pipeline_id)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
