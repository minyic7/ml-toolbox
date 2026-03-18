"""Tests for sandbox runner condition evaluation logic.

The runner normally executes inside a Docker container. These tests
exercise the condition-checking logic in isolation to verify that
edge conditions are evaluated safely inside the sandbox (not on the host).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

RUNNER_PATH = Path(__file__).resolve().parent.parent / "sandbox" / "runner.py"


class TestRunnerConditions:
    """Test that the sandbox runner evaluates edge conditions correctly."""

    def _run_in_subprocess(self, manifest: dict, tmp_path: Path) -> dict:
        """Write a manifest and run the sandbox runner as a subprocess."""
        manifest_path = tmp_path / "test_manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        result = subprocess.run(
            [sys.executable, str(RUNNER_PATH), str(manifest_path)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "result_path": tmp_path / "test_manifest_result.json",
            "error_path": tmp_path / "test_manifest_error.json",
        }

    def test_no_conditions_runs_code(self, tmp_path: Path):
        """Without conditions, code runs normally."""
        manifest = {
            "node_id": "n1",
            "code": "def run(inputs, params):\n    return {'ok': True}\n",
            "inputs": {},
            "params": {},
            "conditions": [],
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        assert out["returncode"] == 0
        result = json.loads(out["result_path"].read_text())
        assert result == {"ok": True}

    def test_true_condition_runs_code(self, tmp_path: Path):
        """When condition evaluates to True, code runs."""
        # Write upstream result
        (tmp_path / "upstream_manifest_result.json").write_text(
            json.dumps({"rows": 50})
        )
        manifest = {
            "node_id": "n1",
            "code": "def run(inputs, params):\n    return {'executed': True}\n",
            "inputs": {},
            "params": {},
            "conditions": [
                {"source_id": "upstream", "condition": "result.get('rows', 0) > 10"}
            ],
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        assert out["returncode"] == 0
        result = json.loads(out["result_path"].read_text())
        assert result == {"executed": True}

    def test_false_condition_skips(self, tmp_path: Path):
        """When condition evaluates to False, node is skipped."""
        (tmp_path / "upstream_manifest_result.json").write_text(
            json.dumps({"rows": 5})
        )
        manifest = {
            "node_id": "n1",
            "code": "def run(inputs, params):\n    return {'executed': True}\n",
            "inputs": {},
            "params": {},
            "conditions": [
                {"source_id": "upstream", "condition": "result.get('rows', 0) > 10"}
            ],
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        assert out["returncode"] == 0
        result = json.loads(out["result_path"].read_text())
        assert result.get("skipped") is True

    def test_invalid_condition_skips(self, tmp_path: Path):
        """Condition that raises an exception → treated as not met → skip."""
        manifest = {
            "node_id": "n1",
            "code": "def run(inputs, params):\n    return {}\n",
            "inputs": {},
            "params": {},
            "conditions": [
                {"source_id": "upstream", "condition": "undefined_var > 0"}
            ],
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        assert out["returncode"] == 0
        result = json.loads(out["result_path"].read_text())
        assert result.get("skipped") is True

    def test_condition_cannot_import(self, tmp_path: Path):
        """Conditions cannot access dangerous builtins like __import__."""
        manifest = {
            "node_id": "n1",
            "code": "def run(inputs, params):\n    return {}\n",
            "inputs": {},
            "params": {},
            "conditions": [
                {"source_id": "upstream", "condition": "__import__('os').system('echo pwned')"}
            ],
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        # Should skip (condition evaluation fails), not crash or execute os commands
        assert out["returncode"] == 0
        result = json.loads(out["result_path"].read_text())
        assert result.get("skipped") is True
