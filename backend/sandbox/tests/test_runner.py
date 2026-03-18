"""Unit tests for sandbox runner.py logic."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

RUNNER_PATH = Path(__file__).resolve().parent.parent / "runner.py"


@pytest.fixture()
def manifest_dir(tmp_path: Path):
    """Create a temporary directory with a manifest file."""
    return tmp_path


def _write_manifest(directory: Path, manifest: dict, name: str = "manifest.json") -> Path:
    path = directory / name
    path.write_text(json.dumps(manifest))
    return path


def _run_runner(manifest_path: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(RUNNER_PATH), str(manifest_path)],
        capture_output=True,
        text=True,
    )


class TestManifestParsing:
    def test_parses_manifest_fields(self, manifest_dir: Path):
        code = "def run(inputs, params): return {'status': 'ok', 'node': _get_output_path()}"
        manifest = {
            "node_id": "node_42",
            "code": code,
            "params": {"alpha": 0.5},
            "inputs": {"/data/input.parquet": "table"},
        }
        path = _write_manifest(manifest_dir, manifest)
        result = _run_runner(path)
        assert result.returncode == 0

        result_path = manifest_dir / "manifest_result.json"
        assert result_path.exists()
        data = json.loads(result_path.read_text())
        assert data["status"] == "ok"

    def test_inputs_and_params_passed_to_run(self, manifest_dir: Path):
        code = "def run(inputs, params): return {'i': inputs, 'p': params}"
        manifest = {
            "node_id": "n1",
            "code": code,
            "params": {"key": "val"},
            "inputs": {"a": 1},
        }
        path = _write_manifest(manifest_dir, manifest)
        result = _run_runner(path)
        assert result.returncode == 0

        data = json.loads((manifest_dir / "manifest_result.json").read_text())
        assert data["i"] == {"a": 1}
        assert data["p"] == {"key": "val"}


class TestGetOutputPath:
    def test_default_output_path(self, manifest_dir: Path):
        code = "def run(inputs, params): return {'path': _get_output_path()}"
        manifest = {"node_id": "abc", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest)
        _run_runner(path)

        data = json.loads((manifest_dir / "manifest_result.json").read_text())
        expected = str(manifest_dir / "abc_output.parquet")
        assert data["path"] == expected

    def test_custom_name_and_ext(self, manifest_dir: Path):
        code = 'def run(inputs, params): return {"path": _get_output_path("model", ".pkl")}'
        manifest = {"node_id": "xyz", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest)
        _run_runner(path)

        data = json.loads((manifest_dir / "manifest_result.json").read_text())
        expected = str(manifest_dir / "xyz_model.pkl")
        assert data["path"] == expected


class TestResultWriting:
    def test_writes_result_json(self, manifest_dir: Path):
        code = "def run(inputs, params): return {'answer': 42}"
        manifest = {"node_id": "n1", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest, "job.json")
        _run_runner(path)

        result_path = manifest_dir / "job_result.json"
        assert result_path.exists()
        assert json.loads(result_path.read_text()) == {"answer": 42}

    def test_result_uses_manifest_stem(self, manifest_dir: Path):
        code = "def run(inputs, params): return {}"
        manifest = {"node_id": "n1", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest, "my_run.json")
        _run_runner(path)

        assert (manifest_dir / "my_run_result.json").exists()


class TestErrorHandling:
    def test_error_writes_error_json_and_exits_1(self, manifest_dir: Path):
        code = "def run(inputs, params): raise ValueError('boom')"
        manifest = {"node_id": "n1", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest)
        result = _run_runner(path)

        assert result.returncode == 1
        err_path = manifest_dir / "manifest_error.json"
        assert err_path.exists()
        err_data = json.loads(err_path.read_text())
        assert "ValueError" in err_data["error"]
        assert "boom" in err_data["error"]

    def test_exec_syntax_error(self, manifest_dir: Path):
        code = "def run(inputs, params):\n  return ]["
        manifest = {"node_id": "n1", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest)
        result = _run_runner(path)

        assert result.returncode == 1
        err_path = manifest_dir / "manifest_error.json"
        assert err_path.exists()

    def test_no_result_on_error(self, manifest_dir: Path):
        code = "def run(inputs, params): raise RuntimeError('fail')"
        manifest = {"node_id": "n1", "code": code, "params": {}, "inputs": {}}
        path = _write_manifest(manifest_dir, manifest)
        _run_runner(path)

        assert not (manifest_dir / "manifest_result.json").exists()
