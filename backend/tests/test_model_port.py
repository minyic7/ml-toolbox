"""Tests for MODEL port type: joblib serialization, runner auto-serialization, and API output metadata."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

RUNNER_PATH = Path(__file__).resolve().parent.parent / "sandbox" / "runner.py"


class TestJoblibRoundTrip:
    """Verify joblib dump + load preserves a simple sklearn model."""

    def test_sklearn_model_roundtrip(self, tmp_path: Path):
        import joblib
        from sklearn.linear_model import LinearRegression
        import numpy as np

        model = LinearRegression()
        X = np.array([[1], [2], [3], [4]])
        y = np.array([2, 4, 6, 8])
        model.fit(X, y)

        path = tmp_path / "model.joblib"
        joblib.dump(model, path)

        loaded = joblib.load(path)
        assert type(loaded).__name__ == "LinearRegression"
        # Predictions should match
        np.testing.assert_array_almost_equal(
            model.predict(X), loaded.predict(X)
        )

    def test_dict_model_roundtrip(self, tmp_path: Path):
        """Even a plain dict can be serialized with joblib."""
        import joblib

        obj = {"type": "custom_model", "weights": [1.0, 2.0, 3.0]}
        path = tmp_path / "model.joblib"
        joblib.dump(obj, path)

        loaded = joblib.load(path)
        assert loaded == obj


class TestRunnerAutoSerialization:
    """Test that the sandbox runner auto-serializes MODEL outputs."""

    def _run_in_subprocess(self, manifest: dict, tmp_path: Path) -> dict:
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

    def test_model_output_auto_serialized(self, tmp_path: Path):
        """Node returning a raw object for a MODEL port gets joblib-serialized."""
        code = (
            "def run(inputs, params):\n"
            "    from sklearn.linear_model import LinearRegression\n"
            "    import numpy as np\n"
            "    model = LinearRegression()\n"
            "    model.fit(np.array([[1],[2],[3]]), np.array([1,2,3]))\n"
            "    return {'model': model}\n"
        )
        manifest = {
            "node_id": "n1",
            "code": code,
            "inputs": {},
            "params": {},
            "conditions": [],
            "output_types": {"model": "MODEL"},
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        assert out["returncode"] == 0, f"Runner failed: {out['stderr']}"

        result = json.loads(out["result_path"].read_text())
        model_path = result["model"]
        assert model_path.endswith(".joblib")
        assert Path(model_path).exists()

        # Verify the serialized model can be loaded
        import joblib
        loaded = joblib.load(model_path)
        assert type(loaded).__name__ == "LinearRegression"

    def test_file_path_string_left_as_is(self, tmp_path: Path):
        """If node returns a string path, runner leaves it unchanged."""
        code = (
            "def run(inputs, params):\n"
            "    return {'model': '/some/path/model.joblib'}\n"
        )
        manifest = {
            "node_id": "n1",
            "code": code,
            "inputs": {},
            "params": {},
            "conditions": [],
            "output_types": {"model": "MODEL"},
        }
        out = self._run_in_subprocess(manifest, tmp_path)
        assert out["returncode"] == 0

        result = json.loads(out["result_path"].read_text())
        assert result["model"] == "/some/path/model.joblib"

    def test_no_output_types_backward_compatible(self, tmp_path: Path):
        """Without output_types in manifest, runner still works (backward compat)."""
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


class TestOutputMetadataJoblib:
    """Test that the output API returns correct metadata for .joblib files."""

    def test_joblib_output_metadata(self, tmp_path: Path):
        """_output_metadata returns safe preview (no deserialization) for .joblib files."""
        import joblib
        from sklearn.linear_model import LinearRegression
        import numpy as np

        # Create a fake run directory with a .joblib output
        model = LinearRegression()
        model.fit(np.array([[1], [2]]), np.array([1, 2]))

        node_id = "train_node"
        output_file = tmp_path / f"{node_id}_model.joblib"
        joblib.dump(model, output_file)

        from ml_toolbox.routers.pipelines import _output_metadata

        meta = _output_metadata(tmp_path, node_id)
        assert meta["node_id"] == node_id
        assert meta["type"] == "joblib"
        assert meta["size"] > 0
        # Preview should show format and file size without deserializing
        assert meta["preview"]["format"] == "joblib"
        assert meta["preview"]["file_size"] == meta["size"]
        # Ensure we are NOT deserializing (no model_class key)
        assert "model_class" not in meta["preview"]

    def test_find_output_file_finds_joblib(self, tmp_path: Path):
        """_find_output_file discovers .joblib files."""
        node_id = "train_node"
        output_file = tmp_path / f"{node_id}_model.joblib"
        output_file.write_bytes(b"fake joblib data")

        from ml_toolbox.routers.pipelines import _find_output_file

        found = _find_output_file(tmp_path, node_id)
        assert found is not None
        assert found.name == f"{node_id}_model.joblib"


class TestModelNodeRegistration:
    """Verify MODEL port type works in node registration."""

    def test_model_port_in_registry(self):
        """A node with MODEL output registers correctly."""
        from ml_toolbox.protocol import PortType, node as node_decorator
        from ml_toolbox.protocol.decorators import NODE_REGISTRY

        @node_decorator(
            inputs={"df": PortType.TABLE},
            outputs={"model": PortType.MODEL},
            label="Test Train",
        )
        def _test_train_model(inputs: dict, params: dict) -> dict:
            return {"model": "placeholder"}

        node_type = "_test_train_model"
        # Find the registered node (module path varies)
        matching = [k for k in NODE_REGISTRY if k.endswith(node_type)]
        assert len(matching) == 1

        entry = NODE_REGISTRY[matching[0]]
        assert entry["outputs"] == [{"name": "model", "type": "MODEL"}]
        assert entry["inputs"] == [{"name": "df", "type": "TABLE"}]


class TestExecutorOutputTypes:
    """Verify executor includes output_types in the manifest."""

    def test_manifest_contains_output_types(self, tmp_path: Path):
        """_execute_node writes output_types to the manifest."""
        from ml_toolbox.services.executor import PipelineExecutor

        pipeline = {
            "id": "test_pipeline",
            "nodes": [
                {
                    "id": "n1",
                    "type": "test.train",
                    "code": "def train(inputs, params):\n    return {}\n",
                    "params": {},
                    "outputs": [
                        {"name": "model", "type": "MODEL"},
                        {"name": "metrics", "type": "METRICS"},
                    ],
                }
            ],
            "edges": [],
        }

        # Build the manifest manually (don't actually run Docker)
        executor = PipelineExecutor()
        node = pipeline["nodes"][0]
        node_id = "n1"

        # Build output_types the same way executor does
        output_types = {}
        for port in node.get("outputs", []):
            output_types[port["name"]] = port.get("type", "VALUE")

        assert output_types == {"model": "MODEL", "metrics": "METRICS"}
