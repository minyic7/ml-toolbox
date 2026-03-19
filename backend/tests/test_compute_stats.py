"""Tests for the compute_stats node."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

# Trigger registration
import ml_toolbox.nodes  # noqa: F401


def _make_input_df(tmp_path: Path) -> Path:
    """Create a simple parquet file with numeric columns."""
    df = pl.DataFrame(
        {
            "name": ["alice", "bob", "carol", "dave", "eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [80.0, 90.0, 70.0, 85.0, 95.0],
        }
    )
    path = tmp_path / "input.parquet"
    df.write_parquet(path)
    return path


class TestComputeStatsRegistration:
    def test_registered_in_registry(self):
        assert "ml_toolbox.nodes.transform.compute_stats" in NODE_REGISTRY

    def test_metadata(self):
        meta = NODE_REGISTRY["ml_toolbox.nodes.transform.compute_stats"]
        assert meta["label"] == "Compute Stats"
        assert meta["category"] == "Transform"
        assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
        assert meta["outputs"] == [{"name": "stats", "type": "VALUE"}]
        param_names = {p["name"] for p in meta["params"]}
        assert param_names == {"column", "statistic"}


class TestComputeStatsExecution:
    def _run(self, tmp_path: Path, input_path: Path, params: dict) -> dict:
        from ml_toolbox.nodes.transform import compute_stats

        def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
            return tmp_path / f"{name}{ext}"

        with patch("ml_toolbox.nodes.transform._get_output_path", side_effect=mock_output):
            return compute_stats(inputs={"df": str(input_path)}, params=params)

    def _load_result(self, result: dict) -> dict:
        return json.loads(Path(result["stats"]).read_text())

    def test_mean(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "mean"})
        data = self._load_result(result)
        assert data["value"] == pytest.approx(35.0)
        assert data["column"] == "age"
        assert data["statistic"] == "mean"

    def test_median(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "median"})
        data = self._load_result(result)
        assert data["value"] == pytest.approx(35.0)

    def test_std(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "std"})
        data = self._load_result(result)
        assert data["value"] == pytest.approx(pl.Series([25, 30, 35, 40, 45]).std())

    def test_min(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "min"})
        data = self._load_result(result)
        assert data["value"] == 25

    def test_max(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "max"})
        data = self._load_result(result)
        assert data["value"] == 45

    def test_count(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "count"})
        data = self._load_result(result)
        assert data["value"] == 5

    def test_sum(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "age", "statistic": "sum"})
        data = self._load_result(result)
        assert data["value"] == 175

    def test_output_is_valid_json(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "score", "statistic": "mean"})
        path = Path(result["stats"])
        assert path.suffix == ".json"
        data = json.loads(path.read_text())
        assert "value" in data
        assert "column" in data
        assert "statistic" in data

    def test_empty_column_uses_first_numeric(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "", "statistic": "mean"})
        data = self._load_result(result)
        # "name" is string, so first numeric column should be "age"
        assert data["column"] == "age"
        assert data["value"] == pytest.approx(35.0)

    def test_missing_column_uses_first_numeric(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "nonexistent", "statistic": "sum"})
        data = self._load_result(result)
        assert data["column"] == "age"

    def test_float_column(self, tmp_path: Path):
        input_path = _make_input_df(tmp_path)
        result = self._run(tmp_path, input_path, {"column": "score", "statistic": "mean"})
        data = self._load_result(result)
        assert data["value"] == pytest.approx(84.0)
        assert data["column"] == "score"
