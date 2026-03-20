"""Tests for the data cleaning transform node."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def _mock_output_path(tmp_path: Path):
    """Return a _get_output_path replacement that maps each name to its own file."""

    def _get(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"

    return _get


def _run_clean(tmp_path: Path, input_df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """Helper: write input to parquet, run the clean node, return output df."""
    from ml_toolbox.nodes.transform import clean

    input_file = tmp_path / "input.parquet"
    input_df.to_parquet(input_file, index=False)

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_path(tmp_path),
    ):
        result = clean(inputs={"df": str(input_file)}, params=params)

    return pd.read_parquet(result["df"])


def test_clean_node_registered():
    meta = NODE_REGISTRY["ml_toolbox.nodes.transform.clean"]
    assert meta["label"] == "Clean Data"
    assert meta["category"] == "Transform"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]


def test_drop_nulls_removes_rows_with_nan(tmp_path: Path):
    df = pd.DataFrame({"a": [1.0, None, 3.0], "b": [4.0, 5.0, None]})
    result = _run_clean(tmp_path, df, {"drop_nulls": True, "drop_duplicates": False, "fill_strategy": "none"})
    assert len(result) == 1
    assert result["a"].iloc[0] == 1.0


def test_fill_strategy_mean_fills_numeric_nulls(tmp_path: Path):
    df = pd.DataFrame({"a": [1.0, None, 3.0], "b": [10.0, 20.0, None]})
    result = _run_clean(tmp_path, df, {"drop_nulls": True, "drop_duplicates": False, "fill_strategy": "mean"})
    # fill takes precedence over drop_nulls, so all 3 rows kept
    assert len(result) == 3
    assert result["a"].iloc[1] == pytest.approx(2.0)
    assert result["b"].iloc[2] == pytest.approx(15.0)


def test_drop_duplicates_removes_duplicate_rows(tmp_path: Path):
    df = pd.DataFrame({"a": [1, 2, 2, 3], "b": [10, 20, 20, 30]})
    result = _run_clean(tmp_path, df, {"drop_nulls": False, "drop_duplicates": True, "fill_strategy": "none"})
    assert len(result) == 3


def test_clean_writes_summary_json(tmp_path: Path):
    import json

    from ml_toolbox.nodes.transform import clean

    df = pd.DataFrame({"a": [1.0, None, 3.0, 3.0], "b": [4.0, 5.0, 6.0, 6.0]})
    input_file = tmp_path / "input.parquet"
    df.to_parquet(input_file, index=False)

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_path(tmp_path),
    ):
        clean(inputs={"df": str(input_file)}, params={"drop_nulls": True, "drop_duplicates": True, "fill_strategy": "none"})

    summary_path = tmp_path / "clean_summary.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text())
    assert summary["rows_before"] == 4
    assert summary["rows_after"] == 2
    assert summary["rows_dropped"] == 2


def test_all_options_disabled_passthrough(tmp_path: Path):
    df = pd.DataFrame({"a": [1.0, None, 2.0, 2.0], "b": [None, 5.0, 6.0, 6.0]})
    result = _run_clean(tmp_path, df, {"drop_nulls": False, "drop_duplicates": False, "fill_strategy": "none"})
    assert len(result) == len(df)
