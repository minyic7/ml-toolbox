"""Tests for the feature engineering transform node."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_feature_eng_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.transform.feature_eng"]
    assert meta["label"] == "Feature Engineering"
    assert meta["category"] == "Transform"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    assert len(meta["params"]) == 3
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"scale_columns", "encode_columns", "bin_columns"}


def _make_input_df(tmp_path: Path) -> Path:
    """Create a sample input parquet file and return its path."""
    rng = np.random.RandomState(42)
    df = pd.DataFrame({
        "age": rng.normal(30, 10, 200),
        "income": rng.normal(50000, 15000, 200),
        "color": rng.choice(["red", "blue", "green"], 200),
        "score": rng.normal(100, 25, 200),
    })
    path = tmp_path / "input.parquet"
    df.to_parquet(path, index=False)
    return path


def test_scaling_normalizes_columns(tmp_path: Path):
    """Standard scaling should produce mean~0, std~1."""
    from ml_toolbox.nodes.transform import feature_eng

    input_path = _make_input_df(tmp_path)
    output_path = tmp_path / "df.parquet"

    with patch("ml_toolbox.nodes.transform._get_output_path", return_value=output_path):
        result = feature_eng(
            inputs={"df": str(input_path)},
            params={"scale_columns": "age, income", "encode_columns": "", "bin_columns": ""},
        )

    df = pd.read_parquet(result["df"])
    for col in ["age", "income"]:
        assert abs(df[col].mean()) < 0.1, f"{col} mean should be ~0"
        assert abs(df[col].std() - 1.0) < 0.1, f"{col} std should be ~1"


def test_one_hot_encoding_creates_binary_columns(tmp_path: Path):
    """One-hot encoding should create binary indicator columns."""
    from ml_toolbox.nodes.transform import feature_eng

    input_path = _make_input_df(tmp_path)
    output_path = tmp_path / "df.parquet"

    with patch("ml_toolbox.nodes.transform._get_output_path", return_value=output_path):
        result = feature_eng(
            inputs={"df": str(input_path)},
            params={"scale_columns": "", "encode_columns": "color", "bin_columns": ""},
        )

    df = pd.read_parquet(result["df"])
    # Original column should be gone
    assert "color" not in df.columns
    # Binary indicator columns should exist
    dummy_cols = [c for c in df.columns if c.startswith("color_")]
    assert len(dummy_cols) == 3  # red, blue, green
    for col in dummy_cols:
        assert set(df[col].unique()).issubset({0, 1, True, False})


def test_binning_creates_quartile_categories(tmp_path: Path):
    """Binning should create a new column with quartile labels."""
    from ml_toolbox.nodes.transform import feature_eng

    input_path = _make_input_df(tmp_path)
    output_path = tmp_path / "df.parquet"

    with patch("ml_toolbox.nodes.transform._get_output_path", return_value=output_path):
        result = feature_eng(
            inputs={"df": str(input_path)},
            params={"scale_columns": "", "encode_columns": "", "bin_columns": "score"},
        )

    df = pd.read_parquet(result["df"])
    assert "score_bin" in df.columns
    assert set(df["score_bin"].unique()) == {"Q1", "Q2", "Q3", "Q4"}
    # Original column should still exist
    assert "score" in df.columns


def test_empty_params_passthrough(tmp_path: Path):
    """Empty params should return the DataFrame unchanged."""
    from ml_toolbox.nodes.transform import feature_eng

    input_path = _make_input_df(tmp_path)
    output_path = tmp_path / "df.parquet"

    input_df = pd.read_parquet(input_path)

    with patch("ml_toolbox.nodes.transform._get_output_path", return_value=output_path):
        result = feature_eng(
            inputs={"df": str(input_path)},
            params={"scale_columns": "", "encode_columns": "", "bin_columns": ""},
        )

    output_df = pd.read_parquet(result["df"])
    assert list(output_df.columns) == list(input_df.columns)
    assert len(output_df) == len(input_df)
    pd.testing.assert_frame_equal(output_df, input_df)
