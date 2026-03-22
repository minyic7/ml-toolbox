"""Tests for the Missing Value Imputer transform node."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — trigger auto-discovery


# ── Registry metadata ──────────────────────────────────────────────

def test_node_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.transform.missing_value_imputer"]
    assert meta["label"] == "Missing Value Imputer"
    assert meta["category"] == "Transform"
    assert meta["inputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    assert meta["outputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"strategy", "constant_value", "columns"}
    assert meta["guide"] != ""


# ── Helpers ────────────────────────────────────────────────────────

def _make_splits(tmp_path: Path, *, with_meta: bool = True) -> dict[str, str]:
    """Create train/val/test parquets with some nulls and return input dict."""
    train = pl.DataFrame({
        "age": [25.0, None, 35.0, 40.0, None, 30.0, 28.0, 45.0, None, 50.0],
        "salary": [50000.0, 60000.0, None, 80000.0, 70000.0, None, 55000.0, 90000.0, 65000.0, None],
        "city": ["NYC", None, "LA", "NYC", "SF", None, "LA", "NYC", None, "SF"],
        "target": [1, 0, 1, 0, 1, 0, 1, 0, 1, 0],
    })
    val = pl.DataFrame({
        "age": [22.0, None, 38.0],
        "salary": [48000.0, None, 72000.0],
        "city": ["NYC", "LA", None],
        "target": [1, 0, 1],
    })
    test = pl.DataFrame({
        "age": [None, 33.0],
        "salary": [62000.0, None],
        "city": [None, "SF"],
        "target": [0, 1],
    })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train.write_parquet(train_path)
    val.write_parquet(val_path)
    test.write_parquet(test_path)

    if with_meta:
        meta = {"target": "target", "columns": {
            "age": {"semantic_type": "continuous", "role": "feature"},
            "salary": {"semantic_type": "continuous", "role": "feature"},
            "city": {"semantic_type": "categorical", "role": "feature"},
            "target": {"semantic_type": "binary", "role": "target"},
        }}
        (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    return {
        "train": str(train_path),
        "val": str(val_path),
        "test": str(test_path),
    }


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path routing to separate files."""
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"out_{name}{ext}"
    return mock_output


def _run(tmp_path: Path, inputs: dict[str, str], params: dict) -> dict:
    from ml_toolbox.nodes.transform import missing_value_imputer
    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        return missing_value_imputer(inputs=inputs, params=params)


# ── Strategy: mean ─────────────────────────────────────────────────

def test_mean_strategy(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": ""})

    train_out = pl.read_parquet(result["train"])
    val_out = pl.read_parquet(result["val"])
    test_out = pl.read_parquet(result["test"])

    # Train: age mean = (25+35+40+30+28+45+50)/7 ≈ 36.14
    assert train_out["age"].null_count() == 0
    assert train_out["salary"].null_count() == 0
    # City is non-numeric — should be skipped for mean strategy
    assert train_out["city"].null_count() > 0

    # Val/test should also have no nulls in numeric columns
    assert val_out["age"].null_count() == 0
    assert test_out["salary"].null_count() == 0

    # Target should NOT be imputed (even if it had nulls)
    assert "target" in train_out.columns


def test_mean_fills_with_train_values(tmp_path: Path):
    """Verify fill values come from train, not val/test."""
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": ""})

    train_in = pl.read_parquet(inputs["train"])
    train_age_mean = train_in["age"].drop_nulls().mean()

    val_out = pl.read_parquet(result["val"])
    # Row 1 of val had null age — should be filled with train mean
    assert val_out["age"][1] == pytest.approx(train_age_mean)


# ── Strategy: median ───────────────────────────────────────────────

def test_median_strategy(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {"strategy": "median", "constant_value": "", "columns": ""})

    train_out = pl.read_parquet(result["train"])
    assert train_out["age"].null_count() == 0
    assert train_out["salary"].null_count() == 0

    train_in = pl.read_parquet(inputs["train"])
    train_age_median = train_in["age"].drop_nulls().median()

    val_out = pl.read_parquet(result["val"])
    assert val_out["age"][1] == pytest.approx(train_age_median)


# ── Strategy: mode ─────────────────────────────────────────────────

def test_mode_strategy(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {"strategy": "mode", "constant_value": "", "columns": ""})

    train_out = pl.read_parquet(result["train"])
    # Mode works on any type — city should also be imputed
    assert train_out["city"].null_count() == 0
    assert train_out["age"].null_count() == 0

    val_out = pl.read_parquet(result["val"])
    assert val_out["city"].null_count() == 0


# ── Strategy: constant ─────────────────────────────────────────────

def test_constant_strategy(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {
        "strategy": "constant",
        "constant_value": "0",
        "columns": "",
    })

    train_out = pl.read_parquet(result["train"])
    assert train_out["age"].null_count() == 0
    assert train_out["salary"].null_count() == 0

    # Numeric columns get filled with 0.0
    train_in = pl.read_parquet(inputs["train"])
    null_age_indices = [i for i, v in enumerate(train_in["age"].to_list()) if v is None]
    for idx in null_age_indices:
        assert train_out["age"][idx] == pytest.approx(0.0)


def test_constant_strategy_string_fill(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {
        "strategy": "constant",
        "constant_value": "UNKNOWN",
        "columns": "city",
    })

    train_out = pl.read_parquet(result["train"])
    assert train_out["city"].null_count() == 0
    # Check that null cities are filled with "UNKNOWN"
    train_in = pl.read_parquet(inputs["train"])
    null_city_indices = [i for i, v in enumerate(train_in["city"].to_list()) if v is None]
    for idx in null_city_indices:
        assert train_out["city"][idx] == "UNKNOWN"


def test_constant_missing_value_error(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    with pytest.raises(ValueError, match="constant_value must be provided"):
        _run(tmp_path, inputs, {
            "strategy": "constant",
            "constant_value": "",
            "columns": "",
        })


# ── Column selection ───────────────────────────────────────────────

def test_specific_columns(tmp_path: Path):
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": "age"})

    train_out = pl.read_parquet(result["train"])
    # age should be imputed
    assert train_out["age"].null_count() == 0
    # salary was NOT in columns list — should still have nulls
    assert train_out["salary"].null_count() > 0


# ── Target column protection ──────────────────────────────────────

def test_target_column_skipped(tmp_path: Path):
    """Target column from .meta.json should never be imputed."""
    # Create data where target has nulls
    train = pl.DataFrame({
        "age": [25.0, None, 35.0, 40.0],
        "target": [1, None, 0, 1],
    })
    val = pl.DataFrame({"age": [22.0], "target": [0]})
    test = pl.DataFrame({"age": [33.0], "target": [1]})

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train.write_parquet(train_path)
    val.write_parquet(val_path)
    test.write_parquet(test_path)

    meta = {"target": "target"}
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path), "val": str(val_path), "test": str(test_path)}
    result = _run(tmp_path, inputs, {"strategy": "mode", "constant_value": "", "columns": ""})

    train_out = pl.read_parquet(result["train"])
    # age should be imputed, target should NOT
    assert train_out["age"].null_count() == 0
    assert train_out["target"].null_count() == 1


# ── Edge cases ─────────────────────────────────────────────────────

def test_all_null_column_skipped(tmp_path: Path):
    """A column with ALL nulls should be skipped with a warning."""
    train = pl.DataFrame({
        "age": [25.0, 35.0, 40.0],
        "empty_col": pl.Series([None, None, None], dtype=pl.Float64),
    })
    val = pl.DataFrame({
        "age": [22.0],
        "empty_col": pl.Series([None], dtype=pl.Float64),
    })
    test = pl.DataFrame({
        "age": [33.0],
        "empty_col": pl.Series([None], dtype=pl.Float64),
    })

    for name, df in [("train", train), ("val", val), ("test", test)]:
        df.write_parquet(tmp_path / f"{name}.parquet")

    inputs = {
        "train": str(tmp_path / "train.parquet"),
        "val": str(tmp_path / "val.parquet"),
        "test": str(tmp_path / "test.parquet"),
    }

    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": ""})

    # Should warn about the all-null column
    warn_msgs = [str(x.message) for x in w]
    assert any("empty_col" in m and "all null" in m for m in warn_msgs)

    # empty_col should still be all null (skipped)
    train_out = pl.read_parquet(result["train"])
    assert train_out["empty_col"].null_count() == 3


def test_mean_on_non_numeric_skipped(tmp_path: Path):
    """Mean strategy on non-numeric column should be skipped with a warning."""
    inputs = _make_splits(tmp_path)

    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": "city"})

    warn_msgs = [str(x.message) for x in w]
    assert any("city" in m and "not numeric" in m for m in warn_msgs)


def test_no_missing_values(tmp_path: Path):
    """Dataset with no nulls should pass through unchanged."""
    train = pl.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
    val = pl.DataFrame({"a": [7.0], "b": [8.0]})
    test = pl.DataFrame({"a": [9.0], "b": [10.0]})

    for name, df in [("train", train), ("val", val), ("test", test)]:
        df.write_parquet(tmp_path / f"{name}.parquet")

    inputs = {
        "train": str(tmp_path / "train.parquet"),
        "val": str(tmp_path / "val.parquet"),
        "test": str(tmp_path / "test.parquet"),
    }
    result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": ""})

    train_out = pl.read_parquet(result["train"])
    assert train_out.equals(train)


def test_schema_unchanged(tmp_path: Path):
    """Output schema should match input schema exactly."""
    inputs = _make_splits(tmp_path)
    result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": ""})

    train_in = pl.read_parquet(inputs["train"])
    train_out = pl.read_parquet(result["train"])
    assert train_out.schema == train_in.schema
    assert train_out.columns == train_in.columns


def test_no_meta_json(tmp_path: Path):
    """Node should work fine without .meta.json — just impute all null columns."""
    inputs = _make_splits(tmp_path, with_meta=False)
    result = _run(tmp_path, inputs, {"strategy": "mean", "constant_value": "", "columns": ""})

    train_out = pl.read_parquet(result["train"])
    # Without meta, target is not protected — but numeric columns should still be imputed
    assert train_out["age"].null_count() == 0
    assert train_out["salary"].null_count() == 0
