"""Tests for the Log Transform node."""

import json
import math
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — triggers @node registration


# ── Registry metadata ─────────────────────────────────────────────


def test_log_transform_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.log_transform.log_transform"]
    assert meta["label"] == "Log Transform"
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
    assert "columns" in param_names
    assert meta["guide"] != ""


# ── Helpers ───────────────────────────────────────────────────────


def _make_parquet(path: Path, columns: dict | None = None) -> Path:
    if columns is None:
        columns = {
            "feature_a": [10.0, 20.0, 30.0, 40.0],
            "feature_b": [2.0, 4.0, 5.0, 8.0],
            "target": [0, 1, 0, 1],
        }
    df = pl.DataFrame(columns)
    df.write_parquet(path)
    return path


def _make_meta(path: Path, target: str = "target", columns: dict | None = None) -> Path:
    if columns is None:
        columns = {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        }
    meta = {"columns": columns, "target": target}
    meta_path = path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta_path


def _make_eda_context(path: Path, context: dict) -> Path:
    ctx_path = path.with_suffix(".eda-context.json")
    ctx_path.write_text(json.dumps(context, indent=2))
    return ctx_path


def _mock_output_factory(tmp_path: Path):
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"
    return mock_output


# ── Basic tests ──────────────────────────────────────────────────


def test_log_transform_explicit_columns(tmp_path: Path):
    """Log transform applies log1p in-place to specified columns."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = log_transform(
            inputs={"train": str(train_path)},
            params={"columns": "feature_a"},
        )

    df = pl.read_parquet(result["train"])
    # In-place: column name unchanged but values are log1p'd
    assert "feature_a" in df.columns
    assert abs(df["feature_a"][0] - math.log(11.0)) < 1e-6
    # feature_b should be unchanged
    assert abs(df["feature_b"][0] - 2.0) < 1e-6


def test_log_transform_auto_select_from_eda(tmp_path: Path):
    """Auto-selects columns with skewness > 1 from EDA context."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)
    _make_eda_context(train_path, {
        "distribution": {
            "feature_a": {"skewness": 2.5, "kurtosis": 1.0, "mean": 25.0, "std": 10.0},
            "feature_b": {"skewness": 0.3, "kurtosis": 0.5, "mean": 5.0, "std": 2.0},
        },
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = log_transform(
            inputs={"train": str(train_path)},
            params={"columns": ""},
        )

    df = pl.read_parquet(result["train"])
    # feature_a should be transformed (skewness=2.5 > 1)
    assert abs(df["feature_a"][0] - math.log(11.0)) < 1e-6
    # feature_b should NOT be transformed (skewness=0.3 < 1)
    assert abs(df["feature_b"][0] - 2.0) < 1e-6


def test_log_transform_auto_select_outliers(tmp_path: Path):
    """Auto-selects columns with outlier_pct > 0.05 from EDA context."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)
    _make_eda_context(train_path, {
        "outliers": {
            "feature_b": {"method": "iqr", "outlier_pct": 0.1},
        },
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = log_transform(
            inputs={"train": str(train_path)},
            params={"columns": ""},
        )

    df = pl.read_parquet(result["train"])
    # feature_b selected due to outlier_pct > 0.05
    assert abs(df["feature_b"][0] - math.log(3.0)) < 1e-6
    # feature_a not selected (no outlier entry)
    assert abs(df["feature_a"][0] - 10.0) < 1e-6


def test_log_transform_negative_values_warning(tmp_path: Path):
    """Warns on negative values."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path, {"col": [-2.0, 0.0, 1.0], "target": [0, 1, 0]})
    _make_meta(train_path, columns={
        "col": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.warns(UserWarning, match="negative value"):
            result = log_transform(
                inputs={"train": str(train_path)},
                params={"columns": "col"},
            )

    df = pl.read_parquet(result["train"])
    # -2 → log(-1) = NaN
    assert df["col"][0] is None or (isinstance(df["col"][0], float) and math.isnan(df["col"][0]))
    # 0 → log(1) = 0
    assert abs(df["col"][1] - 0.0) < 1e-6


def test_log_transform_three_way_split(tmp_path: Path):
    """Applied identically to train/val/test."""
    from ml_toolbox.nodes.log_transform import log_transform

    for split in ("train", "val", "test"):
        _make_parquet(tmp_path / f"{split}.parquet")
    _make_meta(tmp_path / "train.parquet")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = log_transform(
            inputs={
                "train": str(tmp_path / "train.parquet"),
                "val": str(tmp_path / "val.parquet"),
                "test": str(tmp_path / "test.parquet"),
            },
            params={"columns": "feature_a"},
        )

    for split in ("train", "val", "test"):
        assert split in result
        df = pl.read_parquet(result[split])
        assert abs(df["feature_a"][0] - math.log(11.0)) < 1e-6


def test_log_transform_column_not_found(tmp_path: Path):
    """Error when a referenced column does not exist."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            log_transform(
                inputs={"train": str(train_path)},
                params={"columns": "nonexistent"},
            )


def test_log_transform_meta_json_updated(tmp_path: Path):
    """.meta.json is preserved with generated_by updated."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = log_transform(
            inputs={"train": str(train_path)},
            params={"columns": "feature_a"},
        )

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["generated_by"] == "log_transform"
    assert "feature_a" in meta["columns"]
    assert meta["target"] == "target"


def test_log_transform_target_excluded(tmp_path: Path):
    """Target column is excluded from auto-selection."""
    from ml_toolbox.nodes.log_transform import log_transform

    train_path = tmp_path / "train.parquet"
    # Only one numeric column besides target
    _make_parquet(train_path, {"feature_a": [10.0, 20.0], "target": [0, 1]})
    _make_meta(train_path, columns={
        "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.log_transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = log_transform(
            inputs={"train": str(train_path)},
            params={"columns": ""},
        )

    df = pl.read_parquet(result["train"])
    # target should be untouched
    assert df["target"][0] == 0
    assert df["target"][1] == 1
    # feature_a should be log-transformed
    assert abs(df["feature_a"][0] - math.log(11.0)) < 1e-6
