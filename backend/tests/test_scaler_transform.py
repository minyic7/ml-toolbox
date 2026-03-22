"""Tests for the Scaler Transform node."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_scaler_transform_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.scaler_transform.scaler_transform"]
    assert meta["label"] == "Scaler Transform"
    assert meta["category"] == "Transform"
    assert meta["type"] == "ml_toolbox.nodes.scaler_transform.scaler_transform"
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
    assert len(meta["params"]) == 2
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"method", "columns"}
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_splits(tmp_path: Path, n_train: int = 80, n_val: int = 10, n_test: int = 10):
    """Create train/val/test parquet files with predictable numeric data."""
    import random

    random.seed(42)

    def _make_df(n: int, offset: int = 0) -> pl.DataFrame:
        return pl.DataFrame({
            "feature_a": [float(i + offset) for i in range(n)],
            "feature_b": [float((i + offset) * 10) for i in range(n)],
            "category": ["cat" if i % 2 == 0 else "dog" for i in range(n)],
            "target": [i % 2 for i in range(n)],
        })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"

    _make_df(n_train, 0).write_parquet(train_path)
    _make_df(n_val, 100).write_parquet(val_path)
    _make_df(n_test, 200).write_parquet(test_path)

    return train_path, val_path, test_path


def _make_meta(tmp_path: Path, target: str = "target"):
    """Write a .meta.json sidecar alongside train.parquet."""
    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "category": {"dtype": "string", "semantic_type": "categorical", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": target,
        "row_count": 80,
        "generated_by": "test",
    }
    meta_path = tmp_path / "train.meta.json"
    meta_path.write_text(json.dumps(meta))
    return meta_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes train/val/test to separate files."""
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _run_scaler(tmp_path, params, n_train=80, n_val=10, n_test=10, write_meta=True):
    """Helper to set up splits, meta, and run the scaler."""
    from ml_toolbox.nodes.scaler_transform import scaler_transform

    train_path, val_path, test_path = _make_splits(tmp_path, n_train, n_val, n_test)
    if write_meta:
        _make_meta(tmp_path)

    inputs = {
        "train": str(train_path),
        "val": str(val_path),
        "test": str(test_path),
    }

    with patch(
        "ml_toolbox.nodes.scaler_transform._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = scaler_transform(inputs=inputs, params=params)

    return result


# ── StandardScaler tests ─────────────────────────────────────────


def test_standard_scaler_basic(tmp_path: Path):
    """StandardScaler should produce mean≈0, std≈1 on train."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": ""})

    train_df = pl.read_parquet(Path(result["train"]))

    # Scaled columns should have mean ≈ 0 and std ≈ 1 on train
    for col in ("feature_a", "feature_b"):
        mean = train_df[col].mean()
        std = train_df[col].std()
        assert abs(mean) < 1e-10, f"{col} mean={mean}"  # type: ignore[operator]
        assert abs(std - 1.0) < 0.02, f"{col} std={std}"  # type: ignore[operator]

    # Target should NOT be scaled
    assert train_df["target"].dtype == pl.Int64
    assert train_df["target"].to_list() == [i % 2 for i in range(80)]

    # Category column should be untouched
    assert train_df["category"].dtype == pl.String


def test_standard_scaler_transforms_val_test_with_train_params(tmp_path: Path):
    """Val and test should be transformed using train's mean/std."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": ""})

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))

    # Val/test means should NOT be 0 (they use train's params)
    # val starts at offset 100, test at 200 — they should have shifted means
    assert val_df["feature_a"].mean() != pytest.approx(0.0, abs=0.5)  # type: ignore[union-attr]
    assert test_df["feature_a"].mean() != pytest.approx(0.0, abs=0.5)  # type: ignore[union-attr]

    # All should have Float64 dtype for scaled columns
    for df in (train_df, val_df, test_df):
        assert df["feature_a"].dtype == pl.Float64
        assert df["feature_b"].dtype == pl.Float64


def test_standard_scaler_zero_variance_skip(tmp_path: Path):
    """Columns with zero variance should be skipped."""
    from ml_toolbox.nodes.scaler_transform import scaler_transform

    # Create data with a constant column
    train_df = pl.DataFrame({
        "constant": [5.0] * 50,
        "normal": [float(i) for i in range(50)],
        "target": [i % 2 for i in range(50)],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "constant": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "normal": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path)}

    with patch(
        "ml_toolbox.nodes.scaler_transform._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = scaler_transform(inputs=inputs, params={"method": "StandardScaler", "columns": ""})

    out_df = pl.read_parquet(Path(result["train"]))

    # Constant column should be unchanged (skipped)
    assert out_df["constant"].to_list() == [5.0] * 50

    # Normal column should be scaled
    assert abs(out_df["normal"].mean()) < 1e-10  # type: ignore[operator]


# ── MinMaxScaler tests ───────────────────────────────────────────


def test_minmax_scaler_basic(tmp_path: Path):
    """MinMaxScaler should produce values in [0, 1] on train."""
    result = _run_scaler(tmp_path, {"method": "MinMaxScaler", "columns": ""})

    train_df = pl.read_parquet(Path(result["train"]))

    for col in ("feature_a", "feature_b"):
        mn = train_df[col].min()
        mx = train_df[col].max()
        assert mn == pytest.approx(0.0, abs=1e-10), f"{col} min={mn}"
        assert mx == pytest.approx(1.0, abs=1e-10), f"{col} max={mx}"


def test_minmax_scaler_min_eq_max_skip(tmp_path: Path):
    """MinMaxScaler should skip columns where min == max."""
    from ml_toolbox.nodes.scaler_transform import scaler_transform

    train_df = pl.DataFrame({
        "constant": [3.0] * 50,
        "normal": [float(i) for i in range(50)],
        "target": [i % 2 for i in range(50)],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "constant": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "normal": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path)}

    with patch(
        "ml_toolbox.nodes.scaler_transform._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = scaler_transform(inputs=inputs, params={"method": "MinMaxScaler", "columns": ""})

    out_df = pl.read_parquet(Path(result["train"]))
    # Constant column should be unchanged
    assert out_df["constant"].to_list() == [3.0] * 50
    # Normal column should be scaled
    assert out_df["normal"].min() == pytest.approx(0.0, abs=1e-10)


# ── RobustScaler tests ──────────────────────────────────────────


def test_robust_scaler_basic(tmp_path: Path):
    """RobustScaler should produce median≈0 on train."""
    result = _run_scaler(tmp_path, {"method": "RobustScaler", "columns": ""})

    train_df = pl.read_parquet(Path(result["train"]))

    for col in ("feature_a", "feature_b"):
        median = train_df[col].median()
        assert abs(median) < 0.05, f"{col} median={median}"  # type: ignore[operator]


def test_robust_scaler_zero_iqr_skip(tmp_path: Path):
    """RobustScaler should skip columns with zero IQR."""
    from ml_toolbox.nodes.scaler_transform import scaler_transform

    # A column where >75% of values are the same → IQR = 0
    train_df = pl.DataFrame({
        "flat": [1.0] * 45 + [2.0] * 5,
        "normal": [float(i) for i in range(50)],
        "target": [i % 2 for i in range(50)],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "flat": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "normal": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path)}

    with patch(
        "ml_toolbox.nodes.scaler_transform._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = scaler_transform(inputs=inputs, params={"method": "RobustScaler", "columns": ""})

    out_df = pl.read_parquet(Path(result["train"]))
    # Flat column should be unchanged (IQR=0 → skipped)
    assert out_df["flat"].to_list() == [1.0] * 45 + [2.0] * 5


# ── Column selection / edge cases ────────────────────────────────


def test_specific_columns_param(tmp_path: Path):
    """Only columns listed in 'columns' param should be scaled."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": "feature_a"})

    train_df = pl.read_parquet(Path(result["train"]))

    # feature_a should be scaled
    assert abs(train_df["feature_a"].mean()) < 1e-10  # type: ignore[operator]
    # feature_b should NOT be scaled (still original values)
    assert train_df["feature_b"].mean() == pytest.approx(395.0, abs=1.0)  # type: ignore[union-attr]


def test_non_numeric_column_in_columns_param_skipped(tmp_path: Path):
    """Non-numeric columns explicitly listed should be skipped with warning."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": "feature_a, category"})

    train_df = pl.read_parquet(Path(result["train"]))

    # feature_a should be scaled
    assert abs(train_df["feature_a"].mean()) < 1e-10  # type: ignore[operator]
    # category should be untouched (string type)
    assert train_df["category"].dtype == pl.String


def test_target_column_skipped(tmp_path: Path):
    """Target column from .meta.json should never be scaled."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": ""})

    train_df = pl.read_parquet(Path(result["train"]))
    assert train_df["target"].dtype == pl.Int64
    assert train_df["target"].to_list() == [i % 2 for i in range(80)]


def test_meta_json_updated_with_float64(tmp_path: Path):
    """.meta.json sidecar should mark scaled columns as Float64."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": ""})

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()

    meta = json.loads(meta_path.read_text())
    assert meta["columns"]["feature_a"]["dtype"] == "Float64"
    assert meta["columns"]["feature_b"]["dtype"] == "Float64"
    # Non-scaled columns should keep original dtype
    assert meta["columns"]["category"]["dtype"] == "string"
    assert meta["columns"]["target"]["dtype"] == "int64"
    # Target should be preserved
    assert meta["target"] == "target"


def test_empty_val_test_handled(tmp_path: Path):
    """When val/test have 0 rows, they should still be output correctly."""
    from ml_toolbox.nodes.scaler_transform import scaler_transform

    # Create train with data, val/test empty
    train_df = pl.DataFrame({
        "feature_a": [float(i) for i in range(50)],
        "target": [i % 2 for i in range(50)],
    })
    val_df = pl.DataFrame(schema=train_df.schema)

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)

    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path), "val": str(val_path)}

    with patch(
        "ml_toolbox.nodes.scaler_transform._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = scaler_transform(inputs=inputs, params={"method": "StandardScaler", "columns": ""})

    assert "train" in result
    assert "val" in result
    val_out = pl.read_parquet(Path(result["val"]))
    assert val_out.height == 0


def test_no_meta_json_falls_back_to_dtypes(tmp_path: Path):
    """Without .meta.json, node should detect numeric columns from dtypes."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": ""}, write_meta=False)

    train_df = pl.read_parquet(Path(result["train"]))

    # feature_a and feature_b should be scaled (numeric)
    assert abs(train_df["feature_a"].mean()) < 1e-10  # type: ignore[operator]
    # target is also numeric and will be scaled (no meta to identify it)
    # category (string) should be untouched
    assert train_df["category"].dtype == pl.String


def test_row_counts_preserved(tmp_path: Path):
    """Row counts should be identical to input after scaling."""
    result = _run_scaler(tmp_path, {"method": "StandardScaler", "columns": ""})

    assert pl.read_parquet(Path(result["train"])).height == 80
    assert pl.read_parquet(Path(result["val"])).height == 10
    assert pl.read_parquet(Path(result["test"])).height == 10
