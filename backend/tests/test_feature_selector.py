"""Tests for the Feature Selector transform node."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — trigger auto-discovery


# ── Registry / metadata ─────────────────────────────────────────


def test_feature_selector_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.feature_selector.feature_selector"]
    assert meta["label"] == "Feature Selector"
    assert meta["category"] == "Transform"
    assert meta["type"] == "ml_toolbox.nodes.feature_selector.feature_selector"
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
    assert param_names == {"method", "threshold"}
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_splits(tmp_path: Path):
    """Create train/val/test parquet files with predictable data."""
    train_df = pl.DataFrame({
        "feature_a": [float(i) for i in range(100)],           # high variance
        "feature_b": [float(i * 10) for i in range(100)],      # high variance
        "constant": [5.0] * 100,                                # zero variance
        "near_constant": [1.0] * 99 + [1.001],                 # near-zero variance
        "target": [i % 2 for i in range(100)],
    })
    val_df = pl.DataFrame({
        "feature_a": [float(i + 200) for i in range(20)],
        "feature_b": [float((i + 200) * 10) for i in range(20)],
        "constant": [5.0] * 20,
        "near_constant": [1.0] * 20,
        "target": [i % 2 for i in range(20)],
    })
    test_df = pl.DataFrame({
        "feature_a": [float(i + 400) for i in range(20)],
        "feature_b": [float((i + 400) * 10) for i in range(20)],
        "constant": [5.0] * 20,
        "near_constant": [1.0] * 20,
        "target": [i % 2 for i in range(20)],
    })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)
    return train_path, val_path, test_path


def _make_meta(tmp_path: Path, target: str = "target"):
    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "constant": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "near_constant": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": target,
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))


def _mock_output_factory(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _run_selector(tmp_path, params, include_val=True, include_test=True):
    from ml_toolbox.nodes.feature_selector import feature_selector

    train_path, val_path, test_path = _make_splits(tmp_path)
    _make_meta(tmp_path)

    inputs: dict[str, str] = {"train": str(train_path)}
    if include_val:
        inputs["val"] = str(val_path)
    if include_test:
        inputs["test"] = str(test_path)

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = feature_selector(inputs=inputs, params=params)

    return result


# ── Variance threshold tests ────────────────────────────────────


def test_variance_threshold_drops_constant(tmp_path: Path):
    """Constant columns should be dropped with default threshold."""
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 0.01})

    train_df = pl.read_parquet(Path(result["train"]))
    assert "constant" not in train_df.columns
    assert "near_constant" not in train_df.columns
    assert "feature_a" in train_df.columns
    assert "feature_b" in train_df.columns
    assert "target" in train_df.columns


def test_variance_threshold_keeps_high_variance(tmp_path: Path):
    """High-variance features should be kept."""
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 0.01})

    train_df = pl.read_parquet(Path(result["train"]))
    assert "feature_a" in train_df.columns
    assert "feature_b" in train_df.columns


def test_variance_threshold_low_keeps_nonzero(tmp_path: Path):
    """With a very low threshold, near-constant columns survive but zero-variance are dropped."""
    # near_constant has variance ~1e-8, so threshold below that keeps it
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 1e-12})

    train_df = pl.read_parquet(Path(result["train"]))
    assert "constant" not in train_df.columns      # zero variance — dropped
    assert "near_constant" in train_df.columns      # tiny but above threshold
    assert "feature_a" in train_df.columns


# ── Three-way split tests ───────────────────────────────────────


def test_three_way_split_same_columns(tmp_path: Path):
    """All splits should have the same columns after selection."""
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 0.01})

    train_cols = pl.read_parquet(Path(result["train"])).columns
    val_cols = pl.read_parquet(Path(result["val"])).columns
    test_cols = pl.read_parquet(Path(result["test"])).columns

    assert train_cols == val_cols == test_cols


def test_train_only(tmp_path: Path):
    """Should work with train only (no val/test)."""
    result = _run_selector(
        tmp_path,
        {"method": "variance_threshold", "threshold": 0.01},
        include_val=False,
        include_test=False,
    )

    assert "train" in result
    assert "val" not in result
    assert "test" not in result


def test_row_counts_preserved(tmp_path: Path):
    """Row counts should match input after selection."""
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 0.01})

    assert pl.read_parquet(Path(result["train"])).height == 100
    assert pl.read_parquet(Path(result["val"])).height == 20
    assert pl.read_parquet(Path(result["test"])).height == 20


# ── Target protection ───────────────────────────────────────────


def test_target_column_never_dropped(tmp_path: Path):
    """Target column is excluded from feature selection and never removed."""
    from ml_toolbox.nodes.feature_selector import feature_selector

    # Create data where target has low variance but is still protected
    train_df = pl.DataFrame({
        "feature_a": [float(i) for i in range(100)],      # high variance
        "low_var": [1.0] * 99 + [2.0],                     # low variance
        "target": [0] * 99 + [1],                           # low variance — but protected
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "low_var": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = feature_selector(
            inputs={"train": str(train_path)},
            params={"method": "variance_threshold", "threshold": 0.5},
        )

    out_df = pl.read_parquet(Path(result["train"]))
    assert "target" in out_df.columns       # protected
    assert "feature_a" in out_df.columns     # high variance — kept
    assert "low_var" not in out_df.columns   # low variance — dropped


# ── Correlation with target ─────────────────────────────────────


def test_correlation_drops_uncorrelated(tmp_path: Path):
    """Features uncorrelated with target should be dropped."""
    from ml_toolbox.nodes.feature_selector import feature_selector

    # Create data where feature_a correlates with target, constant does not
    train_df = pl.DataFrame({
        "correlated": [float(i % 2) for i in range(100)],    # perfect correlation with target
        "uncorrelated": [float(i) for i in range(100)],       # weak correlation
        "constant": [5.0] * 100,                               # zero correlation
        "target": [i % 2 for i in range(100)],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "correlated": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "uncorrelated": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "constant": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = feature_selector(
            inputs={"train": str(train_path)},
            params={"method": "correlation_with_target", "threshold": 0.5},
        )

    out_df = pl.read_parquet(Path(result["train"]))
    assert "correlated" in out_df.columns
    assert "constant" not in out_df.columns
    assert "target" in out_df.columns


def test_correlation_requires_target(tmp_path: Path):
    """correlation_with_target should fail if no target in .meta.json."""
    from ml_toolbox.nodes.feature_selector import feature_selector

    train_df = pl.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)
    # No .meta.json — no target

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        with pytest.raises(ValueError, match="requires a target column"):
            feature_selector(
                inputs={"train": str(train_path)},
                params={"method": "correlation_with_target", "threshold": 0.1},
            )


# ── Mutual information ──────────────────────────────────────────


def test_mutual_information_drops_uninformative(tmp_path: Path):
    """Features with low MI should be dropped."""
    from ml_toolbox.nodes.feature_selector import feature_selector

    train_df = pl.DataFrame({
        "informative": [float(i % 2) for i in range(200)],   # high MI with target
        "constant": [5.0] * 200,                               # zero MI
        "target": [i % 2 for i in range(200)],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "informative": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "constant": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = feature_selector(
            inputs={"train": str(train_path)},
            params={"method": "mutual_information", "threshold": 0.01},
        )

    out_df = pl.read_parquet(Path(result["train"]))
    assert "informative" in out_df.columns
    assert "constant" not in out_df.columns
    assert "target" in out_df.columns


def test_mutual_information_requires_target(tmp_path: Path):
    """mutual_information should fail if no target in .meta.json."""
    from ml_toolbox.nodes.feature_selector import feature_selector

    train_df = pl.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        with pytest.raises(ValueError, match="requires a target column"):
            feature_selector(
                inputs={"train": str(train_path)},
                params={"method": "mutual_information", "threshold": 0.1},
            )


# ── .meta.json updated ──────────────────────────────────────────


def test_meta_json_updated(tmp_path: Path):
    """.meta.json should reflect removed columns."""
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 0.01})

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()

    meta = json.loads(meta_path.read_text())
    assert "feature_a" in meta["columns"]
    assert "feature_b" in meta["columns"]
    assert "constant" not in meta["columns"]
    assert "near_constant" not in meta["columns"]
    assert meta["target"] == "target"


def test_meta_json_consistent_across_splits(tmp_path: Path):
    """.meta.json should be identical for all splits."""
    result = _run_selector(tmp_path, {"method": "variance_threshold", "threshold": 0.01})

    train_meta = json.loads(Path(result["train"]).with_suffix(".meta.json").read_text())
    val_meta = json.loads(Path(result["val"]).with_suffix(".meta.json").read_text())
    test_meta = json.loads(Path(result["test"]).with_suffix(".meta.json").read_text())

    assert train_meta == val_meta == test_meta


# ── Edge case: all features below threshold ─────────────────────


def test_all_features_below_threshold_errors(tmp_path: Path):
    """Should error if removing all features would leave only the target."""
    from ml_toolbox.nodes.feature_selector import feature_selector

    train_df = pl.DataFrame({
        "a": [5.0] * 50,
        "b": [3.0] * 50,
        "target": [i % 2 for i in range(50)],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    with patch(
        "ml_toolbox.nodes.feature_selector._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        with pytest.raises(ValueError, match="All .* feature columns scored below threshold"):
            feature_selector(
                inputs={"train": str(train_path)},
                params={"method": "variance_threshold", "threshold": 0.01},
            )
