"""Tests for the Random Forest training node."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_random_forest_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.training.random_forest"]
    assert meta["label"] == "Random Forest"
    assert meta["category"] == "Training"
    assert meta["type"] == "ml_toolbox.nodes.training.random_forest"
    assert meta["inputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    assert meta["outputs"] == [
        {"name": "predictions", "type": "TABLE"},
        {"name": "model", "type": "MODEL"},
        {"name": "metrics", "type": "METRICS"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"n_estimators", "max_depth", "min_samples_split", "n_jobs"}
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_classification_data(
    tmp_path: Path, n_train: int = 80, n_val: int = 10, n_test: int = 10
):
    """Create train/val/test parquets for binary classification."""
    import numpy as np

    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pd.DataFrame:
        return pd.DataFrame({
            "feature_a": rng.randn(n),
            "feature_b": rng.randn(n),
            "feature_c": rng.randn(n),
            "target": rng.randint(0, 2, size=n),
        })

    paths = {}
    for split, n in [("train", n_train), ("val", n_val), ("test", n_test)]:
        df = _make_df(n)
        p = tmp_path / f"{split}.parquet"
        df.to_parquet(p, index=False)
        paths[split] = p

    # Write .meta.json for train
    meta = {
        "target": "target",
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_c": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    return paths


def _make_regression_data(
    tmp_path: Path, n_train: int = 80, n_val: int = 10, n_test: int = 10
):
    """Create train/val/test parquets for regression."""
    import numpy as np

    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pd.DataFrame:
        x1 = rng.randn(n)
        x2 = rng.randn(n)
        return pd.DataFrame({
            "feature_a": x1,
            "feature_b": x2,
            "target": x1 * 2.0 + x2 * 0.5 + rng.randn(n) * 0.1,
        })

    paths = {}
    for split, n in [("train", n_train), ("val", n_val), ("test", n_test)]:
        df = _make_df(n)
        p = tmp_path / f"{split}.parquet"
        df.to_parquet(p, index=False)
        paths[split] = p

    meta = {
        "target": "target",
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "float64", "semantic_type": "continuous", "role": "target"},
        },
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    return paths


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes outputs to tmp_path/out."""
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _run_rf(tmp_path: Path, paths: dict, params: dict | None = None):
    """Helper to run the random_forest node with mocked output paths."""
    from ml_toolbox.nodes.training import random_forest

    params = params or {}
    inputs = {k: str(v) for k, v in paths.items()}

    with patch(
        "ml_toolbox.nodes.training._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        return random_forest(inputs=inputs, params=params)


# ── Classification tests ─────────────────────────────────────────


def test_classification_basic(tmp_path: Path):
    """Random Forest should train and produce all outputs for classification."""
    paths = _make_classification_data(tmp_path)
    result = _run_rf(tmp_path, paths)

    # All three outputs should be present
    assert "predictions" in result
    assert "model" in result
    assert "metrics" in result

    # Predictions should be a file path
    pred_df = pd.read_parquet(result["predictions"])
    assert "prediction" in pred_df.columns
    assert "split" in pred_df.columns
    assert set(pred_df["split"].unique()) == {"train", "val", "test"}
    assert len(pred_df) == 100  # 80 + 10 + 10

    # Model should be an sklearn object (not yet serialized — runner does that)
    from sklearn.ensemble import RandomForestClassifier
    assert isinstance(result["model"], RandomForestClassifier)


def test_classification_metrics(tmp_path: Path):
    """Metrics should contain accuracy, f1, and feature importances."""
    paths = _make_classification_data(tmp_path)
    result = _run_rf(tmp_path, paths)

    metrics = json.loads(Path(result["metrics"]).read_text())

    assert metrics["task"] == "classification"
    assert metrics["report_type"] == "random_forest"

    # Train metrics
    assert "accuracy" in metrics["train_metrics"]
    assert "f1_weighted" in metrics["train_metrics"]
    assert 0 <= metrics["train_metrics"]["accuracy"] <= 1

    # Val metrics
    assert "accuracy" in metrics["val_metrics"]

    # Test metrics
    assert "accuracy" in metrics["test_metrics"]

    # Feature importances
    assert "feature_importances" in metrics
    importances = metrics["feature_importances"]
    assert len(importances) == 3  # 3 features
    assert all("feature" in fi and "importance" in fi for fi in importances)
    # Should be sorted descending
    assert importances[0]["importance"] >= importances[-1]["importance"]
    # Importances should sum to ~1.0
    total = sum(fi["importance"] for fi in importances)
    assert abs(total - 1.0) < 0.01


# ── Regression tests ──────────────────────────────────────────────


def test_regression_basic(tmp_path: Path):
    """Random Forest should auto-detect regression and produce correct metrics."""
    paths = _make_regression_data(tmp_path)
    result = _run_rf(tmp_path, paths)

    metrics = json.loads(Path(result["metrics"]).read_text())

    assert metrics["task"] == "regression"
    assert "rmse" in metrics["train_metrics"]
    assert "mae" in metrics["train_metrics"]
    assert "r2" in metrics["train_metrics"]

    # For this simple linear problem, R² on train should be high
    assert metrics["train_metrics"]["r2"] > 0.8

    # Model should be a regressor
    from sklearn.ensemble import RandomForestRegressor
    assert isinstance(result["model"], RandomForestRegressor)


def test_regression_predictions(tmp_path: Path):
    """Regression predictions should be continuous values."""
    paths = _make_regression_data(tmp_path)
    result = _run_rf(tmp_path, paths)

    pred_df = pd.read_parquet(result["predictions"])
    # Predictions should be float, not int
    assert pred_df["prediction"].dtype == "float64"


# ── Parameter handling ────────────────────────────────────────────


def test_custom_params(tmp_path: Path):
    """Custom hyperparameters should be passed to the model."""
    paths = _make_classification_data(tmp_path)
    result = _run_rf(tmp_path, paths, params={
        "n_estimators": 50,
        "max_depth": 5,
        "min_samples_split": 4,
        "n_jobs": "1",
    })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["params"]["n_estimators"] == 50
    assert metrics["params"]["max_depth"] == 5
    assert metrics["params"]["min_samples_split"] == 4
    assert metrics["params"]["n_jobs"] == 1


# ── Optional splits ──────────────────────────────────────────────


def test_train_only(tmp_path: Path):
    """Node should work with only train input (no val/test)."""
    paths = _make_classification_data(tmp_path)
    train_only = {"train": paths["train"]}
    result = _run_rf(tmp_path, train_only)

    pred_df = pd.read_parquet(result["predictions"])
    assert set(pred_df["split"].unique()) == {"train"}
    assert len(pred_df) == 80

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "train_metrics" in metrics
    assert "val_metrics" not in metrics
    assert "test_metrics" not in metrics


def test_train_and_val_only(tmp_path: Path):
    """Node should work with train + val (no test)."""
    paths = _make_classification_data(tmp_path)
    subset = {"train": paths["train"], "val": paths["val"]}
    result = _run_rf(tmp_path, subset)

    pred_df = pd.read_parquet(result["predictions"])
    assert set(pred_df["split"].unique()) == {"train", "val"}

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "train_metrics" in metrics
    assert "val_metrics" in metrics
    assert "test_metrics" not in metrics
    # Summary should use val metrics when val is available
    assert metrics["summary"] == metrics["val_metrics"]


# ── Error handling ────────────────────────────────────────────────


def test_missing_meta_json_raises(tmp_path: Path):
    """Should raise ValueError when .meta.json is missing."""
    import numpy as np

    rng = np.random.RandomState(42)
    df = pd.DataFrame({
        "feature_a": rng.randn(50),
        "target": rng.randint(0, 2, size=50),
    })
    train_path = tmp_path / "train.parquet"
    df.to_parquet(train_path, index=False)

    with pytest.raises(ValueError, match="Cannot determine target column"):
        _run_rf(tmp_path, {"train": train_path})


def test_missing_target_column_raises(tmp_path: Path):
    """Should raise ValueError when target column doesn't exist in data."""
    import numpy as np

    rng = np.random.RandomState(42)
    df = pd.DataFrame({
        "feature_a": rng.randn(50),
        "feature_b": rng.randn(50),
    })
    train_path = tmp_path / "train.parquet"
    df.to_parquet(train_path, index=False)

    meta = {
        "target": "nonexistent_target",
        "columns": {"feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"}},
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    with pytest.raises(ValueError, match="Cannot determine target column"):
        _run_rf(tmp_path, {"train": train_path})


# ── Feature importances detail ────────────────────────────────────


def test_feature_importances_match_features(tmp_path: Path):
    """Feature importances should list exactly the feature columns."""
    paths = _make_classification_data(tmp_path)
    result = _run_rf(tmp_path, paths)

    metrics = json.loads(Path(result["metrics"]).read_text())
    fi_features = {fi["feature"] for fi in metrics["feature_importances"]}
    assert fi_features == {"feature_a", "feature_b", "feature_c"}
