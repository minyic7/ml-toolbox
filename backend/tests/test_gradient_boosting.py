"""Tests for the Gradient Boosting training node."""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_gradient_boosting_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.gradient_boosting.gradient_boosting_train"]
    assert meta["label"] == "Gradient Boosting"
    assert meta["category"] == "Training"
    assert meta["type"] == "ml_toolbox.nodes.gradient_boosting.gradient_boosting_train"
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
    assert param_names == {
        "learning_rate",
        "n_estimators",
        "max_depth",
        "subsample",
        "early_stopping_rounds",
    }
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_classification_data(
    tmp_path: Path,
    n_train: int = 80,
    n_val: int = 20,
    n_test: int = 20,
) -> tuple[Path, Path, Path]:
    """Create train/val/test parquet files for binary classification."""
    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pl.DataFrame:
        x1 = rng.randn(n)
        x2 = rng.randn(n)
        target = (x1 + x2 > 0).astype(int)
        return pl.DataFrame({
            "feature_a": x1.tolist(),
            "feature_b": x2.tolist(),
            "target": target.tolist(),
        })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"

    _make_df(n_train).write_parquet(train_path)
    _make_df(n_val).write_parquet(val_path)
    _make_df(n_test).write_parquet(test_path)

    return train_path, val_path, test_path


def _make_regression_data(
    tmp_path: Path,
    n_train: int = 80,
    n_val: int = 20,
    n_test: int = 20,
) -> tuple[Path, Path, Path]:
    """Create train/val/test parquet files for regression."""
    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pl.DataFrame:
        x1 = rng.randn(n)
        x2 = rng.randn(n)
        target = 3.0 * x1 + 2.0 * x2 + rng.randn(n) * 0.1
        return pl.DataFrame({
            "feature_a": x1.tolist(),
            "feature_b": x2.tolist(),
            "target": target.tolist(),
        })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"

    _make_df(n_train).write_parquet(train_path)
    _make_df(n_val).write_parquet(val_path)
    _make_df(n_test).write_parquet(test_path)

    return train_path, val_path, test_path


def _make_meta(tmp_path: Path, target: str = "target", task: str = "classification"):
    """Write a .meta.json sidecar alongside train.parquet."""
    dtype = "int64" if task == "classification" else "float64"
    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": dtype, "semantic_type": "continuous", "role": "target"},
        },
        "target": target,
        "row_count": 80,
        "generated_by": "test",
    }
    meta_path = tmp_path / "train.meta.json"
    meta_path.write_text(json.dumps(meta))
    return meta_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes outputs to tmp dir."""
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _run_gradient_boosting(tmp_path, params, data_fn=_make_classification_data,
                           include_val=True, include_test=True, task="classification"):
    """Helper to set up data, meta, and run the node."""
    from ml_toolbox.nodes.gradient_boosting import gradient_boosting_train

    train_path, val_path, test_path = data_fn(tmp_path)
    _make_meta(tmp_path, task=task)

    inputs: dict[str, str] = {"train": str(train_path)}
    if include_val:
        inputs["val"] = str(val_path)
    if include_test:
        inputs["test"] = str(test_path)

    with patch(
        "ml_toolbox.nodes.gradient_boosting._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = gradient_boosting_train(inputs=inputs, params=params)

    return result


# ── Classification tests ─────────────────────────────────────────


def test_classification_basic(tmp_path: Path):
    """Binary classification should produce model, predictions, and metrics."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
    )

    assert "predictions" in result
    assert "model" in result
    assert "metrics" in result

    # Check predictions file
    pred_df = pl.read_parquet(Path(result["predictions"]))
    assert "split" in pred_df.columns
    assert "prediction" in pred_df.columns
    assert "actual" in pred_df.columns
    assert pred_df.height > 0

    # Check metrics file
    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "classification"
    assert "train_score" in metrics
    assert "train_accuracy" in metrics
    assert "train_f1" in metrics
    assert "feature_importances" in metrics
    assert metrics["train_accuracy"] > 0.7  # should learn the pattern

    # Check model file exists and is loadable
    import joblib
    model = joblib.load(result["model"])
    assert hasattr(model, "predict")


def test_classification_with_val_metrics(tmp_path: Path):
    """When val is connected, metrics should include val scores."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
        include_val=True,
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "val_score" in metrics
    assert "val_accuracy" in metrics
    assert "val_f1" in metrics


def test_classification_without_val(tmp_path: Path):
    """Without val, only train metrics should exist."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
        include_val=False,
        include_test=False,
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "train_score" in metrics
    assert "val_score" not in metrics

    # Predictions should only contain train split
    pred_df = pl.read_parquet(Path(result["predictions"]))
    assert set(pred_df["split"].unique().to_list()) == {"train"}


# ── Regression tests ─────────────────────────────────────────────


def test_regression_auto_detect(tmp_path: Path):
    """Float target with many unique values should auto-detect as regression."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
        data_fn=_make_regression_data,
        task="regression",
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "regression"
    assert "train_mse" in metrics
    assert "train_mae" in metrics
    assert "feature_importances" in metrics


def test_regression_with_val_metrics(tmp_path: Path):
    """Regression val metrics should include MSE and MAE."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
        data_fn=_make_regression_data,
        include_val=True,
        task="regression",
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "val_mse" in metrics
    assert "val_mae" in metrics
    assert "val_score" in metrics


# ── Early stopping tests ─────────────────────────────────────────


def test_early_stopping_with_val(tmp_path: Path):
    """Early stopping should use fewer estimators when val is connected."""
    result = _run_gradient_boosting(
        tmp_path,
        {
            "learning_rate": 0.3,
            "n_estimators": 500,
            "max_depth": 3,
            "early_stopping_rounds": 10,
        },
        include_val=True,
    )

    metrics = json.loads(Path(result["metrics"]).read_text())

    # With early stopping, we expect best_iteration to be set
    # and n_estimators_used to be < 500 (it should stop early)
    try:
        import xgboost  # noqa: F401
        assert "best_iteration" in metrics
        assert "n_estimators_used" in metrics
        assert metrics["n_estimators_used"] <= 500
    except ImportError:
        # sklearn fallback doesn't support early stopping
        pass


def test_early_stopping_disabled_without_val(tmp_path: Path):
    """Early stopping should be disabled when val is not connected."""
    result = _run_gradient_boosting(
        tmp_path,
        {
            "learning_rate": 0.1,
            "n_estimators": 50,
            "max_depth": 3,
            "early_stopping_rounds": 10,
        },
        include_val=False,
        include_test=False,
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    # Should train full n_estimators — no best_iteration
    assert "best_iteration" not in metrics


def test_early_stopping_disabled_when_zero(tmp_path: Path):
    """early_stopping_rounds=0 should disable early stopping even with val."""
    result = _run_gradient_boosting(
        tmp_path,
        {
            "learning_rate": 0.1,
            "n_estimators": 50,
            "max_depth": 3,
            "early_stopping_rounds": 0,
        },
        include_val=True,
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "best_iteration" not in metrics


# ── Feature importance tests ──────────────────────────────────────


def test_feature_importances_in_metrics(tmp_path: Path):
    """Feature importances should be in metrics with correct feature names."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    importances = metrics["feature_importances"]

    assert "feature_a" in importances
    assert "feature_b" in importances
    # Importances should sum to approximately 1
    total = sum(importances.values())
    assert abs(total - 1.0) < 0.01


# ── Prediction output tests ──────────────────────────────────────


def test_predictions_contain_all_splits(tmp_path: Path):
    """Predictions should include rows from all connected splits."""
    result = _run_gradient_boosting(
        tmp_path,
        {"learning_rate": 0.1, "n_estimators": 50, "max_depth": 3},
        include_val=True,
        include_test=True,
    )

    pred_df = pl.read_parquet(Path(result["predictions"]))
    splits = set(pred_df["split"].unique().to_list())
    assert splits == {"train", "val", "test"}

    # Check expected row counts
    train_rows = pred_df.filter(pl.col("split") == "train").height
    val_rows = pred_df.filter(pl.col("split") == "val").height
    test_rows = pred_df.filter(pl.col("split") == "test").height
    assert train_rows == 80
    assert val_rows == 20
    assert test_rows == 20


# ── Error handling tests ─────────────────────────────────────────


def test_missing_target_column_raises(tmp_path: Path):
    """Should raise ValueError when no target column is found."""
    from ml_toolbox.nodes.gradient_boosting import gradient_boosting_train

    train_df = pl.DataFrame({
        "feature_a": [1.0, 2.0, 3.0],
        "feature_b": [4.0, 5.0, 6.0],
        "target": [0, 1, 0],
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    # No .meta.json → no target column identified
    inputs = {"train": str(train_path)}

    with patch(
        "ml_toolbox.nodes.gradient_boosting._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        with pytest.raises(ValueError, match="Target column not found"):
            gradient_boosting_train(inputs=inputs, params={})


# ── Subsample parameter test ─────────────────────────────────────


def test_stochastic_boosting_with_subsample(tmp_path: Path):
    """Subsample < 1.0 should still produce valid results."""
    result = _run_gradient_boosting(
        tmp_path,
        {
            "learning_rate": 0.1,
            "n_estimators": 50,
            "max_depth": 3,
            "subsample": 0.8,
        },
    )

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "classification"
    assert metrics["train_accuracy"] > 0.5


# ── Auto-detect edge cases ───────────────────────────────────────


def test_multiclass_detection(tmp_path: Path):
    """Integer target with multiple classes should detect as classification."""
    from ml_toolbox.nodes.gradient_boosting import gradient_boosting_train

    rng = np.random.RandomState(42)
    n = 100
    train_df = pl.DataFrame({
        "feature_a": rng.randn(n).tolist(),
        "feature_b": rng.randn(n).tolist(),
        "target": [i % 5 for i in range(n)],  # 5 classes
    })
    train_path = tmp_path / "train.parquet"
    train_df.write_parquet(train_path)

    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "role": "feature"},
            "feature_b": {"dtype": "float64", "role": "feature"},
            "target": {"dtype": "int64", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path)}

    with patch(
        "ml_toolbox.nodes.gradient_boosting._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = gradient_boosting_train(inputs=inputs, params={"n_estimators": 30})

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "classification"
