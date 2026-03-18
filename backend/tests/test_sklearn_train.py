"""Tests for the sklearn estimator training node."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_sklearn_train_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.train.sklearn_train"]
    assert meta["label"] == "Train sklearn Model"
    assert meta["category"] == "Train"
    assert meta["inputs"] == [{"name": "train", "type": "TABLE"}]
    assert meta["outputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "metrics", "type": "METRICS"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"estimator", "target_column", "hyperparams"}


def _make_regression_df(tmp_path: Path) -> Path:
    """Create a simple regression dataset."""
    rng = np.random.RandomState(42)
    X = rng.normal(0, 1, (100, 3))
    y = X @ np.array([1.5, -2.0, 0.5]) + rng.normal(0, 0.1, 100)
    df = pd.DataFrame(X, columns=["f1", "f2", "f3"])
    df["target"] = y
    path = tmp_path / "train.parquet"
    df.to_parquet(path, index=False)
    return path


def _make_classification_df(tmp_path: Path) -> Path:
    """Create a simple classification dataset."""
    rng = np.random.RandomState(42)
    X = rng.normal(0, 1, (100, 3))
    y = (X[:, 0] + X[:, 1] > 0).astype(int)
    df = pd.DataFrame(X, columns=["f1", "f2", "f3"])
    df["label"] = y
    path = tmp_path / "train.parquet"
    df.to_parquet(path, index=False)
    return path


def _run_train(tmp_path, input_path, params):
    from ml_toolbox.nodes.train import sklearn_train

    model_path = tmp_path / "model.joblib"
    metrics_path = tmp_path / "metrics.json"

    call_count = {"n": 0}
    paths = [model_path, metrics_path]

    def mock_output(name="output", ext=".parquet"):
        idx = call_count["n"]
        call_count["n"] += 1
        return paths[idx]

    with patch("ml_toolbox.nodes.train._get_output_path", side_effect=mock_output):
        return sklearn_train(
            inputs={"train": str(input_path)},
            params=params,
        )


def test_linear_regression_fitted(tmp_path: Path):
    """Train LinearRegression on simple data and verify the model is fitted."""
    import joblib

    input_path = _make_regression_df(tmp_path)
    result = _run_train(tmp_path, input_path, {
        "estimator": "LinearRegression",
        "target_column": "target",
        "hyperparams": "{}",
    })

    model = joblib.load(result["model"])
    assert hasattr(model, "coef_"), "Model should be fitted (have coef_)"
    assert model.coef_.shape == (3,)


def test_random_forest_classifier_accuracy_metric(tmp_path: Path):
    """Train RandomForestClassifier and verify accuracy metric exists."""
    import json

    input_path = _make_classification_df(tmp_path)
    result = _run_train(tmp_path, input_path, {
        "estimator": "RandomForestClassifier",
        "target_column": "label",
        "hyperparams": "{}",
    })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "accuracy" in metrics
    assert "train_score" in metrics
    assert "feature_importances" in metrics
    assert 0.0 <= metrics["accuracy"] <= 1.0


def test_custom_hyperparams_applied(tmp_path: Path):
    """Custom hyperparameters should be passed to the estimator."""
    import joblib

    input_path = _make_classification_df(tmp_path)
    result = _run_train(tmp_path, input_path, {
        "estimator": "RandomForestClassifier",
        "target_column": "label",
        "hyperparams": '{"n_estimators": 5, "max_depth": 2}',
    })

    model = joblib.load(result["model"])
    assert model.n_estimators == 5
    assert model.max_depth == 2


def test_invalid_estimator_raises_error(tmp_path: Path):
    """Invalid estimator name should raise a clear ValueError."""
    input_path = _make_classification_df(tmp_path)

    with pytest.raises(ValueError, match="Unknown estimator 'NotARealEstimator'"):
        _run_train(tmp_path, input_path, {
            "estimator": "NotARealEstimator",
            "target_column": "label",
            "hyperparams": "{}",
        })
