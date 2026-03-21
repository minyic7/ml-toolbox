"""Tests for the individual sklearn training nodes."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _run_node(tmp_path: Path, node_func, input_path: Path, params: dict) -> dict:
    """Run a train node with mocked output paths."""
    model_path = tmp_path / "model.joblib"
    metrics_path = tmp_path / "metrics.json"

    call_count = {"n": 0}
    paths = [model_path, metrics_path]

    def mock_output(name="output", ext=".parquet"):
        idx = call_count["n"]
        call_count["n"] += 1
        return paths[idx]

    with patch("ml_toolbox.nodes.train._get_output_path", side_effect=mock_output):
        return node_func(
            inputs={"train": str(input_path)},
            params=params,
        )


# ---------------------------------------------------------------------------
# Registry / metadata tests
# ---------------------------------------------------------------------------


CLASSIFIER_NODES = [
    ("ml_toolbox.nodes.train.random_forest_classifier", "Random Forest Classifier"),
    ("ml_toolbox.nodes.train.gradient_boosting_classifier", "Gradient Boosting Classifier"),
    ("ml_toolbox.nodes.train.logistic_regression", "Logistic Regression"),
    ("ml_toolbox.nodes.train.svc_classifier", "SVC"),
    ("ml_toolbox.nodes.train.decision_tree_classifier", "Decision Tree Classifier"),
    ("ml_toolbox.nodes.train.knn_classifier", "KNN Classifier"),
]

REGRESSOR_NODES = [
    ("ml_toolbox.nodes.train.linear_regression", "Linear Regression"),
    ("ml_toolbox.nodes.train.random_forest_regressor", "Random Forest Regressor"),
    ("ml_toolbox.nodes.train.gradient_boosting_regressor", "Gradient Boosting Regressor"),
    ("ml_toolbox.nodes.train.svr_train", "SVR"),
]


def test_old_sklearn_train_removed():
    """The old sklearn_train node must no longer be registered."""
    assert "ml_toolbox.nodes.train.sklearn_train" not in NODE_REGISTRY


@pytest.mark.parametrize("node_type,label", CLASSIFIER_NODES)
def test_classifier_nodes_registered(node_type: str, label: str):
    """Each individual sklearn classifier should be registered with correct metadata."""
    assert node_type in NODE_REGISTRY
    meta = NODE_REGISTRY[node_type]
    assert meta["label"] == label
    assert meta["category"] == "Classification"
    assert meta["inputs"] == [{"name": "train", "type": "TABLE"}]
    assert meta["outputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "metrics", "type": "METRICS"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert "target_column" in param_names


@pytest.mark.parametrize("node_type,label", REGRESSOR_NODES)
def test_regressor_nodes_registered(node_type: str, label: str):
    """Each individual sklearn regressor should be registered with correct metadata."""
    assert node_type in NODE_REGISTRY
    meta = NODE_REGISTRY[node_type]
    assert meta["label"] == label
    assert meta["category"] == "Regression"
    assert meta["inputs"] == [{"name": "train", "type": "TABLE"}]
    assert meta["outputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "metrics", "type": "METRICS"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert "target_column" in param_names


# ---------------------------------------------------------------------------
# Functional tests — classifiers
# ---------------------------------------------------------------------------


def test_random_forest_classifier(tmp_path: Path):
    import json

    import joblib

    from ml_toolbox.nodes.train import random_forest_classifier

    input_path = _make_classification_df(tmp_path)
    result = _run_node(tmp_path, random_forest_classifier, input_path, {
        "target_column": "label",
        "n_estimators": 10,
        "max_depth": 3,
    })

    model = joblib.load(result["model"])
    assert model.n_estimators == 10
    assert model.max_depth == 3

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "accuracy" in metrics
    assert "train_score" in metrics


def test_logistic_regression(tmp_path: Path):
    import joblib

    from ml_toolbox.nodes.train import logistic_regression

    input_path = _make_classification_df(tmp_path)
    result = _run_node(tmp_path, logistic_regression, input_path, {
        "target_column": "label",
        "C": 0.5,
        "max_iter": 200,
    })

    model = joblib.load(result["model"])
    assert hasattr(model, "coef_")


# ---------------------------------------------------------------------------
# Functional tests — regressors
# ---------------------------------------------------------------------------


def test_linear_regression(tmp_path: Path):
    import joblib

    from ml_toolbox.nodes.train import linear_regression

    input_path = _make_regression_df(tmp_path)
    result = _run_node(tmp_path, linear_regression, input_path, {
        "target_column": "target",
    })

    model = joblib.load(result["model"])
    assert hasattr(model, "coef_")
    assert model.coef_.shape == (3,)


def test_random_forest_regressor(tmp_path: Path):
    import joblib

    from ml_toolbox.nodes.train import random_forest_regressor

    input_path = _make_regression_df(tmp_path)
    result = _run_node(tmp_path, random_forest_regressor, input_path, {
        "target_column": "target",
        "n_estimators": 10,
        "max_depth": 5,
    })

    model = joblib.load(result["model"])
    assert model.n_estimators == 10


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_missing_target_column_raises(tmp_path: Path):
    from ml_toolbox.nodes.train import random_forest_classifier

    input_path = _make_classification_df(tmp_path)
    with pytest.raises(ValueError, match="target_column is required"):
        _run_node(tmp_path, random_forest_classifier, input_path, {
            "target_column": "",
        })
