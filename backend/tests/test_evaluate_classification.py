"""Tests for the classification evaluation node."""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_classification_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluate.classification"]
    assert meta["label"] == "Classification Metrics"
    assert meta["category"] == "Evaluate"
    assert meta["inputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "test", "type": "TABLE"},
    ]
    assert meta["outputs"] == [{"name": "metrics", "type": "METRICS"}]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"target_column"}


def _make_data_and_model(tmp_path: Path, y_values, n_classes=2):
    """Create test data and a trained model. Returns (model_path, test_path)."""
    rng = np.random.RandomState(42)
    n = len(y_values)
    X = rng.normal(0, 1, (n, 3))
    df = pd.DataFrame(X, columns=["f1", "f2", "f3"])
    df["target"] = y_values

    test_path = tmp_path / "test.parquet"
    df.to_parquet(test_path, index=False)

    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model.fit(X, y_values)

    model_path = tmp_path / "model.joblib"
    joblib.dump(model, model_path)

    return model_path, test_path


def _run_evaluate(tmp_path, model_path, test_path, params):
    from ml_toolbox.nodes.evaluate import classification

    metrics_path = tmp_path / "metrics.json"

    def mock_output(name="output", ext=".parquet"):
        return metrics_path

    with patch("ml_toolbox.nodes.evaluate._get_output_path", side_effect=mock_output):
        return classification(
            inputs={"model": str(model_path), "test": str(test_path)},
            params=params,
        )


def test_perfect_predictions_accuracy(tmp_path: Path):
    """Perfect predictions should produce accuracy=1.0."""
    rng = np.random.RandomState(42)
    X = np.array([[1, 0, 0], [-1, 0, 0], [2, 0, 0], [-2, 0, 0]] * 25)
    y = np.array([1, 0, 1, 0] * 25)
    df = pd.DataFrame(X, columns=["f1", "f2", "f3"])
    df["target"] = y

    test_path = tmp_path / "test.parquet"
    df.to_parquet(test_path, index=False)

    # Train a model that will perfectly separate this data
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)

    model_path = tmp_path / "model.joblib"
    joblib.dump(model, model_path)

    result = _run_evaluate(tmp_path, model_path, test_path, {"target_column": "target"})
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert metrics["accuracy"] == 1.0


def test_binary_classification_includes_roc_auc(tmp_path: Path):
    """Binary classification should include ROC AUC in metrics."""
    y = np.array([0, 1] * 50)
    model_path, test_path = _make_data_and_model(tmp_path, y)

    result = _run_evaluate(tmp_path, model_path, test_path, {"target_column": "target"})
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert "roc_auc" in metrics
    assert 0.0 <= metrics["roc_auc"] <= 1.0


def test_multiclass_includes_macro_averaged_metrics(tmp_path: Path):
    """Multiclass classification should include macro-averaged precision, recall, F1."""
    y = np.array([0, 1, 2] * 34)[:100]
    model_path, test_path = _make_data_and_model(tmp_path, y, n_classes=3)

    result = _run_evaluate(tmp_path, model_path, test_path, {"target_column": "target"})
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert "accuracy" in metrics
    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert "confusion_matrix" in metrics
    # ROC AUC should not be present for multiclass
    assert "roc_auc" not in metrics
    # Confusion matrix should be 3x3
    assert len(metrics["confusion_matrix"]) == 3
    assert all(len(row) == 3 for row in metrics["confusion_matrix"])
