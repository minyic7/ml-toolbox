"""Tests for the regression evaluation node."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


class _FakeModel:
    """A picklable fake model that returns pre-set predictions."""

    def __init__(self, predictions):
        self._predictions = np.array(predictions)

    def predict(self, X):
        return self._predictions


def _make_test_data(tmp_path: Path, y_true, y_pred=None):
    """Create a model and test parquet for regression evaluation.

    If y_pred is None, predictions equal y_true (perfect).
    Returns (model_path, test_parquet_path).
    """
    import joblib

    rng = np.random.RandomState(42)
    X = rng.normal(0, 1, (len(y_true), 2))
    df = pd.DataFrame(X, columns=["f1", "f2"])
    df["target"] = y_true

    test_path = tmp_path / "test.parquet"
    df.to_parquet(test_path, index=False)

    preds = y_true if y_pred is None else y_pred
    model = _FakeModel(preds)

    model_path = tmp_path / "model.joblib"
    joblib.dump(model, model_path)

    return model_path, test_path


def _run_evaluate(tmp_path, model_path, test_path, params):
    from ml_toolbox.nodes.evaluate import regression

    metrics_path = tmp_path / "metrics.json"

    def mock_output(name="output", ext=".parquet"):
        return metrics_path

    with patch("ml_toolbox.nodes.evaluate._get_output_path", side_effect=mock_output):
        return regression(
            inputs={"model": str(model_path), "test": str(test_path)},
            params=params,
        )


def test_regression_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluate.regression"]
    assert meta["label"] == "Regression Metrics"
    assert meta["category"] == "Evaluate"
    assert meta["inputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "test", "type": "TABLE"},
    ]
    assert meta["outputs"] == [{"name": "metrics", "type": "METRICS"}]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"target_column"}


def test_perfect_predictions(tmp_path: Path):
    """Perfect predictions should produce RMSE=0 and R²=1.0."""
    import json

    y_true = [1.0, 2.0, 3.0, 4.0, 5.0]
    model_path, test_path = _make_test_data(tmp_path, y_true)

    result = _run_evaluate(tmp_path, model_path, test_path, {"target_column": "target"})

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["rmse"] == 0.0
    assert metrics["r2"] == 1.0
    assert metrics["mae"] == 0.0
    assert metrics["mape"] == 0.0


def test_all_metric_keys_present(tmp_path: Path):
    """Output must contain rmse, mae, r2, and mape keys."""
    import json

    y_true = [1.0, 2.0, 3.0, 4.0, 5.0]
    y_pred = [1.1, 2.2, 2.8, 4.3, 4.7]
    model_path, test_path = _make_test_data(tmp_path, y_true, y_pred)

    result = _run_evaluate(tmp_path, model_path, test_path, {"target_column": "target"})

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert set(metrics.keys()) == {"rmse", "mae", "r2", "mape"}
