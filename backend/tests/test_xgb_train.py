"""Tests for the XGBoost training node."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_xgb_train_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.train.xgb_train"]
    assert meta["label"] == "Train XGBoost"
    assert meta["category"] == "Train"
    assert meta["inputs"] == [{"name": "train", "type": "TABLE"}]
    assert meta["outputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "metrics", "type": "METRICS"},
    ]
    assert len(meta["params"]) == 5
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {
        "objective",
        "target_column",
        "n_estimators",
        "max_depth",
        "learning_rate",
    }


def _make_classification_df(tmp_path: Path) -> Path:
    """Create a binary classification dataset."""
    rng = np.random.RandomState(42)
    n = 200
    df = pd.DataFrame({
        "feat_a": rng.normal(0, 1, n),
        "feat_b": rng.normal(0, 1, n),
        "feat_c": rng.normal(0, 1, n),
        "target": rng.choice([0, 1], n),
    })
    path = tmp_path / "train.parquet"
    df.to_parquet(path, index=False)
    return path


def _make_regression_df(tmp_path: Path) -> Path:
    """Create a regression dataset."""
    rng = np.random.RandomState(42)
    n = 200
    x = rng.normal(0, 1, n)
    df = pd.DataFrame({
        "feat_a": x,
        "feat_b": rng.normal(0, 1, n),
        "value": x * 3 + rng.normal(0, 0.5, n),
    })
    path = tmp_path / "train.parquet"
    df.to_parquet(path, index=False)
    return path


def _patch_output(tmp_path):
    """Return a patched _get_output_path that writes to tmp_path."""
    def _fake_output(name="output", ext=".parquet"):
        return tmp_path / f"{name}{ext}"
    return patch("ml_toolbox.nodes.train._get_output_path", side_effect=_fake_output)


def test_classification_model_is_fitted(tmp_path: Path):
    """Train on classification data and verify the model is fitted."""
    from ml_toolbox.nodes.train import xgb_train

    input_path = _make_classification_df(tmp_path)

    with _patch_output(tmp_path):
        result = xgb_train(
            inputs={"train": str(input_path)},
            params={
                "objective": "binary:logistic",
                "target_column": "target",
                "n_estimators": 50,
                "max_depth": 3,
                "learning_rate": 0.1,
            },
        )

    # Model should be a fitted XGBClassifier (raw object, not path)
    model = result["model"]
    from xgboost import XGBClassifier

    assert isinstance(model, XGBClassifier)
    # Fitted models have a classes_ attribute
    assert hasattr(model, "classes_")

    # Metrics should be a JSON file path with accuracy and f1
    import json

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "accuracy" in metrics
    assert "f1" in metrics
    assert metrics["objective"] == "binary:logistic"


def test_regression_with_squarederror(tmp_path: Path):
    """Train on regression data with reg:squarederror."""
    from ml_toolbox.nodes.train import xgb_train

    input_path = _make_regression_df(tmp_path)

    with _patch_output(tmp_path):
        result = xgb_train(
            inputs={"train": str(input_path)},
            params={
                "objective": "reg:squarederror",
                "target_column": "value",
                "n_estimators": 50,
                "max_depth": 3,
                "learning_rate": 0.1,
            },
        )

    from xgboost import XGBRegressor

    model = result["model"]
    assert isinstance(model, XGBRegressor)

    import json

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "r2" in metrics
    assert "mse" in metrics
    assert "mae" in metrics
    assert metrics["objective"] == "reg:squarederror"


def test_custom_hyperparameters_applied(tmp_path: Path):
    """Verify custom hyperparameters are passed to the model."""
    from ml_toolbox.nodes.train import xgb_train

    input_path = _make_classification_df(tmp_path)

    with _patch_output(tmp_path):
        result = xgb_train(
            inputs={"train": str(input_path)},
            params={
                "objective": "binary:logistic",
                "target_column": "target",
                "n_estimators": 200,
                "max_depth": 10,
                "learning_rate": 0.05,
            },
        )

    model = result["model"]
    assert model.n_estimators == 200
    assert model.max_depth == 10
    assert model.learning_rate == 0.05

    import json

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["n_estimators"] == 200
    assert metrics["max_depth"] == 10
    assert metrics["learning_rate"] == 0.05
