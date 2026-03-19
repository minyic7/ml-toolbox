"""Tests for the feature importance node."""

from pathlib import Path
from unittest.mock import patch

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


class _DummyModel:
    """A model with no feature_importances_ or coef_ attribute."""

    pass


def test_feature_importance_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluate.feature_importance"]
    assert meta["label"] == "Feature Importance"
    assert meta["category"] == "Evaluate"
    assert meta["inputs"] == [
        {"name": "model", "type": "MODEL"},
        {"name": "train", "type": "TABLE"},
    ]
    assert meta["outputs"] == [{"name": "importances", "type": "ARRAY"}]


def _make_model_and_data(tmp_path: Path, model):
    """Train model on simple data and return (model_path, train_path)."""
    rng = np.random.RandomState(42)
    X = rng.normal(0, 1, (100, 4))
    y = (X[:, 0] + X[:, 1] > 0).astype(int)

    df = pd.DataFrame(X, columns=["f1", "f2", "f3", "f4"])
    df["target"] = y

    train_path = tmp_path / "train.parquet"
    df.to_parquet(train_path, index=False)

    model.fit(X, y)
    model_path = tmp_path / "model.pkl"
    joblib.dump(model, model_path)

    return model_path, train_path


def _run_feature_importance(tmp_path, model_path, train_path):
    from ml_toolbox.nodes.evaluate import feature_importance

    npy_path = tmp_path / "importances.npy"

    def mock_output(name="output", ext=".parquet"):
        return npy_path

    with patch("ml_toolbox.nodes.evaluate._get_output_path", side_effect=mock_output):
        return feature_importance(
            inputs={"model": str(model_path), "train": str(train_path)},
            params={},
        )


def test_random_forest_feature_importances(tmp_path: Path):
    """RandomForest model should produce valid feature importances."""
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model_path, train_path = _make_model_and_data(tmp_path, model)

    result = _run_feature_importance(tmp_path, model_path, train_path)
    arr = np.load(result["importances"])

    # 5 columns in training data (f1, f2, f3, f4, target)
    assert len(arr) == 4  # feature_importances_ matches n_features used in fit
    assert arr.sum() > 0


def test_linear_model_coef_fallback(tmp_path: Path):
    """Linear model should fall back to coef_ attribute."""
    model = LogisticRegression(random_state=42, max_iter=200)
    model_path, train_path = _make_model_and_data(tmp_path, model)

    result = _run_feature_importance(tmp_path, model_path, train_path)
    arr = np.load(result["importances"])

    assert len(arr) == 4
    assert arr.sum() > 0


def test_model_without_importances_returns_zeros(tmp_path: Path):
    """Model without feature_importances_ or coef_ should return zeros."""
    rng = np.random.RandomState(42)
    X = rng.normal(0, 1, (100, 3))
    df = pd.DataFrame(X, columns=["a", "b", "c"])

    train_path = tmp_path / "train.parquet"
    df.to_parquet(train_path, index=False)

    model_path = tmp_path / "model.pkl"
    joblib.dump(_DummyModel(), model_path)

    result = _run_feature_importance(tmp_path, model_path, train_path)
    arr = np.load(result["importances"])

    assert len(arr) == 3
    assert np.all(arr == 0)


def test_output_is_valid_npy(tmp_path: Path):
    """Output should be a valid .npy file."""
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model_path, train_path = _make_model_and_data(tmp_path, model)

    result = _run_feature_importance(tmp_path, model_path, train_path)
    output_path = Path(result["importances"])

    assert output_path.exists()
    assert output_path.suffix == ".npy"
    arr = np.load(output_path)
    assert isinstance(arr, np.ndarray)
