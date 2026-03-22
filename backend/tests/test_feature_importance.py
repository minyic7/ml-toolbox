"""Tests for the Feature Importance evaluation node."""

import json
from pathlib import Path
from unittest.mock import patch

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def _train_and_save(tmp_path: Path, model, X, y) -> str:
    model.fit(X, y)
    p = tmp_path / "model.joblib"
    joblib.dump(model, p)
    return str(p)


def test_feature_importance_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluation.feature_importance"]
    assert meta["label"] == "Feature Importance"
    assert meta["category"] == "Evaluation"
    assert meta["inputs"] == [{"name": "model", "type": "MODEL"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]


def test_tree_based_random_forest(tmp_path: Path):
    """RF exposes feature_importances_ — should use tree_importance method."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = pd.DataFrame({"a": np.random.randn(50), "b": np.random.randn(50), "c": np.random.randn(50)})
    y = (X["a"] > 0).astype(int)
    model_path = _train_and_save(tmp_path, RandomForestClassifier(n_estimators=10, random_state=42), X, y)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": model_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "feature_importance"
    assert report["method"] == "tree_importance"
    assert report["summary"]["feature_count"] == 3
    assert report["summary"]["model_type"] == "RandomForestClassifier"
    assert len(report["features"]) == 3

    # Features should be sorted by importance descending
    importances = [f["importance"] for f in report["features"]]
    assert importances == sorted(importances, reverse=True)

    # Importances should sum to ~1
    assert abs(sum(importances) - 1.0) < 1e-4

    # Feature names should come from the DataFrame
    names = {f["name"] for f in report["features"]}
    assert names == {"a", "b", "c"}

    # 'a' should be the most important (target is derived from it)
    assert report["features"][0]["name"] == "a"


def test_tree_based_gradient_boosting(tmp_path: Path):
    """GBT also exposes feature_importances_."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = pd.DataFrame({"x1": np.random.randn(50), "x2": np.random.randn(50)})
    y = 3 * X["x1"] + np.random.randn(50) * 0.1
    model_path = _train_and_save(tmp_path, GradientBoostingRegressor(n_estimators=10, random_state=42), X, y)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": model_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "tree_importance"
    assert report["summary"]["model_type"] == "GradientBoostingRegressor"
    assert report["features"][0]["name"] == "x1"


def test_linear_regression(tmp_path: Path):
    """Linear models use coefficient magnitude."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = pd.DataFrame({"a": np.random.randn(50), "b": np.random.randn(50)})
    y = 5 * X["a"] + 0.1 * X["b"]
    model_path = _train_and_save(tmp_path, LinearRegression(), X, y)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": model_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "coefficient_magnitude"
    assert report["summary"]["model_type"] == "LinearRegression"
    assert len(report["features"]) == 2

    # 'a' has much larger coefficient, should rank first
    assert report["features"][0]["name"] == "a"
    assert report["features"][0]["importance"] > report["features"][1]["importance"]


def test_logistic_regression_multiclass(tmp_path: Path):
    """Multi-class logistic regression has 2D coef_ — should average across classes."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = pd.DataFrame({"f1": np.random.randn(90), "f2": np.random.randn(90), "f3": np.random.randn(90)})
    y = np.array([0] * 30 + [1] * 30 + [2] * 30)
    model_path = _train_and_save(tmp_path, LogisticRegression(max_iter=200, random_state=42), X, y)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": model_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "coefficient_magnitude"
    assert report["summary"]["feature_count"] == 3
    # Should still produce valid normalized importances
    importances = [f["importance"] for f in report["features"]]
    assert abs(sum(importances) - 1.0) < 1e-4


def test_dominant_feature_warning(tmp_path: Path):
    """A feature accounting for >50% importance should trigger a warning."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = pd.DataFrame({"signal": np.random.randn(50), "noise": np.random.randn(50) * 0.001})
    y = X["signal"] * 10
    model_path = _train_and_save(tmp_path, LinearRegression(), X, y)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": model_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    dominant = [w for w in report["warnings"] if w["type"] == "dominant_feature"]
    assert len(dominant) >= 1
    assert "signal" in dominant[0]["message"]


def test_no_feature_names(tmp_path: Path):
    """Model trained on numpy array (no feature_names_in_) should use feature_0, feature_1..."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = np.random.randn(50, 3)
    y = (X[:, 0] > 0).astype(int)
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model.fit(X, y)
    # Remove feature_names_in_ if present
    if hasattr(model, "feature_names_in_"):
        delattr(model, "feature_names_in_")
    p = tmp_path / "model.joblib"
    joblib.dump(model, p)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": str(p)}, params={})

    report = json.loads(Path(result["report"]).read_text())
    names = [f["name"] for f in report["features"]]
    assert names == sorted(names, key=lambda n: -report["features"][names.index(n)]["importance"])
    assert all(n.startswith("feature_") for n in names)


def test_unsupported_model(tmp_path: Path):
    """A model without feature_importances_ or coef_ should produce unsupported report."""
    from ml_toolbox.nodes.evaluation import feature_importance

    # KMeans has neither feature_importances_ nor coef_
    from sklearn.cluster import KMeans

    model = KMeans(n_clusters=2, random_state=42, n_init=10)  # pyright: ignore[reportArgumentType]
    model.fit(np.random.randn(20, 3))
    p = tmp_path / "model.joblib"
    joblib.dump(model, p)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": str(p)}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "unsupported"
    assert report["summary"]["feature_count"] == 0
    assert any(w["type"] == "unsupported_model" for w in report["warnings"])


def test_features_sorted_descending(tmp_path: Path):
    """Feature list must be sorted by importance descending."""
    from ml_toolbox.nodes.evaluation import feature_importance

    np.random.seed(42)
    X = pd.DataFrame({f"f{i}": np.random.randn(50) for i in range(5)})
    y = 5 * X["f0"] + 3 * X["f1"] + X["f2"]
    model_path = _train_and_save(tmp_path, LinearRegression(), X, y)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = feature_importance(inputs={"model": model_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    importances = [f["importance"] for f in report["features"]]
    assert importances == sorted(importances, reverse=True)
