"""Tests for the Model Comparison Evaluation node."""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
import joblib

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_model_comparison_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluate.model_comparison"]
    assert meta["label"] == "Model Comparison"
    assert meta["category"] == "Evaluation"
    assert meta["type"] == "ml_toolbox.nodes.evaluate.model_comparison"
    assert meta["inputs"] == [
        {"name": "model_a", "type": "MODEL"},
        {"name": "model_b", "type": "MODEL"},
        {"name": "model_c", "type": "MODEL"},
        {"name": "model_d", "type": "MODEL"},
        {"name": "test", "type": "TABLE"},
    ]
    assert meta["outputs"] == [
        {"name": "report", "type": "METRICS"},
    ]
    assert len(meta["params"]) == 1
    assert meta["params"][0]["name"] == "target_column"
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _mock_output_factory(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".json") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _make_classification_data(tmp_path: Path, n: int = 100):
    """Create a simple classification dataset and two trained models."""
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "feature_a": rng.randn(n),
        "feature_b": rng.randn(n),
    })
    y = (X["feature_a"] + X["feature_b"] > 0).astype(int)
    test_df = X.copy()
    test_df["target"] = y

    # Save test set
    test_path = tmp_path / "test.parquet"
    test_df.to_parquet(test_path)

    # Write meta.json
    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "test.meta.json").write_text(json.dumps(meta))

    # Train two models
    model_a = LogisticRegression(random_state=42)
    model_a.fit(X, y)
    model_a_path = tmp_path / "model_a.joblib"
    joblib.dump(model_a, model_a_path)

    model_b = DecisionTreeClassifier(random_state=42, max_depth=3)
    model_b.fit(X, y)
    model_b_path = tmp_path / "model_b.joblib"
    joblib.dump(model_b, model_b_path)

    return test_path, model_a_path, model_b_path


def _make_regression_data(tmp_path: Path, n: int = 100):
    """Create a simple regression dataset and two trained models."""
    rng = np.random.RandomState(42)
    X = pd.DataFrame({
        "feature_a": rng.randn(n),
        "feature_b": rng.randn(n),
    })
    y = 3 * X["feature_a"] + 2 * X["feature_b"] + rng.randn(n) * 0.1
    test_df = X.copy()
    test_df["target"] = y

    test_path = tmp_path / "test.parquet"
    test_df.to_parquet(test_path)

    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "float64", "semantic_type": "continuous", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "test.meta.json").write_text(json.dumps(meta))

    model_a = LinearRegression()
    model_a.fit(X, y)
    model_a_path = tmp_path / "model_a.joblib"
    joblib.dump(model_a, model_a_path)

    model_b = DecisionTreeRegressor(random_state=42, max_depth=3)
    model_b.fit(X, y)
    model_b_path = tmp_path / "model_b.joblib"
    joblib.dump(model_b, model_b_path)

    return test_path, model_a_path, model_b_path


# ── Classification tests ────────────────────────────────────────


def test_classification_two_models(tmp_path: Path):
    """Two classification models should produce accuracy/precision/recall/f1."""
    from ml_toolbox.nodes.evaluate import model_comparison

    test_path, model_a_path, model_b_path = _make_classification_data(tmp_path)

    inputs = {
        "model_a": str(model_a_path),
        "model_b": str(model_b_path),
        "test": str(test_path),
    }

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())

    assert report["report_type"] == "model_comparison"
    assert report["summary"]["task_type"] == "classification"
    assert report["summary"]["models_compared"] == 2
    assert report["summary"]["test_rows"] == 100

    # Check comparison table
    table = report["comparison_table"]
    assert set(table["metric_names"]) == {"accuracy", "precision", "recall", "f1_score"}
    assert len(table["models"]) == 2

    # Values should be between 0 and 1
    for metric in table["metric_names"]:
        for val in table["values"][metric]:
            assert 0.0 <= val <= 1.0, f"{metric}={val} out of range"

    # Best should be populated
    for metric in table["metric_names"]:
        assert len(table["best"][metric]) >= 1

    # Models info
    assert len(report["models"]) == 2
    assert report["models"][0]["port"] == "model_a"
    assert report["models"][1]["port"] == "model_b"


def test_regression_two_models(tmp_path: Path):
    """Two regression models should produce MSE/RMSE/MAE/R2."""
    from ml_toolbox.nodes.evaluate import model_comparison

    test_path, model_a_path, model_b_path = _make_regression_data(tmp_path)

    inputs = {
        "model_a": str(model_a_path),
        "model_b": str(model_b_path),
        "test": str(test_path),
    }

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())

    assert report["summary"]["task_type"] == "regression"
    assert set(report["comparison_table"]["metric_names"]) == {"mse", "rmse", "mae", "r2"}

    # Linear regression should outperform decision tree on this linear data
    table = report["comparison_table"]
    model_a_r2 = table["values"]["r2"][0]
    model_b_r2 = table["values"]["r2"][1]
    assert model_a_r2 > model_b_r2, "LinearRegression should beat DecisionTree on linear data"


def test_three_models(tmp_path: Path):
    """Node should accept 3 models (model_c optional)."""
    from ml_toolbox.nodes.evaluate import model_comparison

    rng = np.random.RandomState(42)
    X = pd.DataFrame({"f1": rng.randn(80), "f2": rng.randn(80)})
    y = (X["f1"] > 0).astype(int)
    test_df = X.copy()
    test_df["target"] = y

    test_path = tmp_path / "test.parquet"
    test_df.to_parquet(test_path)
    meta = {"target": "target", "columns": {}}
    (tmp_path / "test.meta.json").write_text(json.dumps(meta))

    # Train 3 models
    paths = {}
    for name, clf in [
        ("model_a", LogisticRegression(random_state=1)),
        ("model_b", DecisionTreeClassifier(random_state=2, max_depth=2)),
        ("model_c", DecisionTreeClassifier(random_state=3, max_depth=5)),
    ]:
        clf.fit(X, y)
        p = tmp_path / f"{name}.joblib"
        joblib.dump(clf, p)
        paths[name] = str(p)

    inputs = {**paths, "test": str(test_path)}

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["models_compared"] == 3
    assert len(report["comparison_table"]["models"]) == 3


def test_four_models(tmp_path: Path):
    """Node should accept the maximum 4 models."""
    from ml_toolbox.nodes.evaluate import model_comparison

    rng = np.random.RandomState(42)
    X = pd.DataFrame({"f1": rng.randn(60), "f2": rng.randn(60)})
    y = (X["f1"] > 0).astype(int)
    test_df = X.copy()
    test_df["target"] = y

    test_path = tmp_path / "test.parquet"
    test_df.to_parquet(test_path)
    meta = {"target": "target", "columns": {}}
    (tmp_path / "test.meta.json").write_text(json.dumps(meta))

    paths = {}
    for name, clf in [
        ("model_a", LogisticRegression(random_state=1)),
        ("model_b", DecisionTreeClassifier(random_state=2, max_depth=1)),
        ("model_c", DecisionTreeClassifier(random_state=3, max_depth=3)),
        ("model_d", DecisionTreeClassifier(random_state=4, max_depth=5)),
    ]:
        clf.fit(X, y)
        p = tmp_path / f"{name}.joblib"
        joblib.dump(clf, p)
        paths[name] = str(p)

    inputs = {**paths, "test": str(test_path)}

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["models_compared"] == 4
    assert len(report["comparison_table"]["models"]) == 4


# ── Error handling ──────────────────────────────────────────────


def test_fewer_than_two_models_raises(tmp_path: Path):
    """Should raise ValueError when only 1 model is provided."""
    from ml_toolbox.nodes.evaluate import model_comparison

    test_path, model_a_path, _ = _make_classification_data(tmp_path)

    inputs = {
        "model_a": str(model_a_path),
        "test": str(test_path),
    }

    with pytest.raises(ValueError, match="at least 2 model"):
        model_comparison(inputs=inputs, params={"target_column": "auto"})


def test_missing_test_set_raises(tmp_path: Path):
    """Should raise ValueError when test input is missing."""
    from ml_toolbox.nodes.evaluate import model_comparison

    _, model_a_path, model_b_path = _make_classification_data(tmp_path)

    inputs = {
        "model_a": str(model_a_path),
        "model_b": str(model_b_path),
    }

    with pytest.raises(ValueError, match="test TABLE"):
        model_comparison(inputs=inputs, params={"target_column": "auto"})


# ── Edge cases ──────────────────────────────────────────────────


def test_no_meta_json_uses_last_column(tmp_path: Path):
    """Without .meta.json, should fall back to last column as target."""
    from ml_toolbox.nodes.evaluate import model_comparison

    rng = np.random.RandomState(42)
    X = pd.DataFrame({"a": rng.randn(60), "b": rng.randn(60)})
    y = (X["a"] > 0).astype(int)
    test_df = X.copy()
    test_df["target"] = y

    test_path = tmp_path / "test.parquet"
    test_df.to_parquet(test_path)
    # No meta.json written

    model_a = LogisticRegression(random_state=42)
    model_a.fit(X, y)
    model_a_path = tmp_path / "model_a.joblib"
    joblib.dump(model_a, model_a_path)

    model_b = DecisionTreeClassifier(random_state=42)
    model_b.fit(X, y)
    model_b_path = tmp_path / "model_b.joblib"
    joblib.dump(model_b, model_b_path)

    inputs = {
        "model_a": str(model_a_path),
        "model_b": str(model_b_path),
        "test": str(test_path),
    }

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["models_compared"] == 2


def test_small_test_set_warning(tmp_path: Path):
    """Test set < 50 rows should produce a warning."""
    from ml_toolbox.nodes.evaluate import model_comparison

    rng = np.random.RandomState(42)
    X = pd.DataFrame({"a": rng.randn(30), "b": rng.randn(30)})
    y = (X["a"] > 0).astype(int)
    test_df = X.copy()
    test_df["target"] = y

    test_path = tmp_path / "test.parquet"
    test_df.to_parquet(test_path)
    meta = {"target": "target", "columns": {}}
    (tmp_path / "test.meta.json").write_text(json.dumps(meta))

    model_a = LogisticRegression(random_state=42)
    model_a.fit(X, y)
    model_a_path = tmp_path / "model_a.joblib"
    joblib.dump(model_a, model_a_path)

    model_b = DecisionTreeClassifier(random_state=42)
    model_b.fit(X, y)
    model_b_path = tmp_path / "model_b.joblib"
    joblib.dump(model_b, model_b_path)

    inputs = {
        "model_a": str(model_a_path),
        "model_b": str(model_b_path),
        "test": str(test_path),
    }

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())
    warning_types = [w["type"] for w in report["warnings"]]
    assert "small_test_set" in warning_types


def test_best_values_highlighted(tmp_path: Path):
    """Best model per metric should be correctly identified."""
    from ml_toolbox.nodes.evaluate import model_comparison

    test_path, model_a_path, model_b_path = _make_classification_data(tmp_path)

    inputs = {
        "model_a": str(model_a_path),
        "model_b": str(model_b_path),
        "test": str(test_path),
    }

    with patch(
        "ml_toolbox.nodes.evaluate._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = model_comparison(inputs=inputs, params={"target_column": "auto"})

    report = json.loads(Path(result["report"]).read_text())
    table = report["comparison_table"]

    # Each metric should have at least one best model
    for metric in table["metric_names"]:
        assert len(table["best"][metric]) >= 1
        # Best model name should be in the models list
        for best_name in table["best"][metric]:
            assert best_name in table["models"]
