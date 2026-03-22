"""Tests for the Decision Tree Training node."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_decision_tree_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.training.decision_tree"]
    assert meta["label"] == "Decision Tree"
    assert meta["category"] == "Training"
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
    assert param_names == {"max_depth", "min_samples_split", "criterion"}
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _mock_output_factory(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _make_classification_data(
    tmp_path: Path,
    n_train: int = 80,
    n_val: int = 20,
    n_test: int = 20,
):
    """Create train/val/test parquet files for a binary classification task."""
    import numpy as np

    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pl.DataFrame:
        f1 = rng.rand(n).tolist()
        f2 = rng.rand(n).tolist()
        target = [int(f1[i] + f2[i] > 1.0) for i in range(n)]
        return pl.DataFrame({"feature_a": f1, "feature_b": f2, "target": target})

    paths = {}
    for name, n in [("train", n_train), ("val", n_val), ("test", n_test)]:
        p = tmp_path / f"{name}.parquet"
        _make_df(n).write_parquet(p)
        paths[name] = str(p)

    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))
    return paths


def _make_regression_data(
    tmp_path: Path,
    n_train: int = 80,
    n_val: int = 20,
):
    """Create train/val parquet files for a regression task."""
    import numpy as np

    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pl.DataFrame:
        f1 = rng.rand(n).tolist()
        f2 = rng.rand(n).tolist()
        target = [f1[i] * 2.0 + f2[i] * 3.0 + rng.normal(0, 0.1) for i in range(n)]
        return pl.DataFrame({"feature_a": f1, "feature_b": f2, "price": target})

    paths = {}
    for name, n in [("train", n_train), ("val", n_val)]:
        p = tmp_path / f"{name}.parquet"
        _make_df(n).write_parquet(p)
        paths[name] = str(p)

    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "price": {"dtype": "float64", "semantic_type": "continuous", "role": "target"},
        },
        "target": "price",
    }
    (tmp_path / "train.meta.json").write_text(json.dumps(meta))
    return paths


def _run_decision_tree(tmp_path, inputs, params):
    from ml_toolbox.nodes.training import decision_tree

    with patch(
        "ml_toolbox.nodes.training._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        return decision_tree(inputs=inputs, params=params)


# ── Classification tests ─────────────────────────────────────────


def test_classification_basic(tmp_path: Path):
    """Should train a classifier and produce predictions + metrics."""
    inputs = _make_classification_data(tmp_path)
    result = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 5, "min_samples_split": 2, "criterion": "gini",
    })

    assert "predictions" in result
    assert "model" in result
    assert "metrics" in result

    # Predictions should have all rows from all splits
    pred_df = pl.read_parquet(Path(result["predictions"]))
    assert pred_df.height == 120  # 80 + 20 + 20
    assert "prediction" in pred_df.columns
    assert "split" in pred_df.columns
    splits = pred_df["split"].unique().to_list()
    assert set(splits) == {"train", "val", "test"}

    # Metrics should have classification metrics
    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "classification"
    for split in ("train", "val", "test"):
        assert "accuracy" in metrics[split]
        assert "f1" in metrics[split]
        assert 0.0 <= metrics[split]["accuracy"] <= 1.0


def test_classification_entropy_criterion(tmp_path: Path):
    """Entropy criterion should work for classification."""
    inputs = _make_classification_data(tmp_path)
    result = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 5, "min_samples_split": 2, "criterion": "entropy",
    })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "classification"
    assert "accuracy" in metrics["train"]


def test_classification_auto_corrects_regression_criterion(tmp_path: Path):
    """If user selects squared_error for classification, it should auto-correct to gini."""
    inputs = _make_classification_data(tmp_path)
    with pytest.warns(UserWarning, match="not valid for classification"):
        result = _run_decision_tree(tmp_path, inputs, {
            "max_depth": 5, "min_samples_split": 2, "criterion": "squared_error",
        })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "classification"


# ── Regression tests ──────────────────────────────────────────────


def test_regression_basic(tmp_path: Path):
    """Should train a regressor and produce regression metrics."""
    inputs = _make_regression_data(tmp_path)
    result = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 10, "min_samples_split": 2, "criterion": "squared_error",
    })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "regression"
    assert "mse" in metrics["train"]
    assert "mae" in metrics["train"]
    assert "r2" in metrics["train"]

    # Train R² should be decent for this simple linear relationship
    assert metrics["train"]["r2"] > 0.5


def test_regression_absolute_error_criterion(tmp_path: Path):
    """absolute_error criterion should work for regression."""
    inputs = _make_regression_data(tmp_path)
    result = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 10, "min_samples_split": 2, "criterion": "absolute_error",
    })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "regression"


def test_regression_auto_corrects_classification_criterion(tmp_path: Path):
    """If user selects gini for regression, it should auto-correct to squared_error."""
    inputs = _make_regression_data(tmp_path)
    with pytest.warns(UserWarning, match="not valid for regression"):
        result = _run_decision_tree(tmp_path, inputs, {
            "max_depth": 10, "min_samples_split": 2, "criterion": "gini",
        })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["task_type"] == "regression"


# ── Auto-detection tests ──────────────────────────────────────────


def test_autodetect_binary_semantic_type(tmp_path: Path):
    """binary semantic_type in metadata → classification."""
    from ml_toolbox.nodes.training import _detect_task_type

    meta = {"columns": {"y": {"semantic_type": "binary"}}}
    df = pl.DataFrame({"y": [0, 1, 0, 1]})
    assert _detect_task_type(meta, "y", df) == "classification"


def test_autodetect_categorical_semantic_type(tmp_path: Path):
    """categorical semantic_type → classification."""
    from ml_toolbox.nodes.training import _detect_task_type

    meta = {"columns": {"y": {"semantic_type": "categorical"}}}
    df = pl.DataFrame({"y": ["a", "b", "c"]})
    assert _detect_task_type(meta, "y", df) == "classification"


def test_autodetect_continuous_semantic_type(tmp_path: Path):
    """continuous semantic_type → regression."""
    from ml_toolbox.nodes.training import _detect_task_type

    meta = {"columns": {"y": {"semantic_type": "continuous"}}}
    df = pl.DataFrame({"y": [1.0, 2.5, 3.7]})
    assert _detect_task_type(meta, "y", df) == "regression"


def test_autodetect_float_without_metadata(tmp_path: Path):
    """Float target without metadata → regression."""
    from ml_toolbox.nodes.training import _detect_task_type

    meta: dict = {}
    df = pl.DataFrame({"y": [1.1, 2.2, 3.3]})
    assert _detect_task_type(meta, "y", df) == "regression"


def test_autodetect_int_few_unique_without_metadata(tmp_path: Path):
    """Integer target with few unique values → classification."""
    from ml_toolbox.nodes.training import _detect_task_type

    meta: dict = {}
    df = pl.DataFrame({"y": [0, 1, 2, 0, 1, 2]})
    assert _detect_task_type(meta, "y", df) == "classification"


# ── Edge cases ────────────────────────────────────────────────────


def test_missing_target_raises(tmp_path: Path):
    """Should raise ValueError when target column is missing."""
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    train_path = tmp_path / "train.parquet"
    df.write_parquet(train_path)
    # No .meta.json → empty target
    with pytest.raises(ValueError, match="Target column"):
        _run_decision_tree(tmp_path, {"train": str(train_path)}, {
            "max_depth": 5, "min_samples_split": 2, "criterion": "gini",
        })


def test_val_only_no_test(tmp_path: Path):
    """Should work with train + val only (no test)."""
    inputs = _make_regression_data(tmp_path)
    result = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 10, "min_samples_split": 2, "criterion": "squared_error",
    })

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "train" in metrics
    assert "val" in metrics
    assert "test" not in metrics


def test_model_is_raw_object(tmp_path: Path):
    """Model output should be a raw sklearn object (runner handles serialization)."""
    inputs = _make_classification_data(tmp_path)
    result = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 5, "min_samples_split": 2, "criterion": "gini",
    })

    # Model should be the raw object, not a path string
    from sklearn.tree import DecisionTreeClassifier
    assert isinstance(result["model"], DecisionTreeClassifier)


def test_max_depth_affects_complexity(tmp_path: Path):
    """Deeper trees should have more leaves (higher complexity)."""
    inputs = _make_classification_data(tmp_path, n_train=200)

    result_shallow = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 2, "min_samples_split": 2, "criterion": "gini",
    })
    result_deep = _run_decision_tree(tmp_path, inputs, {
        "max_depth": 20, "min_samples_split": 2, "criterion": "gini",
    })

    shallow_leaves = result_shallow["model"].get_n_leaves()
    deep_leaves = result_deep["model"].get_n_leaves()
    assert deep_leaves >= shallow_leaves
