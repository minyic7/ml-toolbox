"""Tests for the Logistic Regression training node."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_logistic_regression_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.logistic_regression.logistic_regression"]
    assert meta["label"] == "Logistic Regression"
    assert meta["category"] == "Training"
    assert meta["type"] == "ml_toolbox.nodes.logistic_regression.logistic_regression"
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
    assert param_names == {"C", "max_iter", "solver", "penalty", "multi_class"}
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_classification_data(
    tmp_path: Path,
    n_train: int = 80,
    n_val: int = 10,
    n_test: int = 10,
    n_classes: int = 2,
):
    """Create train/val/test parquet files with numeric features and a target column."""
    import numpy as np

    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pd.DataFrame:
        return pd.DataFrame({
            "feature_a": rng.randn(n),
            "feature_b": rng.randn(n),
            "feature_c": rng.randn(n),
            "target": rng.randint(0, n_classes, size=n),
        })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"

    _make_df(n_train).to_parquet(train_path, index=False)
    _make_df(n_val).to_parquet(val_path, index=False)
    _make_df(n_test).to_parquet(test_path, index=False)

    return train_path, val_path, test_path


def _make_meta(tmp_path: Path, target: str = "target"):
    """Write a .meta.json sidecar alongside train.parquet."""
    meta = {
        "columns": {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_c": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": target,
        "row_count": 80,
        "generated_by": "test",
    }
    meta_path = tmp_path / "train.meta.json"
    meta_path.write_text(json.dumps(meta))
    return meta_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes outputs to tmp_path."""
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _run_logistic(
    tmp_path,
    params=None,
    n_train=80,
    n_val=10,
    n_test=10,
    n_classes=2,
    write_meta=True,
    include_val=True,
    include_test=True,
):
    """Helper to set up data, meta, and run the logistic regression node."""
    from ml_toolbox.nodes.logistic_regression import logistic_regression

    train_path, val_path, test_path = _make_classification_data(
        tmp_path, n_train, n_val, n_test, n_classes
    )
    if write_meta:
        _make_meta(tmp_path)

    if params is None:
        params = {}

    inputs: dict = {"train": str(train_path)}
    if include_val:
        inputs["val"] = str(val_path)
    if include_test:
        inputs["test"] = str(test_path)

    with patch(
        "ml_toolbox.nodes.logistic_regression._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = logistic_regression(inputs=inputs, params=params)

    return result


# ── Basic training tests ─────────────────────────────────────────


def test_basic_train_produces_all_outputs(tmp_path: Path):
    """Node should produce predictions, model, and metrics outputs."""
    result = _run_logistic(tmp_path)

    assert "predictions" in result
    assert "model" in result
    assert "metrics" in result

    # predictions should be a parquet file path
    assert Path(result["predictions"]).exists()
    # model should be a raw sklearn object (auto-serialized by sandbox runner)
    from sklearn.linear_model import LogisticRegression
    assert isinstance(result["model"], LogisticRegression)
    # metrics should be a JSON file path
    assert Path(result["metrics"]).exists()


def test_predictions_schema(tmp_path: Path):
    """Predictions should have y_pred, y_prob_<class>, and split columns."""
    result = _run_logistic(tmp_path)

    pred_df = pd.read_parquet(result["predictions"])
    assert "y_pred" in pred_df.columns
    assert "y_prob_0" in pred_df.columns
    assert "y_prob_1" in pred_df.columns
    assert "split" in pred_df.columns

    # Should have rows from all three splits
    splits = set(pred_df["split"].unique())
    assert splits == {"train", "val", "test"}


def test_predictions_row_counts(tmp_path: Path):
    """Prediction row count should match sum of input split sizes."""
    result = _run_logistic(tmp_path, n_train=80, n_val=10, n_test=10)

    pred_df = pd.read_parquet(result["predictions"])
    assert len(pred_df) == 100  # 80 + 10 + 10

    train_preds = pred_df[pred_df["split"] == "train"]
    val_preds = pred_df[pred_df["split"] == "val"]
    test_preds = pred_df[pred_df["split"] == "test"]
    assert len(train_preds) == 80
    assert len(val_preds) == 10
    assert len(test_preds) == 10


def test_predictions_probabilities_sum_to_one(tmp_path: Path):
    """Probability columns should sum to ~1.0 for each row."""
    result = _run_logistic(tmp_path)

    pred_df = pd.read_parquet(result["predictions"])
    prob_cols = [c for c in pred_df.columns if c.startswith("y_prob_")]
    prob_sum = pred_df[prob_cols].sum(axis=1)
    assert all(abs(s - 1.0) < 1e-6 for s in prob_sum)


def test_metrics_structure(tmp_path: Path):
    """Metrics JSON should have report_type and per-split metrics."""
    result = _run_logistic(tmp_path)

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["report_type"] == "training_metrics"
    assert "train" in metrics
    assert "val" in metrics

    for split in ("train", "val", "test"):
        assert "accuracy" in metrics[split]
        assert "f1" in metrics[split]
        assert "precision" in metrics[split]
        assert "recall" in metrics[split]
        assert "auc" in metrics[split]

        # All metrics should be in [0, 1]
        for key in ("accuracy", "f1", "precision", "recall", "auc"):
            assert 0.0 <= metrics[split][key] <= 1.0, f"{split}.{key} out of range"


def test_model_is_fitted(tmp_path: Path):
    """Returned model should be a fitted LogisticRegression."""
    result = _run_logistic(tmp_path)

    model = result["model"]
    assert hasattr(model, "coef_")
    assert hasattr(model, "classes_")
    assert len(model.coef_[0]) == 3  # 3 features


# ── Parameter tests ──────────────────────────────────────────────


def test_custom_C_param(tmp_path: Path):
    """C parameter should be passed to the model."""
    result = _run_logistic(tmp_path, params={"C": 0.01})
    assert result["model"].C == 0.01


def test_custom_solver_param(tmp_path: Path):
    """Solver parameter should be passed to the model."""
    result = _run_logistic(tmp_path, params={"solver": "saga", "max_iter": 2000})
    assert result["model"].solver == "saga"


def test_penalty_none(tmp_path: Path):
    """penalty='none' should train without regularization."""
    result = _run_logistic(tmp_path, params={"penalty": "none"})
    assert result["model"].penalty is None


def test_l1_penalty_with_saga(tmp_path: Path):
    """L1 penalty should work with saga solver."""
    result = _run_logistic(
        tmp_path, params={"penalty": "l1", "solver": "saga", "max_iter": 2000}
    )
    assert result["model"].penalty == "l1"


# ── Edge cases ───────────────────────────────────────────────────


def test_train_only_no_val_test(tmp_path: Path):
    """Node should work with only train input (no val/test)."""
    result = _run_logistic(tmp_path, include_val=False, include_test=False)

    pred_df = pd.read_parquet(result["predictions"])
    assert set(pred_df["split"].unique()) == {"train"}

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "train" in metrics
    assert "val" not in metrics
    assert "test" not in metrics


def test_multiclass_classification(tmp_path: Path):
    """Node should handle multi-class targets (>2 classes)."""
    result = _run_logistic(tmp_path, n_classes=3)

    pred_df = pd.read_parquet(result["predictions"])
    assert "y_prob_0" in pred_df.columns
    assert "y_prob_1" in pred_df.columns
    assert "y_prob_2" in pred_df.columns

    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "train" in metrics
    assert "accuracy" in metrics["train"]


def test_no_meta_json_raises(tmp_path: Path):
    """Without .meta.json, node should raise a clear error."""
    with pytest.raises(ValueError, match="Cannot determine target column"):
        _run_logistic(tmp_path, write_meta=False)


def test_multiclass_with_saga_solver(tmp_path: Path):
    """Multi-class with saga solver should work."""
    result = _run_logistic(
        tmp_path,
        n_classes=3,
        params={"solver": "saga", "max_iter": 2000},
    )
    assert result["model"].solver == "saga"
    assert len(result["model"].classes_) == 3


def test_multi_class_param_accepted(tmp_path: Path):
    """multi_class parameter should be accepted without error."""
    result = _run_logistic(
        tmp_path,
        n_classes=3,
        params={"multi_class": "multinomial"},
    )
    # sklearn >=1.7 removed multi_class param; verify training still succeeds
    assert hasattr(result["model"], "coef_")
    assert len(result["model"].classes_) == 3


def test_multi_class_ovr_accepted(tmp_path: Path):
    """multi_class='ovr' should be accepted without error."""
    result = _run_logistic(
        tmp_path,
        n_classes=3,
        params={"multi_class": "ovr"},
    )
    assert hasattr(result["model"], "coef_")
    assert len(result["model"].classes_) == 3
