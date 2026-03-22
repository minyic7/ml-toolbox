"""Tests for the Linear Regression training node."""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


# ── Registry / metadata ─────────────────────────────────────────


def test_linear_regression_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.linear_regression.linear_regression"]
    assert meta["label"] == "Linear Regression"
    assert meta["category"] == "Training"
    assert meta["type"] == "ml_toolbox.nodes.linear_regression.linear_regression"
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
    assert param_names == {"fit_intercept", "normalize"}
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_regression_data(
    tmp_path: Path,
    n_train: int = 80,
    n_val: int = 20,
    n_test: int = 20,
) -> tuple[Path, Path, Path]:
    """Create train/val/test parquet files for regression (y = 3*x1 + 2*x2 + noise)."""
    rng = np.random.RandomState(42)

    def _make_df(n: int) -> pd.DataFrame:
        x1 = rng.randn(n)
        x2 = rng.randn(n)
        target = 3.0 * x1 + 2.0 * x2 + rng.randn(n) * 0.1
        return pd.DataFrame({
            "feature_a": x1,
            "feature_b": x2,
            "target": target,
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
            "target": {"dtype": "float64", "semantic_type": "continuous", "role": "target"},
        },
        "target": target,
        "row_count": 80,
        "generated_by": "test",
    }
    meta_path = tmp_path / "train.meta.json"
    meta_path.write_text(json.dumps(meta))
    return meta_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes outputs to tmp dir."""
    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return out_dir / f"{name}{ext}"

    return mock_output


def _run_linear_regression(
    tmp_path: Path,
    params: dict | None = None,
    include_val: bool = True,
    include_test: bool = True,
) -> dict:
    """Helper to set up data, meta, and run the node."""
    from ml_toolbox.nodes.linear_regression import linear_regression

    train_path, val_path, test_path = _make_regression_data(tmp_path)
    _make_meta(tmp_path)

    inputs: dict[str, str] = {"train": str(train_path)}
    if include_val:
        inputs["val"] = str(val_path)
    if include_test:
        inputs["test"] = str(test_path)

    with patch(
        "ml_toolbox.nodes.linear_regression._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        return linear_regression(inputs=inputs, params=params or {})


# ── Basic regression tests ───────────────────────────────────────


def test_regression_basic(tmp_path: Path):
    """Should produce model, predictions, and metrics with correct structure."""
    result = _run_linear_regression(tmp_path)

    assert "predictions" in result
    assert "model" in result
    assert "metrics" in result

    # Check predictions file
    pred_df = pd.read_parquet(result["predictions"])
    assert "split" in pred_df.columns
    assert "prediction" in pred_df.columns
    assert pred_df.shape[0] > 0

    # Check metrics file
    metrics = json.loads(Path(result["metrics"]).read_text())
    assert metrics["report_type"] == "linear_regression"
    assert metrics["task"] == "regression"
    assert "train_metrics" in metrics
    assert "mae" in metrics["train_metrics"]
    assert "rmse" in metrics["train_metrics"]
    assert "r2" in metrics["train_metrics"]
    assert "coefficients" in metrics


def test_regression_metrics_correct(tmp_path: Path):
    """MAE, RMSE, R² should reflect good fit on a clean linear signal."""
    result = _run_linear_regression(tmp_path)
    metrics = json.loads(Path(result["metrics"]).read_text())

    train_m = metrics["train_metrics"]
    # Data is y = 3*x1 + 2*x2 + small noise → R² should be very high
    assert train_m["r2"] > 0.99
    assert train_m["rmse"] < 0.5
    assert train_m["mae"] < 0.5


def test_coefficients_close_to_true(tmp_path: Path):
    """Learned coefficients should approximate the true weights (3, 2)."""
    result = _run_linear_regression(tmp_path)
    metrics = json.loads(Path(result["metrics"]).read_text())

    coef_map = {c["feature"]: c["coefficient"] for c in metrics["coefficients"]}
    assert abs(coef_map["feature_a"] - 3.0) < 0.2
    assert abs(coef_map["feature_b"] - 2.0) < 0.2

    # Intercept should be close to 0
    assert metrics["intercept"] is not None
    assert abs(metrics["intercept"]) < 0.5


def test_coefficients_sorted_by_absolute_value(tmp_path: Path):
    """Coefficients should be sorted by |coefficient| descending."""
    result = _run_linear_regression(tmp_path)
    metrics = json.loads(Path(result["metrics"]).read_text())

    coefficients = metrics["coefficients"]
    abs_values = [abs(c["coefficient"]) for c in coefficients]
    assert abs_values == sorted(abs_values, reverse=True)


# ── Split handling tests ─────────────────────────────────────────


def test_predictions_contain_all_splits(tmp_path: Path):
    """Predictions should include rows from all connected splits."""
    result = _run_linear_regression(tmp_path, include_val=True, include_test=True)

    pred_df = pd.read_parquet(result["predictions"])
    splits = set(pred_df["split"].unique())
    assert splits == {"train", "val", "test"}

    # Check expected row counts
    assert pred_df[pred_df["split"] == "train"].shape[0] == 80
    assert pred_df[pred_df["split"] == "val"].shape[0] == 20
    assert pred_df[pred_df["split"] == "test"].shape[0] == 20


def test_val_metrics_present_when_connected(tmp_path: Path):
    """When val is connected, metrics should include val scores."""
    result = _run_linear_regression(tmp_path, include_val=True)
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert "val_metrics" in metrics
    assert "mae" in metrics["val_metrics"]
    assert "rmse" in metrics["val_metrics"]
    assert "r2" in metrics["val_metrics"]

    # Summary should prefer val metrics
    assert metrics["summary"] == metrics["val_metrics"]


def test_test_metrics_present_when_connected(tmp_path: Path):
    """When test is connected, metrics should include test scores."""
    result = _run_linear_regression(tmp_path, include_test=True)
    metrics = json.loads(Path(result["metrics"]).read_text())
    assert "test_metrics" in metrics
    assert "r2" in metrics["test_metrics"]


def test_train_only(tmp_path: Path):
    """Without val/test, only train metrics and predictions should exist."""
    result = _run_linear_regression(tmp_path, include_val=False, include_test=False)
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert "train_metrics" in metrics
    assert "val_metrics" not in metrics
    assert "test_metrics" not in metrics

    # Summary should use train metrics
    assert metrics["summary"] == metrics["train_metrics"]

    # Predictions should only contain train split
    pred_df = pd.read_parquet(result["predictions"])
    assert set(pred_df["split"].unique()) == {"train"}


# ── Parameter tests ──────────────────────────────────────────────


def test_fit_intercept_false(tmp_path: Path):
    """fit_intercept=false should set intercept to None in metrics."""
    result = _run_linear_regression(tmp_path, params={"fit_intercept": False})
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert metrics["params"]["fit_intercept"] is False
    assert metrics["intercept"] is None


def test_normalize_true(tmp_path: Path):
    """normalize=true should still produce valid results."""
    result = _run_linear_regression(tmp_path, params={"normalize": True})
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert metrics["params"]["normalize"] is True
    # Should still fit well
    assert metrics["train_metrics"]["r2"] > 0.99


def test_params_stored_in_metrics(tmp_path: Path):
    """Param values should be recorded in the metrics output."""
    result = _run_linear_regression(
        tmp_path,
        params={"fit_intercept": True, "normalize": False},
    )
    metrics = json.loads(Path(result["metrics"]).read_text())

    assert metrics["params"]["fit_intercept"] is True
    assert metrics["params"]["normalize"] is False


# ── Error handling tests ─────────────────────────────────────────


def test_missing_meta_json_raises(tmp_path: Path):
    """Should raise ValueError when no .meta.json exists."""
    from ml_toolbox.nodes.linear_regression import linear_regression

    train_df = pd.DataFrame({
        "feature_a": [1.0, 2.0, 3.0],
        "feature_b": [4.0, 5.0, 6.0],
        "target": [10.0, 20.0, 30.0],
    })
    train_path = tmp_path / "train.parquet"
    train_df.to_parquet(train_path, index=False)

    inputs = {"train": str(train_path)}

    with patch(
        "ml_toolbox.nodes.linear_regression._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        with pytest.raises(ValueError, match="Cannot determine target"):
            linear_regression(inputs=inputs, params={})


def test_model_is_sklearn_linear_regression(tmp_path: Path):
    """Returned model should be an sklearn LinearRegression instance."""
    from sklearn.linear_model import LinearRegression

    result = _run_linear_regression(tmp_path)
    assert isinstance(result["model"], LinearRegression)
