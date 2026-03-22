"""Tests for the Regression Metrics evaluation node."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def _make_parquet(tmp_path: Path, df: pd.DataFrame) -> str:
    p = tmp_path / "predictions.parquet"
    df.to_parquet(p, index=False)
    return str(p)


def _write_meta(parquet_path: str, meta: dict) -> None:
    meta_path = Path(parquet_path).with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta))


def test_regression_metrics_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluation.regression_metrics"]
    assert meta["label"] == "Regression Metrics"
    assert meta["category"] == "Evaluation"
    assert meta["inputs"] == [{"name": "predictions", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    assert meta["params"] == []


def test_perfect_regression(tmp_path: Path):
    """Perfect predictions should give MAE=0, RMSE=0, R²=1."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "y_true": [1.0, 2.0, 3.0, 4.0, 5.0],
        "y_pred": [1.0, 2.0, 3.0, 4.0, 5.0],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "training_metrics"
    assert report["task_type"] == "regression"
    assert "all" in report["splits"]
    metrics = report["splits"]["all"]
    assert metrics["mae"] == 0.0
    assert metrics["rmse"] == 0.0
    assert metrics["r2"] == 1.0
    assert metrics["support"] == 5


def test_imperfect_regression(tmp_path: Path):
    """Imperfect predictions should give non-zero errors and R² < 1."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "y_true": [1.0, 2.0, 3.0, 4.0, 5.0],
        "y_pred": [1.5, 2.5, 2.5, 3.5, 5.5],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    metrics = report["splits"]["all"]
    assert metrics["mae"] > 0
    assert metrics["rmse"] > 0
    assert metrics["r2"] < 1.0
    assert metrics["support"] == 5


def test_per_split_regression(tmp_path: Path):
    """Metrics should be computed separately for each split."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "y_true": [1.0, 2.0, 3.0, 4.0, 10.0, 20.0],
        "y_pred": [1.0, 2.0, 3.0, 4.0, 15.0, 25.0],
        "__split__": ["train", "train", "train", "train", "val", "val"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert "train" in report["splits"]
    assert "val" in report["splits"]
    # Train split is perfect
    assert report["splits"]["train"]["mae"] == 0.0
    assert report["splits"]["train"]["r2"] == 1.0
    assert report["splits"]["train"]["support"] == 4
    # Val split has errors
    assert report["splits"]["val"]["mae"] == 5.0
    assert report["splits"]["val"]["support"] == 2
    assert report["split_order"] == ["train", "val"]


def test_split_ordering(tmp_path: Path):
    """Split order should follow train/val/test convention."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "y_true": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "y_pred": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "__split__": ["test", "test", "val", "val", "train", "train"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["split_order"] == ["train", "val", "test"]


def test_target_column_from_meta(tmp_path: Path):
    """Target column should be read from .meta.json sidecar."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "price": [100.0, 200.0, 300.0],
        "y_pred": [110.0, 190.0, 310.0],
    })
    input_path = _make_parquet(tmp_path, df)
    _write_meta(input_path, {"target": "price"})
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    metrics = report["splits"]["all"]
    assert metrics["mae"] == 10.0
    assert metrics["support"] == 3


def test_overfitting_warning(tmp_path: Path):
    """Should warn when val RMSE >> train RMSE."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "y_true": [1.0, 2.0, 3.0, 4.0, 10.0, 20.0],
        "y_pred": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0],
        "__split__": ["train", "train", "train", "train", "val", "val"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert any(w["type"] == "overfitting" for w in report["warnings"])


def test_negative_r2(tmp_path: Path):
    """Terrible predictions should produce negative R²."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({
        "y_true": [1.0, 2.0, 3.0],
        "y_pred": [100.0, -50.0, 200.0],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["splits"]["all"]["r2"] < 0


def test_metric_info_present(tmp_path: Path):
    """Report should include metric descriptions."""
    from ml_toolbox.nodes.evaluation import regression_metrics

    df = pd.DataFrame({"y_true": [1.0, 2.0], "y_pred": [1.0, 2.0]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = regression_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert "metric_info" in report
    assert "mae" in report["metric_info"]
    assert "rmse" in report["metric_info"]
    assert "r2" in report["metric_info"]
