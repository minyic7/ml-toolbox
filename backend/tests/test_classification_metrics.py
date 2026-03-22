"""Tests for the Classification Metrics evaluation node."""

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


def test_classification_metrics_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluation.classification_metrics"]
    assert meta["label"] == "Classification Metrics"
    assert meta["category"] == "Evaluation"
    assert meta["inputs"] == [{"name": "predictions", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    assert meta["params"] == []


def test_binary_classification(tmp_path: Path):
    """Basic binary classification with perfect predictions."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": [0, 0, 1, 1, 1],
        "y_pred": [0, 0, 1, 1, 1],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "training_metrics"
    assert report["task_type"] == "classification"
    assert "all" in report["splits"]
    metrics = report["splits"]["all"]
    assert metrics["accuracy"] == 1.0
    assert metrics["f1_macro"] == 1.0
    assert metrics["precision_macro"] == 1.0
    assert metrics["recall_macro"] == 1.0
    assert metrics["support"] == 5


def test_imperfect_classification(tmp_path: Path):
    """Classification with some errors should produce metrics < 1.0."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": [0, 0, 1, 1, 1, 0, 1, 0],
        "y_pred": [0, 1, 1, 0, 1, 0, 1, 1],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    metrics = report["splits"]["all"]
    assert 0 < metrics["accuracy"] < 1.0
    assert 0 < metrics["f1_macro"] < 1.0
    assert metrics["support"] == 8


def test_per_split_metrics(tmp_path: Path):
    """Metrics should be computed separately for each split."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": [0, 1, 0, 1, 0, 1],
        "y_pred": [0, 1, 0, 1, 1, 0],
        "__split__": ["train", "train", "train", "train", "val", "val"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert "train" in report["splits"]
    assert "val" in report["splits"]
    assert report["splits"]["train"]["accuracy"] == 1.0
    assert report["splits"]["train"]["support"] == 4
    assert report["splits"]["val"]["accuracy"] == 0.0
    assert report["splits"]["val"]["support"] == 2
    assert report["split_order"] == ["train", "val"]


def test_split_ordering(tmp_path: Path):
    """Split order should follow train/val/test convention."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": [0, 1, 0, 1, 0, 1],
        "y_pred": [0, 1, 0, 1, 0, 1],
        "__split__": ["test", "test", "val", "val", "train", "train"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert report["split_order"] == ["train", "val", "test"]


def test_auc_with_probabilities(tmp_path: Path):
    """AUC should be computed when y_prob columns are present."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": [0, 0, 1, 1, 1],
        "y_pred": [0, 0, 1, 1, 1],
        "y_prob_0": [0.9, 0.8, 0.2, 0.1, 0.05],
        "y_prob_1": [0.1, 0.2, 0.8, 0.9, 0.95],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    metrics = report["splits"]["all"]
    assert "auc" in metrics
    assert metrics["auc"] == 1.0


def test_target_column_from_meta(tmp_path: Path):
    """Target column should be read from .meta.json sidecar."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "species": ["cat", "dog", "cat", "dog"],
        "y_pred": ["cat", "dog", "dog", "dog"],
    })
    input_path = _make_parquet(tmp_path, df)
    _write_meta(input_path, {"target": "species"})
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    metrics = report["splits"]["all"]
    assert metrics["accuracy"] == 0.75
    assert metrics["support"] == 4


def test_overfitting_warning(tmp_path: Path):
    """Should warn when train accuracy >> val accuracy."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": [0, 1, 0, 1, 0, 1, 0, 1],
        "y_pred": [0, 1, 0, 1, 1, 0, 1, 0],
        "__split__": ["train", "train", "train", "train", "val", "val", "val", "val"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert any(w["type"] == "overfitting" for w in report["warnings"])


def test_multiclass_classification(tmp_path: Path):
    """Should handle multiclass classification."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({
        "y_true": ["a", "b", "c", "a", "b", "c"],
        "y_pred": ["a", "b", "c", "a", "c", "b"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    metrics = report["splits"]["all"]
    assert 0 < metrics["accuracy"] < 1.0
    assert metrics["support"] == 6


def test_metric_info_present(tmp_path: Path):
    """Report should include metric descriptions."""
    from ml_toolbox.nodes.evaluation import classification_metrics

    df = pd.DataFrame({"y_true": [0, 1], "y_pred": [0, 1]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = classification_metrics(inputs={"predictions": input_path}, params={})

    report = json.loads(Path(result["report"]).read_text())
    assert "metric_info" in report
    assert "accuracy" in report["metric_info"]
    assert "f1_macro" in report["metric_info"]
