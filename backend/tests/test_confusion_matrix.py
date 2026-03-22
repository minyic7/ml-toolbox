"""Tests for the Confusion Matrix evaluation node."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def _make_parquet(tmp_path: Path, df: pd.DataFrame) -> str:
    p = tmp_path / "input.parquet"
    df.to_parquet(p, index=False)
    return str(p)


def test_confusion_matrix_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluate.confusion_matrix"]
    assert meta["label"] == "Confusion Matrix"
    assert meta["category"] == "Evaluate"
    assert meta["inputs"] == [{"name": "predictions", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"normalize"}


def test_perfect_binary_classification(tmp_path: Path):
    """All predictions correct — diagonal should have all counts, off-diagonal zero."""
    from ml_toolbox.nodes.evaluate import confusion_matrix

    df = pd.DataFrame({
        "y_true": [0, 0, 1, 1, 1],
        "y_pred": [0, 0, 1, 1, 1],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluate._get_output_path", return_value=output_file):
        result = confusion_matrix(inputs={"predictions": input_path}, params={"normalize": False})

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "confusion_matrix"
    assert report["accuracy"] == 1.0
    assert report["class_labels"] == ["0", "1"]
    assert report["confusion_matrix"] == [[2, 0], [0, 3]]

    # All per-class metrics should be 1.0
    for pc in report["per_class"]:
        assert pc["precision"] == 1.0
        assert pc["recall"] == 1.0
        assert pc["f1"] == 1.0


def test_imperfect_classification(tmp_path: Path):
    """Some misclassifications — verify matrix values and metrics."""
    from ml_toolbox.nodes.evaluate import confusion_matrix

    # 2 class-0 correct, 1 class-0 misclassified as class-1
    # 3 class-1 correct, 0 class-1 misclassified
    df = pd.DataFrame({
        "y_true": [0, 0, 0, 1, 1, 1],
        "y_pred": [0, 0, 1, 1, 1, 1],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluate._get_output_path", return_value=output_file):
        result = confusion_matrix(inputs={"predictions": input_path}, params={"normalize": False})

    report = json.loads(Path(result["report"]).read_text())
    assert report["accuracy"] == round(5 / 6, 4)
    assert report["confusion_matrix"] == [[2, 1], [0, 3]]
    assert report["summary"]["total_samples"] == 6
    assert report["summary"]["num_classes"] == 2

    # Class 0: precision=2/(2+0)=1.0, recall=2/(2+1)=0.6667
    pc0 = report["per_class"][0]
    assert pc0["label"] == "0"
    assert pc0["precision"] == 1.0
    assert pc0["recall"] == round(2 / 3, 4)

    # Class 1: precision=3/(3+1)=0.75, recall=3/3=1.0
    pc1 = report["per_class"][1]
    assert pc1["label"] == "1"
    assert pc1["precision"] == 0.75
    assert pc1["recall"] == 1.0


def test_multiclass(tmp_path: Path):
    """Three-class classification."""
    from ml_toolbox.nodes.evaluate import confusion_matrix

    df = pd.DataFrame({
        "y_true": ["cat", "cat", "dog", "dog", "bird", "bird"],
        "y_pred": ["cat", "dog", "dog", "dog", "bird", "cat"],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluate._get_output_path", return_value=output_file):
        result = confusion_matrix(inputs={"predictions": input_path}, params={"normalize": False})

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["num_classes"] == 3
    assert sorted(report["class_labels"]) == ["bird", "cat", "dog"]
    assert report["accuracy"] == round(4 / 6, 4)

    # Matrix is 3x3 (sorted labels: bird, cat, dog)
    cm = report["confusion_matrix"]
    assert len(cm) == 3
    assert all(len(row) == 3 for row in cm)

    # Total counts should sum to 6
    total = sum(sum(row) for row in cm)
    assert total == 6


def test_normalized_matrix(tmp_path: Path):
    """Normalized matrix rows should sum to ~1.0."""
    from ml_toolbox.nodes.evaluate import confusion_matrix

    df = pd.DataFrame({
        "y_true": [0, 0, 0, 0, 1, 1],
        "y_pred": [0, 0, 0, 1, 1, 0],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluate._get_output_path", return_value=output_file):
        result = confusion_matrix(inputs={"predictions": input_path}, params={"normalize": True})

    report = json.loads(Path(result["report"]).read_text())
    assert report["normalize"] is True

    # Normalized matrix should have rows summing to 1.0
    cm_norm = report["confusion_matrix_normalized"]
    for row in cm_norm:
        assert abs(sum(row) - 1.0) < 0.01

    # Raw matrix should also be present
    cm_raw = report["confusion_matrix"]
    assert sum(sum(row) for row in cm_raw) == 6


def test_missing_columns(tmp_path: Path):
    """Missing y_true/y_pred columns should produce a warning."""
    from ml_toolbox.nodes.evaluate import confusion_matrix

    df = pd.DataFrame({"feature_a": [1, 2, 3], "feature_b": [4, 5, 6]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluate._get_output_path", return_value=output_file):
        result = confusion_matrix(inputs={"predictions": input_path}, params={"normalize": False})

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "confusion_matrix"
    assert len(report["warnings"]) > 0
    assert report["warnings"][0]["type"] == "missing_columns"
    assert report["confusion_matrix"] == []


def test_class_imbalance_warning(tmp_path: Path):
    """Large class imbalance should trigger a warning."""
    from ml_toolbox.nodes.evaluate import confusion_matrix

    df = pd.DataFrame({
        "y_true": [0] * 100 + [1] * 10,
        "y_pred": [0] * 100 + [1] * 10,
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluate._get_output_path", return_value=output_file):
        result = confusion_matrix(inputs={"predictions": input_path}, params={"normalize": False})

    report = json.loads(Path(result["report"]).read_text())
    warning_types = [w["type"] for w in report["warnings"]]
    assert "class_imbalance" in warning_types


def test_guide_present():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluate.confusion_matrix"]
    assert "Confusion Matrix" in meta["guide"]
    assert "True Positives" in meta["guide"] or "true" in meta["guide"].lower()
