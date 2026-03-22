"""Tests for the ROC & PR Curves evaluation node."""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import average_precision_score, roc_auc_score

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def _make_parquet(tmp_path: Path, df: pd.DataFrame) -> str:
    p = tmp_path / "predictions.parquet"
    df.to_parquet(p, index=False)
    return str(p)


def _run_node(tmp_path: Path, df: pd.DataFrame) -> dict:
    from ml_toolbox.nodes.evaluation import roc_pr_curves

    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.evaluation._get_output_path", return_value=output_file):
        result = roc_pr_curves(inputs={"predictions": input_path}, params={})

    return json.loads(Path(result["report"]).read_text())


# ---------- Metadata ----------


def test_node_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.evaluation.roc_pr_curves"]
    assert meta["label"] == "ROC & PR Curves"
    assert meta["category"] == "Evaluation"
    assert meta["inputs"] == [{"name": "predictions", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    assert meta["params"] == []


# ---------- Binary classification ----------


def test_perfect_binary_classifier(tmp_path: Path):
    """Perfect predictions should give AUC-ROC = 1.0 and AP = 1.0."""
    df = pd.DataFrame({
        "y_true": [0, 0, 0, 1, 1, 1],
        "y_prob_0": [0.9, 0.8, 0.7, 0.1, 0.2, 0.3],
        "y_prob_1": [0.1, 0.2, 0.3, 0.9, 0.8, 0.7],
    })
    report = _run_node(tmp_path, df)

    assert report["report_type"] == "roc_pr_curves"
    assert report["task"] == "binary"
    assert report["positive_class"] == 1
    assert report["summary"]["roc_auc"] == 1.0
    assert report["summary"]["average_precision"] == 1.0
    assert report["summary"]["n_samples"] == 6


def test_random_binary_classifier(tmp_path: Path):
    """Random predictions should give AUC-ROC ≈ 0.5."""
    np.random.seed(42)
    n = 500
    y_true = np.array([0] * (n // 2) + [1] * (n // 2))
    y_prob = np.random.rand(n)
    df = pd.DataFrame({
        "y_true": y_true,
        "y_prob_0": 1 - y_prob,
        "y_prob_1": y_prob,
    })
    report = _run_node(tmp_path, df)

    assert 0.35 < report["summary"]["roc_auc"] < 0.65


def test_auc_matches_sklearn(tmp_path: Path):
    """AUC values should match sklearn's computation."""
    np.random.seed(123)
    n = 200
    y_true = np.random.randint(0, 2, size=n)
    y_prob = np.clip(y_true + np.random.normal(0, 0.5, n), 0, 1)
    df = pd.DataFrame({
        "y_true": y_true,
        "y_prob_0": 1 - y_prob,
        "y_prob_1": y_prob,
    })
    report = _run_node(tmp_path, df)

    expected_auc = roc_auc_score(y_true, y_prob)
    expected_ap = average_precision_score(y_true, y_prob)

    assert abs(report["summary"]["roc_auc"] - round(expected_auc, 4)) < 1e-3
    assert abs(report["summary"]["average_precision"] - round(expected_ap, 4)) < 1e-3


def test_curve_data_present(tmp_path: Path):
    """ROC and PR curve data points should be present and valid."""
    df = pd.DataFrame({
        "y_true": [0, 0, 1, 1, 0, 1],
        "y_prob_0": [0.8, 0.6, 0.3, 0.2, 0.7, 0.4],
        "y_prob_1": [0.2, 0.4, 0.7, 0.8, 0.3, 0.6],
    })
    report = _run_node(tmp_path, df)

    # ROC curve
    roc = report["roc_curve"]
    assert len(roc["fpr"]) == len(roc["tpr"])
    assert len(roc["fpr"]) >= 2
    assert roc["fpr"][0] == 0.0  # starts at (0,0)
    assert roc["fpr"][-1] == 1.0  # ends at (1,1)
    assert all(0 <= v <= 1 for v in roc["fpr"])
    assert all(0 <= v <= 1 for v in roc["tpr"])

    # PR curve
    pr = report["pr_curve"]
    assert len(pr["recall"]) == len(pr["precision"])
    assert len(pr["recall"]) >= 2
    assert all(0 <= v <= 1 for v in pr["recall"])
    assert all(0 <= v <= 1 for v in pr["precision"])


def test_prevalence_reported(tmp_path: Path):
    """Prevalence of the positive class should be reported."""
    df = pd.DataFrame({
        "y_true": [0, 0, 0, 0, 1],
        "y_prob_0": [0.9, 0.8, 0.7, 0.6, 0.1],
        "y_prob_1": [0.1, 0.2, 0.3, 0.4, 0.9],
    })
    report = _run_node(tmp_path, df)

    assert report["summary"]["prevalence"] == 0.2


# ---------- Error handling ----------


def test_no_prob_columns_fails(tmp_path: Path):
    """Should fail clearly when no y_prob columns are present."""
    df = pd.DataFrame({
        "y_true": [0, 1, 0, 1],
        "y_pred": [0, 1, 0, 0],
    })
    with pytest.raises(ValueError, match="No probability columns found"):
        _run_node(tmp_path, df)


def test_no_ground_truth_fails(tmp_path: Path):
    """Should fail clearly when no ground-truth column is present."""
    df = pd.DataFrame({
        "predictions": [0, 1, 0, 1],
        "y_prob_0": [0.8, 0.2, 0.7, 0.3],
        "y_prob_1": [0.2, 0.8, 0.3, 0.7],
    })
    with pytest.raises(ValueError, match="Missing ground-truth column"):
        _run_node(tmp_path, df)


def test_single_class_fails(tmp_path: Path):
    """Should fail when only one class is present."""
    df = pd.DataFrame({
        "y_true": [1, 1, 1, 1],
        "y_prob_0": [0.2, 0.3, 0.1, 0.4],
        "y_prob_1": [0.8, 0.7, 0.9, 0.6],
    })
    with pytest.raises(ValueError, match="Need at least 2 classes"):
        _run_node(tmp_path, df)


# ---------- Alternative column names ----------


def test_target_column_name(tmp_path: Path):
    """Should accept 'target' as ground-truth column."""
    df = pd.DataFrame({
        "target": [0, 0, 1, 1],
        "y_prob_0": [0.8, 0.6, 0.3, 0.2],
        "y_prob_1": [0.2, 0.4, 0.7, 0.8],
    })
    report = _run_node(tmp_path, df)
    assert report["report_type"] == "roc_pr_curves"


# ---------- Warnings ----------


def test_low_auc_warning(tmp_path: Path):
    """Low AUC-ROC should generate a warning."""
    # Near-random predictions
    df = pd.DataFrame({
        "y_true": [0, 1, 0, 1, 0, 1, 0, 1],
        "y_prob_0": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
        "y_prob_1": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
    })
    report = _run_node(tmp_path, df)
    warning_types = [w["type"] for w in report["warnings"]]
    assert "low_roc_auc" in warning_types or "medium_roc_auc" in warning_types


def test_imbalance_warning(tmp_path: Path):
    """Highly imbalanced data should warn to focus on PR curve."""
    # 5% positive rate
    n = 100
    y_true = np.array([0] * 95 + [1] * 5)
    y_prob = np.clip(y_true * 0.6 + np.random.RandomState(42).rand(n) * 0.4, 0, 1)
    df = pd.DataFrame({
        "y_true": y_true,
        "y_prob_0": 1 - y_prob,
        "y_prob_1": y_prob,
    })
    report = _run_node(tmp_path, df)
    warning_types = [w["type"] for w in report["warnings"]]
    assert "high_imbalance" in warning_types


# ---------- Multi-class ----------


def test_multiclass_ovr(tmp_path: Path):
    """Multi-class should compute per-class OvR curves."""
    np.random.seed(42)
    n = 90
    y_true = np.array([0] * 30 + [1] * 30 + [2] * 30)
    df = pd.DataFrame({
        "y_true": y_true,
        "y_prob_0": np.clip(np.where(y_true == 0, 0.7, 0.15) + np.random.normal(0, 0.1, n), 0, 1),
        "y_prob_1": np.clip(np.where(y_true == 1, 0.7, 0.15) + np.random.normal(0, 0.1, n), 0, 1),
        "y_prob_2": np.clip(np.where(y_true == 2, 0.7, 0.15) + np.random.normal(0, 0.1, n), 0, 1),
    })
    report = _run_node(tmp_path, df)

    assert report["report_type"] == "roc_pr_curves"
    assert report["task"] == "multiclass"
    assert report["summary"]["n_classes"] == 3
    assert report["summary"]["n_samples"] == 90
    assert len(report["per_class"]) == 3

    for cls_report in report["per_class"]:
        assert "roc_auc" in cls_report
        assert "average_precision" in cls_report
        assert "roc_curve" in cls_report
        assert "pr_curve" in cls_report
        assert cls_report["roc_auc"] > 0.5  # better than random


# ---------- Downsampling ----------


def test_large_dataset_downsampled(tmp_path: Path):
    """Curve points should be downsampled for large datasets."""
    np.random.seed(42)
    n = 5000
    y_true = np.random.randint(0, 2, size=n)
    y_prob = np.clip(y_true + np.random.normal(0, 0.5, n), 0, 1)
    df = pd.DataFrame({
        "y_true": y_true,
        "y_prob_0": 1 - y_prob,
        "y_prob_1": y_prob,
    })
    report = _run_node(tmp_path, df)

    assert len(report["roc_curve"]["fpr"]) <= 200
    assert len(report["pr_curve"]["recall"]) <= 200
