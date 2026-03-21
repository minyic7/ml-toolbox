"""Tests for the Correlation Matrix EDA node."""

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


def test_correlation_matrix_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.eda.correlation_matrix"]
    assert meta["label"] == "Correlation Matrix"
    assert meta["category"] == "Eda"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"method", "target_column"}


def test_known_correlation(tmp_path: Path):
    """Verify correlation value for perfectly correlated columns."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [2, 4, 6, 8, 10]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(inputs={"df": input_path}, params={"method": "pearson", "target_column": ""})

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "correlation_matrix"
    assert report["method"] == "pearson"
    assert report["summary"]["numeric_columns"] == 2
    assert report["summary"]["total_pairs"] == 1

    # Perfect positive correlation
    assert report["top_pairs"][0]["r"] == 1.0
    assert report["top_pairs"][0]["a"] == "a"
    assert report["top_pairs"][0]["b"] == "b"

    # Matrix should be 2x2 with 1s on diagonal
    assert report["matrix"]["columns"] == ["a", "b"]
    assert report["matrix"]["values"][0][0] == 1.0
    assert report["matrix"]["values"][1][1] == 1.0
    assert report["matrix"]["values"][0][1] == 1.0


def test_pearson_vs_spearman(tmp_path: Path):
    """Pearson and Spearman should differ for non-linear monotonic data."""
    from ml_toolbox.nodes.eda import correlation_matrix

    # Exponential relationship: monotonic but not linear
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [1, 4, 9, 16, 25]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result_p = correlation_matrix(inputs={"df": input_path}, params={"method": "pearson", "target_column": ""})
    report_p = json.loads(Path(result_p["report"]).read_text())

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result_s = correlation_matrix(inputs={"df": input_path}, params={"method": "spearman", "target_column": ""})
    report_s = json.loads(Path(result_s["report"]).read_text())

    # Spearman should be 1.0 (perfect monotonic), Pearson < 1.0
    assert report_s["top_pairs"][0]["r"] == 1.0
    assert report_p["top_pairs"][0]["r"] < 1.0
    assert report_p["method"] == "pearson"
    assert report_s["method"] == "spearman"


def test_target_column(tmp_path: Path):
    """Target correlations should be present and sorted by |r|."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": [5, 4, 3, 2, 1],
        "target": [2, 4, 6, 8, 10],
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(
            inputs={"df": input_path},
            params={"method": "pearson", "target_column": "target"},
        )

    report = json.loads(Path(result["report"]).read_text())
    assert "target_correlations" in report
    tc = report["target_correlations"]
    assert len(tc) == 2  # a and b, not target itself
    # Both should have |r| = 1.0 (a perfectly positive, b perfectly negative)
    assert abs(tc[0]["r"]) == 1.0
    assert abs(tc[1]["r"]) == 1.0
    # Sorted by absolute value descending
    assert abs(tc[0]["r"]) >= abs(tc[1]["r"])


def test_high_correlation_warnings(tmp_path: Path):
    """Pairs with |r| > 0.8 should produce warnings."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": [1.1, 2.05, 3.02, 3.98, 5.01],  # nearly perfect correlation
        "c": [5, 3, 1, 4, 2],  # uncorrelated
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(inputs={"df": input_path}, params={"method": "pearson", "target_column": ""})

    report = json.loads(Path(result["report"]).read_text())
    high_warnings = [w for w in report["warnings"] if w["type"] == "high_correlation"]
    assert len(high_warnings) >= 1
    # a-b pair should be flagged
    flagged_cols = {tuple(sorted(w["columns"])) for w in high_warnings}
    assert ("a", "b") in flagged_cols
    assert report["summary"]["high_correlation_pairs"] >= 1


def test_fewer_than_two_numeric_columns(tmp_path: Path):
    """Edge case: fewer than 2 numeric columns should return empty matrix."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [90, 85]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(inputs={"df": input_path}, params={"method": "pearson", "target_column": ""})

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["numeric_columns"] == 1
    assert report["summary"]["total_pairs"] == 0
    assert report["top_pairs"] == []
    assert any(w["type"] == "insufficient_columns" for w in report["warnings"])


def test_no_numeric_columns(tmp_path: Path):
    """Edge case: zero numeric columns."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({"name": ["Alice", "Bob"], "city": ["NYC", "LA"]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(inputs={"df": input_path}, params={"method": "pearson", "target_column": ""})

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["numeric_columns"] == 0
    assert report["matrix"]["values"] == []


def test_both_method(tmp_path: Path):
    """method='both' should produce matrix_pearson and matrix_spearman."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [2, 4, 6, 8, 10]})
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(inputs={"df": input_path}, params={"method": "both", "target_column": ""})

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "both"
    assert "matrix_pearson" in report
    assert "matrix_spearman" in report
    assert "matrix" not in report  # single matrix key should not be present
    assert report["matrix_pearson"]["columns"] == ["a", "b"]
    assert report["matrix_spearman"]["columns"] == ["a", "b"]


def test_top_pairs_sorted_by_abs_r(tmp_path: Path):
    """top_pairs should be sorted by abs_r descending."""
    from ml_toolbox.nodes.eda import correlation_matrix

    df = pd.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": [5, 4, 3, 2, 1],       # r = -1.0 with a
        "c": [1, 1.5, 2, 2.5, 3],   # moderate positive with a
    })
    input_path = _make_parquet(tmp_path, df)
    output_file = tmp_path / "report.json"

    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = correlation_matrix(inputs={"df": input_path}, params={"method": "pearson", "target_column": ""})

    report = json.loads(Path(result["report"]).read_text())
    abs_values = [p["abs_r"] for p in report["top_pairs"]]
    assert abs_values == sorted(abs_values, reverse=True)
