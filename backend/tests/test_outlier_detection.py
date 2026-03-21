"""Tests for the Outlier Detection EDA node."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_outlier_detection_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.eda.outlier_detection"]
    assert meta["label"] == "Outlier Detection"
    assert meta["category"] == "Eda"
    assert meta["type"] == "ml_toolbox.nodes.eda.outlier_detection"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    assert len(meta["params"]) == 3
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"method", "iqr_multiplier", "zscore_threshold"}
    assert meta["guide"] != ""


def _make_input_parquet(tmp_path: Path, values: dict) -> Path:
    """Create an input parquet file from a dict of columns."""
    df = pd.DataFrame(values)
    input_path = tmp_path / "input.parquet"
    df.to_parquet(input_path)
    return input_path


def test_iqr_method_detects_known_outliers(tmp_path: Path):
    """IQR method should flag values beyond the fences."""
    from ml_toolbox.nodes.eda import outlier_detection

    # Normal data with clear outliers
    normal = list(range(1, 101))  # 1-100
    outliers = [500, 600, 700]
    values = normal + outliers
    input_path = _make_input_parquet(tmp_path, {"value": values, "name": ["a"] * len(values)})

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "iqr", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )

    report = json.loads(Path(result["report"]).read_text())
    assert report["report_type"] == "outlier_detection"
    assert report["method"] == "iqr"
    assert report["summary"]["total_rows"] == 103
    assert report["summary"]["numeric_columns"] == 1  # only "value" is numeric

    # The outliers (500, 600, 700) should be detected
    value_col = next(c for c in report["columns"] if c["name"] == "value")
    assert value_col["outlier_count"] >= 3
    assert "q1" in value_col
    assert "q3" in value_col
    assert "iqr" in value_col
    assert "lower_fence" in value_col
    assert "upper_fence" in value_col
    assert 500 in value_col["outlier_values_sample"] or 700 in value_col["outlier_values_sample"]


def test_zscore_method(tmp_path: Path):
    """Z-score method should detect outliers and include mean/std/z_max."""
    from ml_toolbox.nodes.eda import outlier_detection

    normal = list(range(1, 101))
    outliers = [1000]
    values = normal + outliers
    input_path = _make_input_parquet(tmp_path, {"value": values})

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "zscore", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "zscore"
    value_col = next(c for c in report["columns"] if c["name"] == "value")
    assert value_col["outlier_count"] >= 1
    assert "mean" in value_col
    assert "std" in value_col
    assert "z_max" in value_col
    # IQR fields should NOT be present for zscore-only
    assert "q1" not in value_col


def test_no_outliers(tmp_path: Path):
    """Uniform data with no outliers should produce zero counts."""
    from ml_toolbox.nodes.eda import outlier_detection

    # Tight range, no outliers possible
    values = list(range(50, 60)) * 10  # 100 rows, values 50-59
    input_path = _make_input_parquet(tmp_path, {"value": values})

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "iqr", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )

    report = json.loads(Path(result["report"]).read_text())
    assert report["summary"]["columns_with_outliers"] == 0
    assert report["summary"]["total_outlier_cells"] == 0
    value_col = next(c for c in report["columns"] if c["name"] == "value")
    assert value_col["outlier_count"] == 0
    assert value_col["outlier_values_sample"] == []


def test_iqr_multiplier_affects_detection(tmp_path: Path):
    """Higher IQR multiplier should detect fewer outliers."""
    from ml_toolbox.nodes.eda import outlier_detection

    normal = list(range(1, 101))
    mild_outliers = [200, 250]  # mild
    extreme_outliers = [1000]  # extreme
    values = normal + mild_outliers + extreme_outliers
    input_path = _make_input_parquet(tmp_path, {"value": values})

    # With standard multiplier (1.5)
    output_file1 = tmp_path / "report1.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file1):
        result1 = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "iqr", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )
    report1 = json.loads(Path(result1["report"]).read_text())

    # With extreme multiplier (3.0)
    output_file2 = tmp_path / "report2.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file2):
        result2 = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "iqr", "iqr_multiplier": 3.0, "zscore_threshold": 3.0},
        )
    report2 = json.loads(Path(result2["report"]).read_text())

    col1 = next(c for c in report1["columns"] if c["name"] == "value")
    col2 = next(c for c in report2["columns"] if c["name"] == "value")
    assert col1["outlier_count"] >= col2["outlier_count"]


def test_outlier_values_sample_limited_to_5(tmp_path: Path):
    """outlier_values_sample should contain at most 5 values."""
    from ml_toolbox.nodes.eda import outlier_detection

    normal = list(range(1, 101))
    outliers = [500, 600, 700, 800, 900, 1000, 1100, 1200]  # 8 outliers
    values = normal + outliers
    input_path = _make_input_parquet(tmp_path, {"value": values})

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "iqr", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )

    report = json.loads(Path(result["report"]).read_text())
    value_col = next(c for c in report["columns"] if c["name"] == "value")
    assert len(value_col["outlier_values_sample"]) <= 5
    assert value_col["outlier_count"] >= 8


def test_both_method(tmp_path: Path):
    """Method 'both' should include both IQR and z-score fields."""
    from ml_toolbox.nodes.eda import outlier_detection

    normal = list(range(1, 101))
    outliers = [500]
    values = normal + outliers
    input_path = _make_input_parquet(tmp_path, {"value": values})

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "both", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )

    report = json.loads(Path(result["report"]).read_text())
    assert report["method"] == "both"
    value_col = next(c for c in report["columns"] if c["name"] == "value")
    # Should have both IQR and z-score fields
    assert "q1" in value_col
    assert "q3" in value_col
    assert "iqr" in value_col
    assert "mean" in value_col
    assert "std" in value_col
    assert "z_max" in value_col


def test_columns_sorted_by_outlier_pct(tmp_path: Path):
    """Columns should be sorted by outlier_pct descending."""
    from ml_toolbox.nodes.eda import outlier_detection

    n = 100
    input_path = _make_input_parquet(tmp_path, {
        "clean": list(range(n)),  # no outliers
        "some_outliers": list(range(n - 2)) + [10000, 20000],  # 2 outliers
        "many_outliers": list(range(n - 10)) + [10000 + i * 1000 for i in range(10)],  # 10 outliers
    })

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = outlier_detection(
            inputs={"df": str(input_path)},
            params={"method": "iqr", "iqr_multiplier": 1.5, "zscore_threshold": 3.0},
        )

    report = json.loads(Path(result["report"]).read_text())
    pcts = [c["outlier_pct"] for c in report["columns"]]
    assert pcts == sorted(pcts, reverse=True)
