"""Tests for the Missing Analysis EDA node."""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import ml_toolbox.nodes  # noqa: F401
from ml_toolbox.protocol import NODE_REGISTRY


def test_missing_analysis_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.eda.missing_analysis"]
    assert meta["label"] == "Missing Analysis"
    assert meta["category"] == "Eda"
    assert meta["type"] == "ml_toolbox.nodes.eda.missing_analysis"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    assert meta["params"] == []


def test_mixed_missing_patterns(tmp_path: Path):
    """DataFrame with varying missing patterns across columns."""
    from ml_toolbox.nodes.eda import missing_analysis

    df = pd.DataFrame({
        "complete": range(100),
        "low_missing": [None if i == 0 else i for i in range(100)],       # 1% missing
        "medium_missing": [None if i < 10 else i for i in range(100)],    # 10% missing
        "high_missing": [None if i < 50 else i for i in range(100)],      # 50% missing
    })
    input_file = tmp_path / "input.parquet"
    df.to_parquet(input_file)

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = missing_analysis(inputs={"df": str(input_file)}, params={})

    report = json.loads(Path(result["report"]).read_text())

    # Summary checks
    assert report["report_type"] == "missing_analysis"
    assert report["summary"]["total_rows"] == 100
    assert report["summary"]["total_columns"] == 4
    assert report["summary"]["total_missing_cells"] == 61  # 1 + 10 + 50
    assert report["summary"]["total_cells"] == 400
    assert report["summary"]["no_missing_count"] == 1  # "complete" column

    # Columns sorted by missing_pct descending (only columns with missing > 0)
    cols = report["columns"]
    assert len(cols) == 3
    assert cols[0]["name"] == "high_missing"
    assert cols[1]["name"] == "medium_missing"
    assert cols[2]["name"] == "low_missing"

    # Severity classification
    assert cols[0]["severity"] == "high"
    assert cols[1]["severity"] == "medium"
    assert cols[2]["severity"] == "low"

    # present_count
    assert cols[0]["present_count"] == 50
    assert cols[1]["present_count"] == 90
    assert cols[2]["present_count"] == 99

    # Warnings for medium and high
    warning_types = {w["column"]: w["type"] for w in report["warnings"]}
    assert warning_types["high_missing"] == "critical_missing"
    assert warning_types["medium_missing"] == "high_missing"
    assert "low_missing" not in warning_types


def test_no_missing_values(tmp_path: Path):
    """DataFrame with no missing values at all."""
    from ml_toolbox.nodes.eda import missing_analysis

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    input_file = tmp_path / "input.parquet"
    df.to_parquet(input_file)

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = missing_analysis(inputs={"df": str(input_file)}, params={})

    report = json.loads(Path(result["report"]).read_text())

    assert report["columns"] == []
    assert report["warnings"] == []
    assert report["summary"]["total_missing_cells"] == 0
    assert report["summary"]["overall_missing_pct"] == 0.0
    assert report["summary"]["complete_rows"] == 3
    assert report["summary"]["complete_rows_pct"] == 1.0
    assert report["summary"]["no_missing_count"] == 2


def test_severity_thresholds(tmp_path: Path):
    """Verify exact boundary behavior for severity classification."""
    from ml_toolbox.nodes.eda import missing_analysis

    # 1000 rows: 0 missing = none, 49 = 4.9% (low), 50 = 5% (medium), 301 = 30.1% (high)
    df = pd.DataFrame({
        "exactly_5pct": [None if i < 50 else i for i in range(1000)],    # 5% -> medium
        "just_below_5pct": [None if i < 49 else i for i in range(1000)], # 4.9% -> low
        "just_above_30pct": [None if i < 301 else i for i in range(1000)],  # 30.1% -> high
        "exactly_30pct": [None if i < 300 else i for i in range(1000)],  # 30% -> medium
    })
    input_file = tmp_path / "input.parquet"
    df.to_parquet(input_file)

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = missing_analysis(inputs={"df": str(input_file)}, params={})

    report = json.loads(Path(result["report"]).read_text())
    severity_map = {c["name"]: c["severity"] for c in report["columns"]}

    assert severity_map["exactly_5pct"] == "medium"
    assert severity_map["just_below_5pct"] == "low"
    assert severity_map["just_above_30pct"] == "high"
    assert severity_map["exactly_30pct"] == "medium"


def test_complete_rows_calculation(tmp_path: Path):
    """Verify complete_rows counts only rows with zero missing values."""
    from ml_toolbox.nodes.eda import missing_analysis

    df = pd.DataFrame({
        "a": [1, None, 3, None, 5],
        "b": [None, 2, 3, None, 5],
    })
    input_file = tmp_path / "input.parquet"
    df.to_parquet(input_file)

    output_file = tmp_path / "report.json"
    with patch("ml_toolbox.nodes.eda._get_output_path", return_value=output_file):
        result = missing_analysis(inputs={"df": str(input_file)}, params={})

    report = json.loads(Path(result["report"]).read_text())

    # Rows 2 (idx=2) and 4 (idx=4) are complete
    assert report["summary"]["complete_rows"] == 2
    assert report["summary"]["complete_rows_pct"] == 0.4
