"""Tests for the Distribution Profile EDA node."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ml_toolbox.protocol.decorators import NODE_REGISTRY


# ── helpers ──────────────────────────────────────────────────────────


def _make_input_parquet(tmp_path: Path, n_rows: int = 200) -> Path:
    """Create a mixed numeric + categorical DataFrame as parquet."""
    import numpy as np

    rng = np.random.default_rng(42)
    df = pd.DataFrame(
        {
            "age": rng.normal(40, 15, n_rows).round(1),
            "income": rng.exponential(50000, n_rows).round(2),  # skewed
            "gender": rng.choice(["M", "F", "Other"], n_rows, p=[0.5, 0.4, 0.1]),
            "city": [f"city_{i % 30}" for i in range(n_rows)],  # high cardinality
            "target": rng.choice([0, 1], n_rows, p=[0.8, 0.2]),  # imbalanced
        }
    )
    path = tmp_path / "input.parquet"
    df.to_parquet(path, index=False)
    return path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that writes to tmp_path."""

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"

    return mock_output


# ── metadata tests ───────────────────────────────────────────────────


def test_distribution_profile_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.eda.distribution_profile"]
    assert meta["label"] == "Distribution Profile"
    assert meta["category"] == "Eda"
    assert meta["type"] == "ml_toolbox.nodes.eda.distribution_profile"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "report", "type": "METRICS"}]
    assert len(meta["params"]) == 1
    assert meta["params"][0]["name"] == "target_column"
    assert meta["guide"] != ""


# ── execution tests ──────────────────────────────────────────────────


def test_mixed_numeric_and_categorical(tmp_path: Path):
    """Profile a DataFrame with both numeric and categorical columns."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": ""},
        )

    report = json.loads(Path(result["report"]).read_text())

    assert report["report_type"] == "distribution_profile"
    assert report["summary"]["total_rows"] == 200
    assert report["summary"]["total_columns"] == 5
    assert report["summary"]["numeric_count"] == 3  # age, income, target
    assert report["summary"]["categorical_count"] == 2  # gender, city

    col_names = [c["name"] for c in report["columns"]]
    assert "age" in col_names
    assert "gender" in col_names

    # All columns should have role "feature" when no target specified
    for col in report["columns"]:
        assert col["role"] == "feature"

    # No target section when target_column is empty
    assert "target" not in report


def test_with_target_column(tmp_path: Path):
    """Target column should produce a separate target section."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": "target"},
        )

    report = json.loads(Path(result["report"]).read_text())

    assert "target" in report
    assert report["target"]["name"] == "target"
    assert "class_balance" in report["target"]
    assert len(report["target"]["class_balance"]) == 2

    # Target column in columns array should have role "target"
    target_col = next(c for c in report["columns"] if c["name"] == "target")
    assert target_col["role"] == "target"


def test_without_target_column(tmp_path: Path):
    """Empty target_column should skip the target section."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": ""},
        )

    report = json.loads(Path(result["report"]).read_text())
    assert "target" not in report


def test_histogram_structure(tmp_path: Path):
    """Numeric columns should have histogram with bin_edges and counts."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": ""},
        )

    report = json.loads(Path(result["report"]).read_text())

    age_col = next(c for c in report["columns"] if c["name"] == "age")
    assert "histogram" in age_col
    hist = age_col["histogram"]
    assert "bin_edges" in hist
    assert "counts" in hist
    # 10 bins → 11 edges, 10 counts
    assert len(hist["bin_edges"]) == 11
    assert len(hist["counts"]) == 10
    assert sum(hist["counts"]) == 200


def test_warnings_generated(tmp_path: Path):
    """Skewed columns and high-cardinality categoricals should produce warnings."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": "target"},
        )

    report = json.loads(Path(result["report"]).read_text())

    warning_types = {w["type"] for w in report["warnings"]}
    # income is exponentially distributed → skewed
    assert "skewed" in warning_types
    # city has 30 unique values → high cardinality
    assert "high_cardinality" in warning_types
    # target is 80/20 → class imbalance (ratio 4:1)
    assert "class_imbalance" in warning_types

    # Each warning should have column, type, message
    for w in report["warnings"]:
        assert "column" in w
        assert "type" in w
        assert "message" in w


def test_numeric_stats_fields(tmp_path: Path):
    """Numeric columns should have all expected stat fields."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": ""},
        )

    report = json.loads(Path(result["report"]).read_text())

    age_col = next(c for c in report["columns"] if c["name"] == "age")
    expected_keys = {
        "count", "mean", "median", "std", "min", "max",
        "skewness", "kurtosis", "q25", "q50", "q75",
    }
    assert set(age_col["stats"].keys()) == expected_keys


def test_categorical_stats_fields(tmp_path: Path):
    """Categorical columns should have cardinality and top_values."""
    from ml_toolbox.nodes.eda import distribution_profile

    input_path = _make_input_parquet(tmp_path)

    with patch(
        "ml_toolbox.nodes.eda._get_output_path",
        side_effect=_mock_output_factory(tmp_path),
    ):
        result = distribution_profile(
            inputs={"df": str(input_path)},
            params={"target_column": ""},
        )

    report = json.loads(Path(result["report"]).read_text())

    gender_col = next(c for c in report["columns"] if c["name"] == "gender")
    assert "cardinality" in gender_col["stats"]
    assert "top_values" in gender_col["stats"]
    assert gender_col["stats"]["cardinality"] == 3

    for tv in gender_col["stats"]["top_values"]:
        assert "value" in tv
        assert "count" in tv
        assert "pct" in tv
