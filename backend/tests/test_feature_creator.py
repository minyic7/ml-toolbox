"""Tests for the Feature Creator transform node."""

import json
import math
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — triggers @node registration


# ── Registry metadata ─────────────────────────────────────────────


def test_feature_creator_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.feature_creator.feature_creator"]
    assert meta["label"] == "Feature Creator"
    assert meta["category"] == "Transform"
    assert meta["type"] == "ml_toolbox.nodes.feature_creator.feature_creator"
    assert meta["inputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    assert meta["outputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert "operations" in param_names
    assert meta["guide"] != ""


# ── Helpers ───────────────────────────────────────────────────────


def _make_parquet(path: Path, columns: dict | None = None) -> Path:
    """Create a parquet file with default or custom columns."""
    if columns is None:
        columns = {
            "feature_a": [10.0, 20.0, 30.0, 40.0],
            "feature_b": [2.0, 4.0, 5.0, 8.0],
            "age": [25, 30, 35, 40],
            "target": [0, 1, 0, 1],
        }
    df = pl.DataFrame(columns)
    df.write_parquet(path)
    return path


def _make_meta(path: Path, target: str = "target", columns: dict | None = None) -> Path:
    """Write a .meta.json sidecar alongside a parquet file."""
    if columns is None:
        columns = {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "age": {"dtype": "int64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        }
    meta = {"columns": columns, "target": target}
    meta_path = path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes splits to separate files."""
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"
    return mock_output


# ── log1p tests ───────────────────────────────────────────────────


def test_log1p_basic(tmp_path: Path):
    """log1p creates a new column with ln(1 + x)."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "log1p:feature_a"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_log1p" in df.columns
    assert "feature_a" in df.columns  # original preserved
    # Verify values: ln(1 + 10) ≈ 2.3979
    assert abs(df["feature_a_log1p"][0] - math.log(11.0)) < 1e-6


def test_log1p_negative_values_warning(tmp_path: Path):
    """log1p warns on negative values and produces NaN for val < -1."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path, {"col": [-2.0, -1.0, 0.0, 1.0], "target": [0, 1, 0, 1]})
    _make_meta(train_path, columns={
        "col": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.warns(UserWarning, match="negative value"):
            result = feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "log1p:col"},
            )

    df = pl.read_parquet(result["train"])
    # -2 → log(-1) = NaN, -1 → log(0) = -inf (treated as NaN by float)
    assert df["col_log1p"][0] is None or (isinstance(df["col_log1p"][0], float) and math.isnan(df["col_log1p"][0]))
    # 0 → log(1) = 0
    assert abs(df["col_log1p"][2] - 0.0) < 1e-6
    # 1 → log(2) ≈ 0.693
    assert abs(df["col_log1p"][3] - math.log(2.0)) < 1e-6


# ── ratio tests ───────────────────────────────────────────────────


def test_ratio_basic(tmp_path: Path):
    """ratio creates col_a / col_b."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "ratio:feature_a:feature_b"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_div_feature_b" in df.columns
    # 10/2 = 5.0
    assert abs(df["feature_a_div_feature_b"][0] - 5.0) < 1e-6


def test_ratio_division_by_zero(tmp_path: Path):
    """Division by zero produces NaN + warning."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path, {
        "a": [10.0, 20.0],
        "b": [0.0, 5.0],
        "target": [0, 1],
    })
    _make_meta(train_path, columns={
        "a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.warns(UserWarning, match="zero value"):
            result = feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "ratio:a:b"},
            )

    df = pl.read_parquet(result["train"])
    # 10/0 → NaN
    assert df["a_div_b"][0] is None
    # 20/5 = 4.0
    assert abs(df["a_div_b"][1] - 4.0) < 1e-6


# ── poly tests ────────────────────────────────────────────────────


def test_poly_basic(tmp_path: Path):
    """poly creates col^n."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "poly:age:2"},
        )

    df = pl.read_parquet(result["train"])
    assert "age_pow2" in df.columns
    # 25^2 = 625
    assert abs(df["age_pow2"][0] - 625.0) < 1e-6
    # 40^2 = 1600
    assert abs(df["age_pow2"][3] - 1600.0) < 1e-6


def test_poly_cubic(tmp_path: Path):
    """poly:COL:3 creates col^3."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path, {"x": [2.0, 3.0], "target": [0, 1]})
    _make_meta(train_path, columns={
        "x": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "poly:x:3"},
        )

    df = pl.read_parquet(result["train"])
    assert "x_pow3" in df.columns
    assert abs(df["x_pow3"][0] - 8.0) < 1e-6
    assert abs(df["x_pow3"][1] - 27.0) < 1e-6


# ── interaction tests ─────────────────────────────────────────────


def test_interaction_basic(tmp_path: Path):
    """interaction creates col_a * col_b."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "interaction:feature_a:feature_b"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_x_feature_b" in df.columns
    # 10 * 2 = 20
    assert abs(df["feature_a_x_feature_b"][0] - 20.0) < 1e-6


# ── date decomposition tests ─────────────────────────────────────


def test_date_decomposition(tmp_path: Path):
    """date creates year/month/day/weekday/hour columns."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15, 14, 30), datetime(2024, 1, 1, 0, 0)],
        "target": [0, 1],
    })
    df.write_parquet(train_path)
    _make_meta(train_path, columns={
        "ts": {"dtype": "datetime", "semantic_type": "datetime", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "date:ts"},
        )

    df = pl.read_parquet(result["train"])
    assert "ts_year" in df.columns
    assert "ts_month" in df.columns
    assert "ts_day" in df.columns
    assert "ts_weekday" in df.columns
    assert "ts_hour" in df.columns

    assert df["ts_year"][0] == 2023
    assert df["ts_month"][0] == 6
    assert df["ts_day"][0] == 15
    assert df["ts_hour"][0] == 14

    assert df["ts_year"][1] == 2024
    assert df["ts_month"][1] == 1


def test_date_string_column(tmp_path: Path):
    """date parses string columns as datetime."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "date_str": ["2023-06-15", "2024-01-01"],
        "target": [0, 1],
    })
    df.write_parquet(train_path)
    _make_meta(train_path, columns={
        "date_str": {"dtype": "str", "semantic_type": "datetime", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "date:date_str"},
        )

    df = pl.read_parquet(result["train"])
    assert df["date_str_year"][0] == 2023
    assert df["date_str_month"][1] == 1


# ── Three-way split tests ────────────────────────────────────────


def test_three_way_split(tmp_path: Path):
    """Operations applied identically to train/val/test."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    for split in ("train", "val", "test"):
        path = tmp_path / f"{split}.parquet"
        _make_parquet(path)
    _make_meta(tmp_path / "train.parquet")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={
                "train": str(tmp_path / "train.parquet"),
                "val": str(tmp_path / "val.parquet"),
                "test": str(tmp_path / "test.parquet"),
            },
            params={"operations": "log1p:feature_a, poly:age:2"},
        )

    for split in ("train", "val", "test"):
        assert split in result
        df = pl.read_parquet(result[split])
        assert "feature_a_log1p" in df.columns
        assert "age_pow2" in df.columns
        # Original columns preserved
        assert "feature_a" in df.columns
        assert "age" in df.columns
        assert df.height == 4


def test_train_only_no_val_test(tmp_path: Path):
    """Only train output when val/test not provided."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "poly:age:2"},
        )

    assert "train" in result
    assert "val" not in result
    assert "test" not in result


# ── .meta.json tests ─────────────────────────────────────────────


def test_meta_json_updated_with_new_columns(tmp_path: Path):
    """.meta.json includes new column entries after feature creation."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "log1p:feature_a, ratio:feature_a:feature_b, poly:age:2"},
        )

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())

    # Original columns preserved
    assert "feature_a" in meta["columns"]
    assert "feature_b" in meta["columns"]
    assert "age" in meta["columns"]
    assert "target" in meta["columns"]
    assert meta["target"] == "target"

    # New columns added
    assert "feature_a_log1p" in meta["columns"]
    assert meta["columns"]["feature_a_log1p"]["dtype"] == "Float64"
    assert meta["columns"]["feature_a_log1p"]["role"] == "feature"

    assert "feature_a_div_feature_b" in meta["columns"]
    assert meta["columns"]["feature_a_div_feature_b"]["dtype"] == "Float64"

    assert "age_pow2" in meta["columns"]
    assert meta["columns"]["age_pow2"]["dtype"] == "Float64"

    assert meta["generated_by"] == "feature_creator"


def test_meta_json_three_way(tmp_path: Path):
    """.meta.json written for all three splits."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    for split in ("train", "val", "test"):
        path = tmp_path / f"{split}.parquet"
        _make_parquet(path)
    _make_meta(tmp_path / "train.parquet")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={
                "train": str(tmp_path / "train.parquet"),
                "val": str(tmp_path / "val.parquet"),
                "test": str(tmp_path / "test.parquet"),
            },
            params={"operations": "interaction:feature_a:feature_b"},
        )

    for split in ("train", "val", "test"):
        meta_path = Path(result[split]).with_suffix(".meta.json")
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert "feature_a_x_feature_b" in meta["columns"]


# ── Error handling tests ──────────────────────────────────────────


def test_empty_operations_error(tmp_path: Path):
    """Empty operations param raises an error."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="operations is empty"):
            feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": ""},
            )


def test_column_not_found_error(tmp_path: Path):
    """Error when a referenced column does not exist."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "log1p:nonexistent"},
            )


def test_unknown_operation_error(tmp_path: Path):
    """Error for unknown operation type."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Unknown operation type"):
            feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "sqrt:feature_a"},
            )


def test_poly_power_must_be_at_least_2(tmp_path: Path):
    """poly power < 2 is rejected."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="poly power must be >= 2"):
            feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "poly:age:1"},
            )


def test_malformed_ratio_error(tmp_path: Path):
    """Malformed ratio expression raises an error."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="ratio requires two column names"):
            feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "ratio:feature_a"},
            )


def test_date_on_numeric_column_error(tmp_path: Path):
    """date on a numeric column raises an error."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="not a date/datetime type"):
            feature_creator(
                inputs={"train": str(train_path)},
                params={"operations": "date:feature_a"},
            )


# ── Multiple operations in one call ──────────────────────────────


def test_multiple_operations(tmp_path: Path):
    """Multiple operations in a single call all produce output."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={
                "operations": "log1p:feature_a, ratio:feature_a:feature_b, poly:age:2, interaction:feature_a:feature_b"
            },
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_log1p" in df.columns
    assert "feature_a_div_feature_b" in df.columns
    assert "age_pow2" in df.columns
    assert "feature_a_x_feature_b" in df.columns
    # All original columns preserved
    assert "feature_a" in df.columns
    assert "feature_b" in df.columns
    assert "age" in df.columns
    assert "target" in df.columns
    # Row count unchanged
    assert df.height == 4


def test_row_count_preserved(tmp_path: Path):
    """Row count stays the same after feature creation."""
    from ml_toolbox.nodes.feature_creator import feature_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    original_df = pl.read_parquet(train_path)

    with patch(
        "ml_toolbox.nodes.feature_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = feature_creator(
            inputs={"train": str(train_path)},
            params={"operations": "log1p:feature_a, poly:age:3"},
        )

    out_df = pl.read_parquet(result["train"])
    assert out_df.height == original_df.height
