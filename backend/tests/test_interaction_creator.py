"""Tests for the Interaction Creator node."""

import json
import math
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — triggers @node registration


# ── Registry metadata ─────────────────────────────────────────────


def test_interaction_creator_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.interaction_creator.interaction_creator"]
    assert meta["label"] == "Interaction Creator"
    assert meta["category"] == "Transform"
    assert meta["inputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert "pairs" in param_names
    assert "operation" in param_names
    assert meta["guide"] != ""


# ── Helpers ───────────────────────────────────────────────────────


def _make_parquet(path: Path, columns: dict | None = None) -> Path:
    if columns is None:
        columns = {
            "feature_a": [10.0, 20.0, 30.0, 40.0],
            "feature_b": [2.0, 4.0, 5.0, 8.0],
            "target": [0, 1, 0, 1],
        }
    df = pl.DataFrame(columns)
    df.write_parquet(path)
    return path


def _make_meta(path: Path, target: str = "target", columns: dict | None = None) -> Path:
    if columns is None:
        columns = {
            "feature_a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "feature_b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        }
    meta = {"columns": columns, "target": target}
    meta_path = path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta_path


def _make_eda_context(path: Path, context: dict) -> Path:
    ctx_path = path.with_suffix(".eda-context.json")
    ctx_path.write_text(json.dumps(context, indent=2))
    return ctx_path


def _mock_output_factory(tmp_path: Path):
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"
    return mock_output


# ── Multiply tests ───────────────────────────────────────────────


def test_multiply_basic(tmp_path: Path):
    """Multiply creates col_a * col_b."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "feature_a:feature_b", "operation": "multiply"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_x_feature_b" in df.columns
    assert abs(df["feature_a_x_feature_b"][0] - 20.0) < 1e-6  # 10*2
    # Originals preserved
    assert "feature_a" in df.columns
    assert "feature_b" in df.columns


def test_ratio_basic(tmp_path: Path):
    """Ratio creates col_a / col_b."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "feature_a:feature_b", "operation": "ratio"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_div_feature_b" in df.columns
    assert abs(df["feature_a_div_feature_b"][0] - 5.0) < 1e-6  # 10/2


def test_ratio_division_by_zero(tmp_path: Path):
    """Division by zero produces NaN + warning."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

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
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.warns(UserWarning, match="zero value"):
            result = interaction_creator(
                inputs={"train": str(train_path)},
                params={"pairs": "a:b", "operation": "ratio"},
            )

    df = pl.read_parquet(result["train"])
    assert df["a_div_b"][0] is None  # NaN from division by zero
    assert abs(df["a_div_b"][1] - 4.0) < 1e-6


def test_add_and_subtract(tmp_path: Path):
    """Add and subtract operations work correctly."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "feature_a:feature_b", "operation": "add"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_plus_feature_b" in df.columns
    assert abs(df["feature_a_plus_feature_b"][0] - 12.0) < 1e-6  # 10+2

    # Now test subtract
    out_dir2 = tmp_path / "out2"
    out_dir2.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir2),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "feature_a:feature_b", "operation": "subtract"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_minus_feature_b" in df.columns
    assert abs(df["feature_a_minus_feature_b"][0] - 8.0) < 1e-6  # 10-2


def test_auto_select_from_eda(tmp_path: Path):
    """Auto-selects high-correlation pairs from EDA context."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)
    _make_eda_context(train_path, {
        "correlation": {
            "high_pairs": [["feature_a", "feature_b", 0.95]],
        },
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "", "operation": "multiply"},
        )

    df = pl.read_parquet(result["train"])
    assert "feature_a_x_feature_b" in df.columns


def test_no_pairs_no_eda_error(tmp_path: Path):
    """Error when no pairs provided and no EDA context."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="No pairs specified"):
            interaction_creator(
                inputs={"train": str(train_path)},
                params={"pairs": "", "operation": "multiply"},
            )


def test_column_not_found(tmp_path: Path):
    """Error when a referenced column does not exist."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            interaction_creator(
                inputs={"train": str(train_path)},
                params={"pairs": "nonexistent:feature_b", "operation": "multiply"},
            )


def test_invalid_pair_format(tmp_path: Path):
    """Error for malformed pair expression."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Invalid pair format"):
            interaction_creator(
                inputs={"train": str(train_path)},
                params={"pairs": "feature_a", "operation": "multiply"},
            )


def test_three_way_split(tmp_path: Path):
    """Applied identically to train/val/test."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    for split in ("train", "val", "test"):
        _make_parquet(tmp_path / f"{split}.parquet")
    _make_meta(tmp_path / "train.parquet")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={
                "train": str(tmp_path / "train.parquet"),
                "val": str(tmp_path / "val.parquet"),
                "test": str(tmp_path / "test.parquet"),
            },
            params={"pairs": "feature_a:feature_b", "operation": "multiply"},
        )

    for split in ("train", "val", "test"):
        assert split in result
        df = pl.read_parquet(result[split])
        assert "feature_a_x_feature_b" in df.columns


def test_meta_json_updated(tmp_path: Path):
    """.meta.json includes new column entries."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "feature_a:feature_b", "operation": "multiply"},
        )

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert "feature_a_x_feature_b" in meta["columns"]
    assert meta["columns"]["feature_a_x_feature_b"]["dtype"] == "Float64"
    assert meta["columns"]["feature_a_x_feature_b"]["role"] == "feature"
    assert meta["generated_by"] == "interaction_creator"


def test_multiple_pairs(tmp_path: Path):
    """Multiple pairs in a single call."""
    from ml_toolbox.nodes.interaction_creator import interaction_creator

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path, {
        "a": [1.0, 2.0],
        "b": [3.0, 4.0],
        "c": [5.0, 6.0],
        "target": [0, 1],
    })
    _make_meta(train_path, columns={
        "a": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "b": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "c": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.interaction_creator._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = interaction_creator(
            inputs={"train": str(train_path)},
            params={"pairs": "a:b, b:c", "operation": "multiply"},
        )

    df = pl.read_parquet(result["train"])
    assert "a_x_b" in df.columns
    assert "b_x_c" in df.columns
    assert abs(df["a_x_b"][0] - 3.0) < 1e-6  # 1*3
    assert abs(df["b_x_c"][0] - 15.0) < 1e-6  # 3*5
