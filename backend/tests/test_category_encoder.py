"""Tests for the Category Encoder transform node."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — trigger auto-registration


# ── Helpers ──────────────────────────────────────────────────────

def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that writes to tmp_path."""
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"
    return mock_output


def _make_splits(tmp_path: Path, *, with_meta: bool = True):
    """Create train/val/test parquet files with categorical columns."""
    train_df = pl.DataFrame({
        "color": ["red", "blue", "green", "red", "blue", "green", "red", "blue"],
        "size": ["S", "M", "L", "S", "M", "L", "S", "M"],
        "price": [10.0, 20.0, 30.0, 15.0, 25.0, 35.0, 12.0, 22.0],
        "target": [0, 1, 1, 0, 1, 0, 0, 1],
    })
    val_df = pl.DataFrame({
        "color": ["red", "green"],
        "size": ["M", "S"],
        "price": [18.0, 28.0],
        "target": [0, 1],
    })
    test_df = pl.DataFrame({
        "color": ["blue", "red", "green"],
        "size": ["L", "S", "M"],
        "price": [32.0, 11.0, 27.0],
        "target": [1, 0, 1],
    })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    if with_meta:
        meta = {
            "columns": {
                "color": {"dtype": "object", "semantic_type": "categorical", "role": "feature"},
                "size": {"dtype": "object", "semantic_type": "categorical", "role": "feature"},
                "price": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
                "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
            },
            "row_count": 8,
        }
        for p in (train_path, val_path, test_path):
            p.with_suffix(".meta.json").write_text(json.dumps(meta))

    return {"train": str(train_path), "val": str(val_path), "test": str(test_path)}


def _make_splits_with_unseen(tmp_path: Path):
    """Create splits where val/test contain unseen categories."""
    train_df = pl.DataFrame({
        "color": ["red", "blue", "green", "red"],
        "value": [1.0, 2.0, 3.0, 4.0],
        "target": [0, 1, 1, 0],
    })
    val_df = pl.DataFrame({
        "color": ["red", "purple"],  # "purple" is unseen
        "value": [5.0, 6.0],
        "target": [0, 1],
    })
    test_df = pl.DataFrame({
        "color": ["yellow"],  # "yellow" is unseen
        "value": [7.0],
        "target": [1],
    })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    meta = {
        "columns": {
            "color": {"dtype": "object", "semantic_type": "categorical", "role": "feature"},
            "value": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
    }
    for p in (train_path, val_path, test_path):
        p.with_suffix(".meta.json").write_text(json.dumps(meta))

    return {"train": str(train_path), "val": str(val_path), "test": str(test_path)}


# ── Registration tests ───────────────────────────────────────────

def test_category_encoder_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.transform.category_encoder"]
    assert meta["label"] == "Category Encoder"
    assert meta["category"] == "Transform"
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
    assert len(meta["params"]) == 3
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"method", "columns", "handle_unknown"}
    assert meta["guide"] != ""


# ── Label encoding tests ────────────────────────────────────────

def test_label_encoding_basic(tmp_path: Path):
    """Label encoding maps each category to a unique int."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    # color and size should be int now, price and target unchanged
    assert train_out["color"].dtype == pl.Int64
    assert train_out["size"].dtype == pl.Int64
    assert train_out["price"].dtype == pl.Float64
    # target should NOT be encoded (it's the target column)
    assert train_out["target"].dtype == pl.Int64

    # Verify mapping is consistent: same value → same int
    color_vals = train_out["color"].to_list()
    # Sorted unique original values: blue=0, green=1, red=2
    assert color_vals[0] == 2  # red → 2
    assert color_vals[1] == 0  # blue → 0
    assert color_vals[2] == 1  # green → 1

    # Val and test should also be encoded
    val_out = pl.read_parquet(result["val"])
    assert val_out["color"].dtype == pl.Int64
    test_out = pl.read_parquet(result["test"])
    assert test_out["color"].dtype == pl.Int64


def test_label_encoding_meta_updated(tmp_path: Path):
    """Label encoding updates .meta.json: dtype → int64, semantic_type → encoded."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "", "handle_unknown": "encode_as_unknown"},
        )

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())

    assert meta["columns"]["color"]["dtype"] == "int64"
    assert meta["columns"]["color"]["semantic_type"] == "encoded"
    assert "encoding" in meta["columns"]["color"]
    assert meta["columns"]["color"]["encoding"]["method"] == "label"

    # Price and target should be unchanged
    assert meta["columns"]["price"]["dtype"] == "float64"
    assert meta["columns"]["target"]["role"] == "target"


def test_label_encoding_unseen_encode_as_unknown(tmp_path: Path):
    """Unseen categories in val/test → -1 with encode_as_unknown."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits_with_unseen(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "color", "handle_unknown": "encode_as_unknown"},
        )

    val_out = pl.read_parquet(result["val"])
    val_colors = val_out["color"].to_list()
    # "red" is known (→ some int >= 0), "purple" is unseen (→ -1)
    assert val_colors[1] == -1  # purple → -1

    test_out = pl.read_parquet(result["test"])
    test_colors = test_out["color"].to_list()
    assert test_colors[0] == -1  # yellow → -1


def test_label_encoding_unseen_error(tmp_path: Path):
    """handle_unknown=error raises ValueError for unseen categories."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits_with_unseen(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        with pytest.raises(ValueError, match="Unseen category 'purple'.*val"):
            category_encoder(
                inputs=inputs,
                params={"method": "label", "columns": "color", "handle_unknown": "error"},
            )


def test_label_encoding_specific_columns(tmp_path: Path):
    """Only encode specified columns, leave others untouched."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "color", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    assert train_out["color"].dtype == pl.Int64
    # size should remain string since we only asked for color
    assert train_out["size"].dtype == pl.Utf8


# ── One-hot encoding tests ──────────────────────────────────────

def test_onehot_encoding_basic(tmp_path: Path):
    """One-hot creates N binary columns per original column."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "one_hot", "columns": "color", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])

    # Original "color" column should be removed
    assert "color" not in train_out.columns

    # 3 new binary columns: color_blue, color_green, color_red
    assert "color_blue" in train_out.columns
    assert "color_green" in train_out.columns
    assert "color_red" in train_out.columns

    # First row was "red" → color_red=1, others=0
    assert train_out["color_red"][0] == 1
    assert train_out["color_blue"][0] == 0
    assert train_out["color_green"][0] == 0

    # Other columns preserved
    assert "size" in train_out.columns
    assert "price" in train_out.columns
    assert "target" in train_out.columns


def test_onehot_encoding_meta_updated(tmp_path: Path):
    """One-hot updates .meta.json: removes original, adds binary columns."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "one_hot", "columns": "color", "handle_unknown": "encode_as_unknown"},
        )

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    meta = json.loads(meta_path.read_text())

    # Original column removed from metadata
    assert "color" not in meta["columns"]

    # New binary columns in metadata
    assert "color_blue" in meta["columns"]
    assert meta["columns"]["color_blue"]["dtype"] == "int64"
    assert meta["columns"]["color_blue"]["semantic_type"] == "binary"
    assert meta["columns"]["color_blue"]["encoding"]["method"] == "one_hot"
    assert meta["columns"]["color_blue"]["encoding"]["source_column"] == "color"


def test_onehot_unseen_all_zeros(tmp_path: Path):
    """Unseen categories in val/test → all-zero row (no new columns)."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits_with_unseen(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "one_hot", "columns": "color", "handle_unknown": "encode_as_unknown"},
        )

    val_out = pl.read_parquet(result["val"])
    # "purple" is unseen → all-zero row
    # Columns: color_blue, color_green, color_red
    purple_row = val_out.row(1)  # second row was "purple"
    # Find the one-hot columns
    onehot_cols = [c for c in val_out.columns if c.startswith("color_")]
    assert len(onehot_cols) == 3
    for col in onehot_cols:
        assert val_out[col][1] == 0  # all zeros for unseen "purple"


def test_onehot_unseen_error(tmp_path: Path):
    """handle_unknown=error raises ValueError for unseen categories in one-hot."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits_with_unseen(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        with pytest.raises(ValueError, match="Unseen category 'purple'.*val"):
            category_encoder(
                inputs=inputs,
                params={"method": "one_hot", "columns": "color", "handle_unknown": "error"},
            )


# ── Edge case tests ─────────────────────────────────────────────

def test_target_column_skipped(tmp_path: Path):
    """Target column is never encoded, even if it looks categorical."""
    from ml_toolbox.nodes.transform import category_encoder

    # Create data where target is categorical
    train_df = pl.DataFrame({
        "feature": ["a", "b", "c", "a"],
        "target": ["cat", "dog", "cat", "dog"],
    })
    val_df = pl.DataFrame({
        "feature": ["b"],
        "target": ["cat"],
    })
    test_df = pl.DataFrame({
        "feature": ["c"],
        "target": ["dog"],
    })

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    meta = {
        "columns": {
            "feature": {"dtype": "object", "semantic_type": "categorical", "role": "feature"},
            "target": {"dtype": "object", "semantic_type": "categorical", "role": "target"},
        },
    }
    for p in (train_path, val_path, test_path):
        p.with_suffix(".meta.json").write_text(json.dumps(meta))

    inputs = {"train": str(train_path), "val": str(val_path), "test": str(test_path)}
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    assert train_out["feature"].dtype == pl.Int64  # encoded
    assert train_out["target"].dtype == pl.Utf8  # NOT encoded — still string


def test_empty_val_test(tmp_path: Path):
    """Empty val/test DataFrames are handled gracefully."""
    from ml_toolbox.nodes.transform import category_encoder

    train_df = pl.DataFrame({
        "color": ["red", "blue", "green"],
        "value": [1.0, 2.0, 3.0],
    })
    val_df = pl.DataFrame(schema={"color": pl.Utf8, "value": pl.Float64})
    test_df = pl.DataFrame(schema={"color": pl.Utf8, "value": pl.Float64})

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    inputs = {"train": str(train_path), "val": str(val_path), "test": str(test_path)}
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "color", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    assert train_out["color"].dtype == pl.Int64
    val_out = pl.read_parquet(result["val"])
    assert val_out.height == 0
    test_out = pl.read_parquet(result["test"])
    assert test_out.height == 0


def test_no_categorical_columns_passthrough(tmp_path: Path):
    """If no categorical columns found, data passes through unchanged."""
    from ml_toolbox.nodes.transform import category_encoder

    train_df = pl.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
    val_df = pl.DataFrame({"a": [4], "b": [7.0]})
    test_df = pl.DataFrame({"a": [5], "b": [8.0]})

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    inputs = {"train": str(train_path), "val": str(val_path), "test": str(test_path)}
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    assert train_out.columns == ["a", "b"]
    assert train_out["a"].to_list() == [1, 2, 3]


def test_onehot_multiple_columns(tmp_path: Path):
    """One-hot encoding multiple columns creates correct column expansion."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "one_hot", "columns": "color, size", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    # Original columns removed
    assert "color" not in train_out.columns
    assert "size" not in train_out.columns
    # color: 3 categories, size: 3 categories → 6 new columns
    onehot_cols = [c for c in train_out.columns if c.startswith("color_") or c.startswith("size_")]
    assert len(onehot_cols) == 6
    # Non-encoded columns preserved
    assert "price" in train_out.columns
    assert "target" in train_out.columns


def test_row_counts_preserved(tmp_path: Path):
    """Encoding doesn't change row counts in any split."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "", "handle_unknown": "encode_as_unknown"},
        )

    assert pl.read_parquet(result["train"]).height == 8
    assert pl.read_parquet(result["val"]).height == 2
    assert pl.read_parquet(result["test"]).height == 3


def test_without_metadata_falls_back_to_dtype(tmp_path: Path):
    """Without .meta.json, uses string dtype columns as categorical."""
    from ml_toolbox.nodes.transform import category_encoder

    inputs = _make_splits(tmp_path, with_meta=False)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch("ml_toolbox.nodes.transform._get_output_path",
               side_effect=_mock_output_factory(out_dir)):
        result = category_encoder(
            inputs=inputs,
            params={"method": "label", "columns": "", "handle_unknown": "encode_as_unknown"},
        )

    train_out = pl.read_parquet(result["train"])
    # color and size are Utf8 → should be encoded
    assert train_out["color"].dtype == pl.Int64
    assert train_out["size"].dtype == pl.Int64
    # price is Float64 → untouched
    assert train_out["price"].dtype == pl.Float64
