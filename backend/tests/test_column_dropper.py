"""Tests for the Column Dropper transform node."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — triggers @node registration


def test_column_dropper_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.transform.column_dropper"]
    assert meta["label"] == "Column Dropper"
    assert meta["category"] == "Transform"
    assert meta["type"] == "ml_toolbox.nodes.transform.column_dropper"
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
    assert "columns_to_drop" in param_names
    assert meta["guide"] != ""


# ── Helpers ──────────────────────────────────────────────────────


def _make_parquet(path: Path, columns: dict | None = None) -> Path:
    """Create a parquet file with default or custom columns."""
    if columns is None:
        columns = {
            "feature1": [1, 2, 3],
            "feature2": [4.0, 5.0, 6.0],
            "id_col": ["a", "b", "c"],
            "target": [0, 1, 0],
        }
    df = pl.DataFrame(columns)
    df.write_parquet(path)
    return path


def _make_meta(path: Path, target: str = "target", columns: dict | None = None) -> Path:
    """Write a .meta.json sidecar alongside a parquet file."""
    if columns is None:
        columns = {
            "feature1": {"name": "feature1", "dtype": "int64", "role": "feature"},
            "feature2": {"name": "feature2", "dtype": "float64", "role": "feature"},
            "id_col": {"name": "id_col", "dtype": "str", "role": "identifier"},
            "target": {"name": "target", "dtype": "int64", "role": "target"},
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


# ── Tests ────────────────────────────────────────────────────────


def test_basic_drop_train_only(tmp_path: Path):
    """Drop a single column from train split only."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = column_dropper(
            inputs={"train": str(train_path)},
            params={"columns_to_drop": "id_col"},
        )

    assert "train" in result
    out_df = pl.read_parquet(result["train"])
    assert "id_col" not in out_df.columns
    assert set(out_df.columns) == {"feature1", "feature2", "target"}
    assert out_df.height == 3

    # val and test should not be in result
    assert "val" not in result
    assert "test" not in result


def test_three_way_drop(tmp_path: Path):
    """Drop columns from train + val + test splits."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"

    _make_parquet(train_path)
    _make_meta(train_path)
    _make_parquet(val_path)
    _make_parquet(test_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = column_dropper(
            inputs={
                "train": str(train_path),
                "val": str(val_path),
                "test": str(test_path),
            },
            params={"columns_to_drop": "id_col, feature2"},
        )

    for split in ("train", "val", "test"):
        assert split in result
        df = pl.read_parquet(result[split])
        assert set(df.columns) == {"feature1", "target"}
        assert df.height == 3


def test_column_not_found_error(tmp_path: Path):
    """Error when a specified column does not exist."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Columns not found in schema"):
            column_dropper(
                inputs={"train": str(train_path)},
                params={"columns_to_drop": "nonexistent_col"},
            )


def test_target_column_protected(tmp_path: Path):
    """Target column is NOT dropped even if listed — warning is issued."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.warns(UserWarning, match="Target column 'target' cannot be dropped"):
            result = column_dropper(
                inputs={"train": str(train_path)},
                params={"columns_to_drop": "target, id_col"},
            )

    out_df = pl.read_parquet(result["train"])
    assert "target" in out_df.columns  # protected
    assert "id_col" not in out_df.columns  # dropped


def test_target_only_drop_errors(tmp_path: Path):
    """Trying to drop only the target column results in an error."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.warns(UserWarning):
            with pytest.raises(ValueError, match="No columns to drop"):
                column_dropper(
                    inputs={"train": str(train_path)},
                    params={"columns_to_drop": "target"},
                )


def test_meta_json_updated(tmp_path: Path):
    """.meta.json reflects removed columns after drop."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = column_dropper(
            inputs={"train": str(train_path)},
            params={"columns_to_drop": "id_col"},
        )

    meta_out_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_out_path.exists()
    updated_meta = json.loads(meta_out_path.read_text())
    assert "id_col" not in updated_meta["columns"]
    assert "feature1" in updated_meta["columns"]
    assert "target" in updated_meta["columns"]
    assert updated_meta["target"] == "target"


def test_meta_json_updated_three_way(tmp_path: Path):
    """.meta.json updated correctly for all three splits."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    val_path = tmp_path / "val.parquet"
    test_path = tmp_path / "test.parquet"

    _make_parquet(train_path)
    _make_meta(train_path)
    _make_parquet(val_path)
    _make_parquet(test_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = column_dropper(
            inputs={
                "train": str(train_path),
                "val": str(val_path),
                "test": str(test_path),
            },
            params={"columns_to_drop": "id_col, feature2"},
        )

    for split in ("train", "val", "test"):
        meta_path = Path(result[split]).with_suffix(".meta.json")
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert "id_col" not in meta["columns"]
        assert "feature2" not in meta["columns"]
        assert "feature1" in meta["columns"]
        assert "target" in meta["columns"]


def test_empty_columns_to_drop_errors(tmp_path: Path):
    """Empty columns_to_drop param raises an error."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="columns_to_drop is empty"):
            column_dropper(
                inputs={"train": str(train_path)},
                params={"columns_to_drop": ""},
            )


def test_multiple_columns_drop(tmp_path: Path):
    """Drop multiple columns at once."""
    from ml_toolbox.nodes.transform import column_dropper

    train_path = tmp_path / "train.parquet"
    _make_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.transform._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = column_dropper(
            inputs={"train": str(train_path)},
            params={"columns_to_drop": "id_col, feature2"},
        )

    out_df = pl.read_parquet(result["train"])
    assert set(out_df.columns) == {"feature1", "target"}
