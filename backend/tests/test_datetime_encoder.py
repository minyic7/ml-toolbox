"""Tests for the DateTime Encoder node."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401 — triggers @node registration


# ── Registry metadata ─────────────────────────────────────────────


def test_datetime_encoder_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.datetime_encoder.datetime_encoder"]
    assert meta["label"] == "DateTime Encoder"
    assert meta["category"] == "Transform"
    assert meta["inputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    param_names = {p["name"] for p in meta["params"]}
    assert "column" in param_names
    assert "components" in param_names
    assert "drop_original" in param_names
    assert meta["guide"] != ""


# ── Helpers ───────────────────────────────────────────────────────


def _make_meta(path: Path, target: str = "target", columns: dict | None = None) -> Path:
    if columns is None:
        columns = {
            "ts": {"dtype": "datetime", "semantic_type": "datetime", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        }
    meta = {"columns": columns, "target": target}
    meta_path = path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta_path


def _mock_output_factory(tmp_path: Path):
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"
    return mock_output


# ── Basic tests ──────────────────────────────────────────────────


def test_datetime_decomposition(tmp_path: Path):
    """Decomposes datetime column into year/month/day/weekday."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15, 14, 30), datetime(2024, 1, 1, 0, 0)],
        "target": [0, 1],
    })
    df.write_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={"train": str(train_path)},
            params={"column": "ts", "components": "year,month,day,weekday", "drop_original": True},
        )

    df = pl.read_parquet(result["train"])
    assert "ts_year" in df.columns
    assert "ts_month" in df.columns
    assert "ts_day" in df.columns
    assert "ts_weekday" in df.columns
    assert "ts" not in df.columns  # dropped

    assert df["ts_year"][0] == 2023
    assert df["ts_month"][0] == 6
    assert df["ts_day"][0] == 15

    assert df["ts_year"][1] == 2024
    assert df["ts_month"][1] == 1


def test_datetime_keep_original(tmp_path: Path):
    """Original column preserved when drop_original=False."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15, 14, 30)],
        "target": [0],
    })
    df.write_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={"train": str(train_path)},
            params={"column": "ts", "components": "year,month", "drop_original": False},
        )

    df = pl.read_parquet(result["train"])
    assert "ts" in df.columns  # preserved
    assert "ts_year" in df.columns
    assert "ts_month" in df.columns


def test_datetime_with_hour_minute(tmp_path: Path):
    """Extract hour and minute components."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15, 14, 30)],
        "target": [0],
    })
    df.write_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={"train": str(train_path)},
            params={"column": "ts", "components": "hour,minute", "drop_original": True},
        )

    df = pl.read_parquet(result["train"])
    assert df["ts_hour"][0] == 14
    assert df["ts_minute"][0] == 30


def test_datetime_string_column(tmp_path: Path):
    """Parses string columns as datetime."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

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
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={"train": str(train_path)},
            params={"column": "date_str", "components": "year,month", "drop_original": True},
        )

    df = pl.read_parquet(result["train"])
    assert df["date_str_year"][0] == 2023
    assert df["date_str_month"][1] == 1


def test_datetime_auto_detect(tmp_path: Path):
    """Auto-detects datetime columns from metadata."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15, 14, 30)],
        "value": [100.0],
        "target": [0],
    })
    df.write_parquet(train_path)
    _make_meta(train_path, columns={
        "ts": {"dtype": "datetime", "semantic_type": "datetime", "role": "feature"},
        "value": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={"train": str(train_path)},
            params={"column": "", "components": "year,month", "drop_original": True},
        )

    df = pl.read_parquet(result["train"])
    assert "ts_year" in df.columns
    assert "ts_month" in df.columns


def test_datetime_numeric_column_error(tmp_path: Path):
    """Error when column is not datetime."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "value": [100.0, 200.0],
        "target": [0, 1],
    })
    df.write_parquet(train_path)
    _make_meta(train_path, columns={
        "value": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="not a date/datetime type"):
            datetime_encoder(
                inputs={"train": str(train_path)},
                params={"column": "value", "components": "year", "drop_original": True},
            )


def test_datetime_column_not_found(tmp_path: Path):
    """Error when referenced column does not exist."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({"target": [0, 1]})
    df.write_parquet(train_path)
    _make_meta(train_path, columns={
        "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
    })

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            datetime_encoder(
                inputs={"train": str(train_path)},
                params={"column": "nonexistent", "components": "year", "drop_original": True},
            )


def test_invalid_component_error(tmp_path: Path):
    """Error for unknown component."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15)],
        "target": [0],
    })
    df.write_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        with pytest.raises(ValueError, match="Unknown component 'second'"):
            datetime_encoder(
                inputs={"train": str(train_path)},
                params={"column": "ts", "components": "year,second", "drop_original": True},
            )


def test_three_way_split(tmp_path: Path):
    """Applied identically to train/val/test."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    for split in ("train", "val", "test"):
        path = tmp_path / f"{split}.parquet"
        df = pl.DataFrame({
            "ts": [datetime(2023, 6, 15, 14, 30)],
            "target": [0],
        })
        df.write_parquet(path)
    _make_meta(tmp_path / "train.parquet")

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={
                "train": str(tmp_path / "train.parquet"),
                "val": str(tmp_path / "val.parquet"),
                "test": str(tmp_path / "test.parquet"),
            },
            params={"column": "ts", "components": "year,month", "drop_original": True},
        )

    for split in ("train", "val", "test"):
        assert split in result
        df = pl.read_parquet(result[split])
        assert "ts_year" in df.columns
        assert df["ts_year"][0] == 2023


def test_meta_json_updated(tmp_path: Path):
    """.meta.json updated with new columns and dropped original."""
    from ml_toolbox.nodes.datetime_encoder import datetime_encoder

    train_path = tmp_path / "train.parquet"
    df = pl.DataFrame({
        "ts": [datetime(2023, 6, 15)],
        "target": [0],
    })
    df.write_parquet(train_path)
    _make_meta(train_path)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with patch(
        "ml_toolbox.nodes.datetime_encoder._get_output_path",
        side_effect=_mock_output_factory(out_dir),
    ):
        result = datetime_encoder(
            inputs={"train": str(train_path)},
            params={"column": "ts", "components": "year,month,day", "drop_original": True},
        )

    meta_path = Path(result["train"]).with_suffix(".meta.json")
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())

    assert "ts" not in meta["columns"]  # original dropped
    assert "ts_year" in meta["columns"]
    assert meta["columns"]["ts_year"]["dtype"] == "Int32"
    assert meta["columns"]["ts_year"]["semantic_type"] == "ordinal"
    assert "ts_month" in meta["columns"]
    assert "ts_day" in meta["columns"]
    assert meta["generated_by"] == "datetime_encoder"
