"""Tests for the Parquet reader node."""

from pathlib import Path
from unittest.mock import patch

import polars as pl

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_parquet_reader_registered():
    """parquet_reader should be in the registry with correct metadata."""
    meta = NODE_REGISTRY["ml_toolbox.nodes.ingest.parquet_reader"]
    assert meta["label"] == "Parquet Reader"
    assert meta["category"] == "Ingest"
    assert meta["inputs"] == []
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    assert len(meta["params"]) == 2
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"path", "columns"}


def test_parquet_reader_reads_file(tmp_path: Path):
    """Reading a parquet file should produce the correct DataFrame shape."""
    from ml_toolbox.nodes.ingest import parquet_reader

    # Create a sample parquet file
    src = tmp_path / "input.parquet"
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df.write_parquet(src)

    output_file = tmp_path / "df.parquet"
    with patch(
        "ml_toolbox.nodes.ingest._get_output_path", return_value=output_file
    ):
        result = parquet_reader(inputs={}, params={"path": str(src), "columns": ""})

    out_df = pl.read_parquet(result["df"])
    assert out_df.shape == (3, 3)
    assert set(out_df.columns) == {"a", "b", "c"}


def test_parquet_reader_column_selection(tmp_path: Path):
    """Specifying columns should only load those columns."""
    from ml_toolbox.nodes.ingest import parquet_reader

    src = tmp_path / "input.parquet"
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df.write_parquet(src)

    output_file = tmp_path / "df.parquet"
    with patch(
        "ml_toolbox.nodes.ingest._get_output_path", return_value=output_file
    ):
        result = parquet_reader(
            inputs={}, params={"path": str(src), "columns": "a, c"}
        )

    out_df = pl.read_parquet(result["df"])
    assert out_df.shape == (3, 2)
    assert set(out_df.columns) == {"a", "c"}
