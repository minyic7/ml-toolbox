"""Tests for the CSV reader ingest node."""

from pathlib import Path
from unittest.mock import patch

import polars as pl

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_csv_reader_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.ingest.csv_reader"]
    assert meta["label"] == "CSV Reader"
    assert meta["category"] == "Ingest"
    assert meta["type"] == "ml_toolbox.nodes.ingest.csv_reader"
    assert meta["inputs"] == []
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    assert len(meta["params"]) == 3
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"path", "separator", "header"}


def test_csv_reader_reads_csv(tmp_path: Path):
    """Read a standard CSV and verify DataFrame shape and column names."""
    from ml_toolbox.nodes.ingest import csv_reader

    csv_file = tmp_path / "data.csv"
    csv_file.write_text("name,age,score\nAlice,30,95.5\nBob,25,87.0\nCharlie,35,92.3\n")

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = csv_reader(inputs={}, params={"path": str(csv_file), "separator": ",", "header": True})

    parquet_path = Path(result["df"])
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert df.height == 3
    assert df.columns == ["name", "age", "score"]


def test_csv_reader_custom_separator(tmp_path: Path):
    """Read a tab-separated file."""
    from ml_toolbox.nodes.ingest import csv_reader

    csv_file = tmp_path / "data.tsv"
    csv_file.write_text("name\tage\tscore\nAlice\t30\t95.5\nBob\t25\t87.0\n")

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = csv_reader(inputs={}, params={"path": str(csv_file), "separator": "\t", "header": True})

    df = pl.read_parquet(Path(result["df"]))
    assert df.height == 2
    assert df.columns == ["name", "age", "score"]


def test_csv_reader_no_header(tmp_path: Path):
    """Read CSV without header row."""
    from ml_toolbox.nodes.ingest import csv_reader

    csv_file = tmp_path / "data.csv"
    csv_file.write_text("Alice,30,95.5\nBob,25,87.0\n")

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = csv_reader(inputs={}, params={"path": str(csv_file), "separator": ",", "header": False})

    df = pl.read_parquet(Path(result["df"]))
    assert df.height == 2
    assert df.width == 3
    # pandas assigns integer column names (0, 1, 2) when no header
    assert df.columns == ["0", "1", "2"]
