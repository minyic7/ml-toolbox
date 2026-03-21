"""Tests for the node protocol, decorator, and ingest nodes."""

from pathlib import Path
from unittest.mock import patch

import polars as pl

from ml_toolbox.protocol import NODE_REGISTRY


# Import nodes to trigger registration
import ml_toolbox.nodes  # noqa: F401


EXPECTED_NODES = {
    "ml_toolbox.nodes.ingest.csv_reader",
    "ml_toolbox.nodes.ingest.parquet_reader",
}


def test_registry_contains_all_ingest_nodes():
    """Both ingest nodes should be registered after import."""
    assert EXPECTED_NODES.issubset(NODE_REGISTRY.keys())


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
    assert "default_code" in meta


def test_parquet_reader_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.ingest.parquet_reader"]
    assert meta["label"] == "Parquet Reader"
    assert meta["category"] == "Ingest"
    assert meta["inputs"] == []
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"path", "columns"}


def test_csv_reader_produces_parquet(tmp_path: Path):
    """Running csv_reader should produce a valid parquet file."""
    # Create a CSV file to read
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,value\n1,10\n2,20\n3,30\n")

    from ml_toolbox.nodes.ingest import csv_reader

    output_file = tmp_path / "df.parquet"
    with patch(
        "ml_toolbox.nodes.ingest._get_output_path", return_value=output_file
    ):
        result = csv_reader(inputs={}, params={"path": str(csv_path), "separator": ",", "header": True})

    parquet_path = Path(result["df"])
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert df.height == 3
    assert set(df.columns) == {"id", "value"}
