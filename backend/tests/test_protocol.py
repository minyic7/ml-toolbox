"""Tests for the node protocol, decorator, and demo nodes."""

from pathlib import Path
from unittest.mock import patch

import polars as pl

from ml_toolbox.protocol import NODE_REGISTRY


# Import nodes to trigger registration
import ml_toolbox.nodes  # noqa: F401


EXPECTED_NODES = {
    "ml_toolbox.nodes.demo.run",
    "ml_toolbox.nodes.demo.clean_data",
    "ml_toolbox.nodes.demo.summarize_data",
}


def test_registry_contains_all_demo_nodes():
    """All 3 demo nodes should be registered after import."""
    assert EXPECTED_NODES.issubset(NODE_REGISTRY.keys())


def test_generate_data_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.demo.run"]
    assert meta["label"] == "Generate Data"
    assert meta["category"] == "Demo"
    assert meta["type"] == "ml_toolbox.nodes.demo.run"
    assert meta["inputs"] == []
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    assert len(meta["params"]) == 1
    param = meta["params"][0]
    assert param["type"] == "slider"
    assert param["name"] == "rows"
    assert param["min"] == 10
    assert param["max"] == 1000
    assert "default_code" in meta


def test_clean_data_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.demo.clean_data"]
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    assert len(meta["params"]) == 1
    param = meta["params"][0]
    assert param["type"] == "select"
    assert param["name"] == "strategy"
    assert "mean" in param["options"]


def test_summarize_data_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.demo.summarize_data"]
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [{"name": "summary", "type": "METRICS"}]
    assert meta["params"] == []


def test_generate_data_produces_parquet(tmp_path: Path):
    """Running generate_data should produce a valid parquet file."""
    from ml_toolbox.nodes.demo import run as generate_data

    output_file = tmp_path / "df.parquet"
    with patch(
        "ml_toolbox.nodes.demo._get_output_path", return_value=output_file
    ):
        result = generate_data(inputs={}, params={"rows": 50})

    parquet_path = Path(result["df"])
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert df.height == 50
    assert set(df.columns) == {"id", "value_a", "value_b", "category"}
