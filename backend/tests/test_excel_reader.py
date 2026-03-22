"""Tests for the Excel reader ingest node."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import polars as pl

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_excel_reader_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.ingest.excel_reader"]
    assert meta["label"] == "Excel Reader"
    assert meta["category"] == "Ingest"
    assert meta["type"] == "ml_toolbox.nodes.ingest.excel_reader"
    assert meta["inputs"] == []
    assert meta["outputs"] == [{"name": "df", "type": "TABLE"}]
    assert len(meta["params"]) == 4
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"path", "sheet_name", "header_row", "skip_rows"}


def test_excel_reader_reads_xlsx(tmp_path: Path):
    """Read a standard .xlsx file and verify DataFrame shape and columns."""
    from ml_toolbox.nodes.ingest import excel_reader

    xlsx_file = tmp_path / "data.xlsx"
    df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35], "score": [95.5, 87.0, 92.3]})
    df.to_excel(xlsx_file, index=False, engine="openpyxl")

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = excel_reader(inputs={}, params={"path": str(xlsx_file)})

    parquet_path = Path(result["df"])
    assert parquet_path.exists()
    out_df = pl.read_parquet(parquet_path)
    assert out_df.height == 3
    assert out_df.columns == ["name", "age", "score"]


def test_excel_reader_sheet_selection(tmp_path: Path):
    """Reading a specific sheet by name should return that sheet's data."""
    from ml_toolbox.nodes.ingest import excel_reader

    xlsx_file = tmp_path / "multi.xlsx"
    with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
        pd.DataFrame({"a": [1, 2]}).to_excel(writer, sheet_name="first", index=False)
        pd.DataFrame({"b": [3, 4, 5]}).to_excel(writer, sheet_name="second", index=False)

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = excel_reader(inputs={}, params={"path": str(xlsx_file), "sheet_name": "second"})

    out_df = pl.read_parquet(result["df"])
    assert out_df.height == 3
    assert out_df.columns == ["b"]


def test_excel_reader_skip_rows(tmp_path: Path):
    """skip_rows should skip rows before reading data."""
    from ml_toolbox.nodes.ingest import excel_reader

    xlsx_file = tmp_path / "data.xlsx"
    # Write a DataFrame with extra rows at the top
    df = pd.DataFrame({"x": ["ignore", "col_a", "1", "2"], "y": ["ignore", "col_b", "3", "4"]})
    df.to_excel(xlsx_file, index=False, header=False, engine="openpyxl")

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = excel_reader(
            inputs={},
            params={"path": str(xlsx_file), "skip_rows": "1", "header_row": "0"},
        )

    out_df = pl.read_parquet(result["df"])
    assert out_df.height == 2
    assert out_df.columns == ["col_a", "col_b"]


def test_excel_reader_default_first_sheet(tmp_path: Path):
    """Empty sheet_name should default to the first sheet."""
    from ml_toolbox.nodes.ingest import excel_reader

    xlsx_file = tmp_path / "multi.xlsx"
    with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
        pd.DataFrame({"first_col": [10, 20]}).to_excel(writer, sheet_name="Sheet1", index=False)
        pd.DataFrame({"second_col": [30, 40]}).to_excel(writer, sheet_name="Sheet2", index=False)

    output_file = tmp_path / "df.parquet"
    with patch("ml_toolbox.nodes.ingest._get_output_path", return_value=output_file):
        result = excel_reader(inputs={}, params={"path": str(xlsx_file), "sheet_name": ""})

    out_df = pl.read_parquet(result["df"])
    assert out_df.columns == ["first_col"]
