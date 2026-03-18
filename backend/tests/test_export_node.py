"""Tests for the Export Table and Export Model nodes."""

import csv
import io
from pathlib import Path

import joblib
import polars as pl
import pytest
from fastapi.testclient import TestClient

from ml_toolbox.main import app
from ml_toolbox.nodes.export import export_model, export_table

client = TestClient(app)


@pytest.fixture()
def sample_parquet(tmp_path: Path) -> Path:
    """Write a small sample DataFrame to parquet and return its path."""
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    p = tmp_path / "input.parquet"
    df.write_parquet(p)
    return p


class TestExportTableCSV:
    def test_csv_export_produces_valid_csv(self, sample_parquet: Path, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.nodes.export._get_output_path",
            lambda name, ext=".parquet": tmp_path / f"{name}{ext}",
        )
        result = export_table(
            {"df": str(sample_parquet)},
            {"format": "csv", "filename": "output"},
        )
        # Output passes through original input for chaining
        assert result["file"] == str(sample_parquet)

        out = tmp_path / "output.csv"
        assert out.exists()
        assert out.suffix == ".csv"

        reader = csv.DictReader(io.StringIO(out.read_text()))
        rows = list(reader)
        assert len(rows) == 3
        assert rows[0]["a"] == "1"
        assert rows[0]["b"] == "x"


class TestExportTableParquet:
    def test_parquet_export_produces_valid_parquet(self, sample_parquet: Path, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.nodes.export._get_output_path",
            lambda name, ext=".parquet": tmp_path / f"{name}{ext}",
        )
        result = export_table(
            {"df": str(sample_parquet)},
            {"format": "parquet", "filename": "output"},
        )
        # Output passes through original input for chaining
        assert result["file"] == str(sample_parquet)

        out = tmp_path / "output.parquet"
        assert out.exists()
        assert out.suffix == ".parquet"

        df = pl.read_parquet(out)
        assert df.shape == (3, 2)
        assert df["a"].to_list() == [1, 2, 3]
        assert df["b"].to_list() == ["x", "y", "z"]


class TestExportTableFilename:
    def test_custom_filename_is_applied(self, sample_parquet: Path, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.nodes.export._get_output_path",
            lambda name, ext=".parquet": tmp_path / f"{name}{ext}",
        )
        result = export_table(
            {"df": str(sample_parquet)},
            {"format": "csv", "filename": "my_export"},
        )
        # Output passes through original input for chaining
        assert result["file"] == str(sample_parquet)

        out = tmp_path / "my_export.csv"
        assert out.exists()
        assert out.name == "my_export.csv"


@pytest.fixture()
def sample_model(tmp_path: Path) -> Path:
    """Dump a simple sklearn model to joblib and return its path."""
    from sklearn.linear_model import LinearRegression
    import numpy as np

    model = LinearRegression()
    model.fit(np.array([[1], [2], [3]]), np.array([1, 2, 3]))
    p = tmp_path / "input_model.joblib"
    joblib.dump(model, p)
    return p


class TestExportModelJoblib:
    def test_exported_file_is_valid_joblib(self, sample_model: Path, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.nodes.export._get_output_path",
            lambda name, ext=".parquet": tmp_path / f"{name}{ext}",
        )
        result = export_model(
            {"model": str(sample_model)},
            {"filename": "model"},
        )
        # Passthrough for chaining
        assert result["model"] == str(sample_model)

        out = tmp_path / "model.joblib"
        assert out.exists()
        assert out.suffix == ".joblib"

        # Verify the file can be loaded back and is a valid model
        loaded = joblib.load(out)
        assert hasattr(loaded, "predict")

    def test_custom_filename_is_applied(self, sample_model: Path, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.nodes.export._get_output_path",
            lambda name, ext=".parquet": tmp_path / f"{name}{ext}",
        )
        result = export_model(
            {"model": str(sample_model)},
            {"filename": "my_model"},
        )
        assert result["model"] == str(sample_model)

        out = tmp_path / "my_model.joblib"
        assert out.exists()
        assert out.name == "my_model.joblib"


class TestExportModelRegistration:
    def test_node_registered(self):
        resp = client.get("/api/nodes/ml_toolbox.nodes.export.export_model")
        assert resp.status_code == 200
        node = resp.json()
        assert node["label"] == "Export Model"
        assert node["category"] == "Export"
        assert len(node["inputs"]) == 1
        assert node["inputs"][0]["name"] == "model"
        assert node["inputs"][0]["type"] == "MODEL"
        assert len(node["outputs"]) == 1
        assert node["outputs"][0]["name"] == "model"
        assert node["outputs"][0]["type"] == "MODEL"


class TestExportTableRegistration:
    def test_node_registered(self):
        resp = client.get("/api/nodes/ml_toolbox.nodes.export.export_table")
        assert resp.status_code == 200
        node = resp.json()
        assert node["label"] == "Export Table"
        assert node["category"] == "Export"
        assert len(node["inputs"]) == 1
        assert node["inputs"][0]["name"] == "df"
        assert node["inputs"][0]["type"] == "TABLE"
        assert len(node["outputs"]) == 1
        assert node["outputs"][0]["name"] == "file"
        assert node["outputs"][0]["type"] == "TABLE"
