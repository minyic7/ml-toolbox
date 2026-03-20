"""Tests for _file_metadata() — preview generation for all output file types."""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest

from ml_toolbox.routers.pipelines import _file_metadata


# ── .parquet ────────────────────────────────────────────────────


def test_parquet_preview(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    path = tmp_path / "test.parquet"
    df.to_parquet(path)

    meta = _file_metadata(path)

    assert meta["type"] == "parquet"
    assert meta["file"] == "test.parquet"
    assert meta["size"] > 0
    assert meta["preview"]["columns"] == ["a", "b"]
    assert len(meta["preview"]["rows"]) == 3
    assert meta["preview"]["total_rows"] == 3


# ── .csv ────────────────────────────────────────────────────────


def test_csv_preview(tmp_path: Path) -> None:
    path = tmp_path / "test.csv"
    path.write_text("a,b\n1,2\n3,4\n5,6")

    meta = _file_metadata(path)

    assert meta["type"] == "csv"
    assert meta["preview"]["columns"] == ["a", "b"]
    assert len(meta["preview"]["rows"]) == 3
    # CSV preview doesn't know total rows (only reads head)
    assert meta["preview"]["total_rows"] == -1


# ── .json (METRICS) ────────────────────────────────────────────


def test_json_preview(tmp_path: Path) -> None:
    path = tmp_path / "test.json"
    path.write_text(json.dumps({"accuracy": 0.95, "f1": 0.87}))

    meta = _file_metadata(path)

    assert meta["type"] == "json"
    assert meta["preview"]["accuracy"] == 0.95
    assert meta["preview"]["f1"] == 0.87


# ── .joblib (MODEL) ─────────────────────────────────────────────


def test_joblib_preview(tmp_path: Path) -> None:
    path = tmp_path / "test.joblib"
    joblib.dump({"dummy": True}, path)

    meta = _file_metadata(path)

    assert meta["type"] == "joblib"
    assert meta["preview"]["format"] == "joblib"
    assert meta["preview"]["file_size"] > 0


# ── .npy (ARRAY) ────────────────────────────────────────────────


def test_npy_preview(tmp_path: Path) -> None:
    arr = np.random.randn(100, 50)
    path = tmp_path / "test.npy"
    np.save(path, arr)

    meta = _file_metadata(path)

    assert meta["type"] == "npy"
    assert meta["preview"]["shape"] == [100, 50]
    assert meta["preview"]["dtype"] == "float64"
    assert len(meta["preview"]["values"]) == 20  # truncated to first 20
    assert meta["preview"]["total_elements"] == 5000


def test_npy_empty_array(tmp_path: Path) -> None:
    path = tmp_path / "empty.npy"
    np.save(path, np.array([]))

    meta = _file_metadata(path)

    assert meta["preview"]["shape"] == [0]
    assert meta["preview"]["values"] == []
    assert meta["preview"]["total_elements"] == 0


def test_npy_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.npy"
    path.write_bytes(b"not a numpy file")

    meta = _file_metadata(path)

    # Graceful degradation: no preview when file is unreadable
    assert "preview" not in meta


# ── .pt (TENSOR) ────────────────────────────────────────────────


def test_pt_preview(tmp_path: Path) -> None:
    path = tmp_path / "test.pt"
    path.write_bytes(b"fake pt data")

    meta = _file_metadata(path)

    assert meta["type"] == "pt"
    assert meta["preview"]["format"] == "pytorch"
    assert meta["preview"]["file_size"] > 0
