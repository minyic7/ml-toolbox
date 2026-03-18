"""Tests for the train/test split node."""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

# Trigger registration
import ml_toolbox.nodes  # noqa: F401


def _make_input_df(tmp_path: Path, n: int = 100) -> Path:
    """Create a simple parquet file with n rows and a binary category column."""
    import random

    random.seed(0)
    df = pl.DataFrame(
        {
            "id": list(range(n)),
            "value": [random.gauss(0, 1) for _ in range(n)],
            "category": ["A"] * (n // 2) + ["B"] * (n - n // 2),
        }
    )
    path = tmp_path / "input.parquet"
    df.write_parquet(path)
    return path


class TestSplitNodeRegistration:
    def test_registered_in_registry(self):
        assert "ml_toolbox.nodes.transform.split" in NODE_REGISTRY

    def test_metadata(self):
        meta = NODE_REGISTRY["ml_toolbox.nodes.transform.split"]
        assert meta["label"] == "Train/Test Split"
        assert meta["category"] == "Transform"
        assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
        assert meta["outputs"] == [
            {"name": "train", "type": "TABLE"},
            {"name": "test", "type": "TABLE"},
        ]
        assert len(meta["params"]) == 3
        param_names = {p["name"] for p in meta["params"]}
        assert param_names == {"test_size", "random_seed", "stratify_column"}


class TestSplitExecution:
    def test_80_20_split_proportions(self, tmp_path: Path):
        """An 80/20 split of 100 rows should produce 80 train and 20 test."""
        from ml_toolbox.nodes.transform import split

        input_path = _make_input_df(tmp_path, n=100)
        train_out = tmp_path / "train.parquet"
        test_out = tmp_path / "test.parquet"

        def mock_output(name="output", ext=".parquet"):
            return tmp_path / f"{name}{ext}"

        with patch("ml_toolbox.nodes.transform._get_output_path", side_effect=mock_output):
            result = split(
                inputs={"df": str(input_path)},
                params={"test_size": 0.2, "random_seed": 42, "stratify_column": ""},
            )

        train_df = pl.read_parquet(result["train"])
        test_df = pl.read_parquet(result["test"])

        assert train_df.height == 80
        assert test_df.height == 20
        # No overlap
        train_ids = set(train_df["id"].to_list())
        test_ids = set(test_df["id"].to_list())
        assert train_ids.isdisjoint(test_ids)
        assert train_ids | test_ids == set(range(100))

    def test_same_seed_produces_same_split(self, tmp_path: Path):
        """Running with the same random_seed should produce identical splits."""
        from ml_toolbox.nodes.transform import split

        input_path = _make_input_df(tmp_path, n=100)

        results = []
        for i in range(2):
            sub = tmp_path / f"run{i}"
            sub.mkdir()

            def mock_output(name="output", ext=".parquet", _sub=sub):
                return _sub / f"{name}{ext}"

            with patch("ml_toolbox.nodes.transform._get_output_path", side_effect=mock_output):
                result = split(
                    inputs={"df": str(input_path)},
                    params={"test_size": 0.2, "random_seed": 42, "stratify_column": ""},
                )
            results.append(result)

        train1 = pl.read_parquet(results[0]["train"])
        train2 = pl.read_parquet(results[1]["train"])
        assert train1["id"].to_list() == train2["id"].to_list()

    def test_stratified_split_preserves_class_proportions(self, tmp_path: Path):
        """Stratified split should preserve category distribution in both sets."""
        from ml_toolbox.nodes.transform import split

        input_path = _make_input_df(tmp_path, n=100)

        def mock_output(name="output", ext=".parquet"):
            return tmp_path / f"{name}{ext}"

        with patch("ml_toolbox.nodes.transform._get_output_path", side_effect=mock_output):
            result = split(
                inputs={"df": str(input_path)},
                params={"test_size": 0.2, "random_seed": 42, "stratify_column": "category"},
            )

        train_df = pl.read_parquet(result["train"])
        test_df = pl.read_parquet(result["test"])

        # Original has 50% A, 50% B. Both splits should preserve this.
        train_a = train_df.filter(pl.col("category") == "A").height
        train_b = train_df.filter(pl.col("category") == "B").height
        test_a = test_df.filter(pl.col("category") == "A").height
        test_b = test_df.filter(pl.col("category") == "B").height

        # With stratification, proportions should be exactly preserved
        assert train_a == 40
        assert train_b == 40
        assert test_a == 10
        assert test_b == 10
