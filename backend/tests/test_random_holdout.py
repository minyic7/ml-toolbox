"""Tests for the Random Hold-out preprocessing node."""

from pathlib import Path
from unittest.mock import patch

import polars as pl

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_random_holdout_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.preprocessing.random_holdout"]
    assert meta["label"] == "Random Hold-out"
    assert meta["category"] == "Preprocessing"
    assert meta["type"] == "ml_toolbox.nodes.preprocessing.random_holdout"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    assert len(meta["params"]) == 5
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"test_size", "val_size", "random_seed", "stratify_column", "shuffle"}
    assert meta["guide"] != ""


def _make_input_parquet(tmp_path: Path, n_rows: int = 100) -> Path:
    """Create a simple input parquet file and return its path."""
    df = pl.DataFrame({
        "feature1": list(range(n_rows)),
        "feature2": [float(i * 2) for i in range(n_rows)],
        "target": [i % 2 for i in range(n_rows)],
    })
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)
    return input_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes train/val/test to separate files."""
    train_out = tmp_path / "train.parquet"
    val_out = tmp_path / "val.parquet"
    test_out = tmp_path / "test.parquet"

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        if name == "train":
            return train_out
        if name == "val":
            return val_out
        return test_out

    return mock_output


def test_basic_split_default(tmp_path: Path):
    """Default 70/10/20 split."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "val_size": 0.1, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height == 70
    assert val_df.height == 10
    assert test_df.height == 20
    assert train_df.height + val_df.height + test_df.height == 100


def test_custom_test_size(tmp_path: Path):
    """Custom test_size=0.3, val_size=0.1 → 60/10/30."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.3, "val_size": 0.1, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert test_df.height == 30
    # val_fraction = 0.1 / 0.7 ≈ 0.1428; sklearn rounds to 11 from 70 rows
    assert val_df.height == 11
    assert train_df.height == 59
    assert train_df.height + val_df.height + test_df.height == 100


def test_stratify_column(tmp_path: Path):
    """Stratified split preserves class ratios across all three sets."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "val_size": 0.1, "random_seed": 42, "stratify_column": "target", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))

    # Original has 50/50 class split; all sets should preserve that ratio
    train_ratio = float(train_df["target"].sum()) / train_df.height
    val_ratio = float(val_df["target"].sum()) / val_df.height
    test_ratio = float(test_df["target"].sum()) / test_df.height
    assert abs(train_ratio - 0.5) < 0.05
    assert abs(val_ratio - 0.5) < 0.15  # smaller set, wider tolerance
    assert abs(test_ratio - 0.5) < 0.05


def test_shuffle_false_preserves_order(tmp_path: Path):
    """With shuffle=False, data order is preserved."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "val_size": 0.1, "random_seed": 42, "stratify_column": "", "shuffle": False},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))

    # Without shuffle, splits are contiguous slices
    # train_test_split without shuffle takes last N as test, so:
    # first split: train_val = first 80, test = last 20
    # second split: train = first 70 of train_val, val = last 10 of train_val
    assert train_df["feature1"].to_list() == list(range(0, 70))
    assert val_df["feature1"].to_list() == list(range(70, 80))
    assert test_df["feature1"].to_list() == list(range(80, 100))


def test_train_val_test_counts_equal_original(tmp_path: Path):
    """Train + val + test row counts equal original dataset size."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    n_rows = 137  # odd number to test rounding
    input_path = _make_input_parquet(tmp_path, n_rows=n_rows)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "val_size": 0.1, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height + val_df.height + test_df.height == n_rows


def test_val_size_zero(tmp_path: Path):
    """val_size=0 produces empty val DataFrame, train=80, test=20."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "val_size": 0.0, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height == 80
    assert val_df.height == 0
    assert test_df.height == 20
    # val should have same schema but no rows
    assert val_df.schema == train_df.schema


def test_custom_val_size(tmp_path: Path):
    """val_size=0.2, test_size=0.2 → train=60, val=20, test=20."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "val_size": 0.2, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height == 60
    assert val_df.height == 20
    assert test_df.height == 20
