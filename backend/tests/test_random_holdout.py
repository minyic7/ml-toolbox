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
    assert meta["outputs"] == [{"name": "train", "type": "TABLE"}, {"name": "test", "type": "TABLE"}]
    assert len(meta["params"]) == 4
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"test_size", "random_seed", "stratify_column", "shuffle"}
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


def test_basic_split_default(tmp_path: Path):
    """Default 80/20 split."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)
    train_out = tmp_path / "train.parquet"
    test_out = tmp_path / "test.parquet"

    call_count = 0

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        nonlocal call_count
        call_count += 1
        return train_out if name == "train" else test_out

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=mock_output):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height == 80
    assert test_df.height == 20
    assert train_df.height + test_df.height == 100


def test_custom_test_size(tmp_path: Path):
    """Custom 70/30 split."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)
    train_out = tmp_path / "train.parquet"
    test_out = tmp_path / "test.parquet"

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return train_out if name == "train" else test_out

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=mock_output):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.3, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height == 70
    assert test_df.height == 30


def test_stratify_column(tmp_path: Path):
    """Stratified split preserves class ratios."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)
    train_out = tmp_path / "train.parquet"
    test_out = tmp_path / "test.parquet"

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return train_out if name == "train" else test_out

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=mock_output):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "random_seed": 42, "stratify_column": "target", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    test_df = pl.read_parquet(Path(result["test"]))

    # Original has 50/50 class split; both sets should preserve that ratio
    train_ratio = float(train_df["target"].sum()) / train_df.height
    test_ratio = float(test_df["target"].sum()) / test_df.height
    assert abs(train_ratio - 0.5) < 0.05
    assert abs(test_ratio - 0.5) < 0.05


def test_shuffle_false_preserves_order(tmp_path: Path):
    """With shuffle=False, data order is preserved."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    input_path = _make_input_parquet(tmp_path, n_rows=100)
    train_out = tmp_path / "train.parquet"
    test_out = tmp_path / "test.parquet"

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return train_out if name == "train" else test_out

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=mock_output):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "random_seed": 42, "stratify_column": "", "shuffle": False},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    test_df = pl.read_parquet(Path(result["test"]))

    # Without shuffle, train should be first 80 rows, test should be last 20
    assert train_df["feature1"].to_list() == list(range(80))
    assert test_df["feature1"].to_list() == list(range(80, 100))


def test_train_test_counts_equal_original(tmp_path: Path):
    """Train + test row counts equal original dataset size."""
    from ml_toolbox.nodes.preprocessing import random_holdout

    n_rows = 137  # odd number to test rounding
    input_path = _make_input_parquet(tmp_path, n_rows=n_rows)
    train_out = tmp_path / "train.parquet"
    test_out = tmp_path / "test.parquet"

    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return train_out if name == "train" else test_out

    with patch("ml_toolbox.nodes.preprocessing._get_output_path", side_effect=mock_output):
        result = random_holdout(
            inputs={"df": str(input_path)},
            params={"test_size": 0.2, "random_seed": 42, "stratify_column": "", "shuffle": True},
        )

    train_df = pl.read_parquet(Path(result["train"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height + test_df.height == n_rows
