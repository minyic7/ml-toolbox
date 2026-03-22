"""Tests for the Stratified Hold-out split node."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from ml_toolbox.protocol import NODE_REGISTRY

import ml_toolbox.nodes  # noqa: F401


def test_stratified_holdout_metadata():
    meta = NODE_REGISTRY["ml_toolbox.nodes.split.stratified_holdout"]
    assert meta["label"] == "Stratified Hold-out"
    assert meta["category"] == "Split"
    assert meta["type"] == "ml_toolbox.nodes.split.stratified_holdout"
    assert meta["inputs"] == [{"name": "df", "type": "TABLE"}]
    assert meta["outputs"] == [
        {"name": "train", "type": "TABLE"},
        {"name": "val", "type": "TABLE"},
        {"name": "test", "type": "TABLE"},
    ]
    assert len(meta["params"]) == 4
    param_names = {p["name"] for p in meta["params"]}
    assert param_names == {"train_ratio", "val_ratio", "test_ratio", "seed"}
    assert meta["guide"] != ""


# ── Helpers ───────────────────────────────────────────────────────


def _make_input(tmp_path: Path, n_rows: int = 120, class_dist: dict[int, int] | None = None) -> Path:
    """Create input parquet + .meta.json sidecar.

    class_dist: {class_label: count} — defaults to balanced binary (50/50).
    """
    if class_dist is None:
        # Balanced binary
        targets = [i % 2 for i in range(n_rows)]
    else:
        targets = []
        for label, count in class_dist.items():
            targets.extend([label] * count)
        n_rows = len(targets)

    df = pl.DataFrame({
        "feature1": list(range(n_rows)),
        "feature2": [float(i * 2) for i in range(n_rows)],
        "target": targets,
    })
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {
            "feature1": {"dtype": "int64", "semantic_type": "continuous", "role": "feature"},
            "feature2": {"dtype": "float64", "semantic_type": "continuous", "role": "feature"},
            "target": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "target": "target",
        "row_count": n_rows,
        "generated_by": "test",
    }
    meta_path = tmp_path / "input.meta.json"
    meta_path.write_text(json.dumps(meta))

    return input_path


def _mock_output_factory(tmp_path: Path):
    """Return a mock _get_output_path that routes train/val/test to separate files."""
    def mock_output(name: str = "output", ext: str = ".parquet") -> Path:
        return tmp_path / f"{name}{ext}"
    return mock_output


def _run_stratified(tmp_path: Path, input_path: Path, **param_overrides) -> dict:
    """Run stratified_holdout with default params, allowing overrides."""
    from ml_toolbox.nodes.split import stratified_holdout

    params = {
        "train_ratio": 0.7,
        "val_ratio": 0.15,
        "test_ratio": 0.15,
        "seed": "42",
        **param_overrides,
    }
    with patch("ml_toolbox.nodes.split._get_output_path", side_effect=_mock_output_factory(tmp_path)):
        return stratified_holdout(inputs={"df": str(input_path)}, params=params)


# ── Basic split tests ─────────────────────────────────────────────


def test_basic_split_default(tmp_path: Path):
    """Default 70/15/15 split preserves total row count."""
    input_path = _make_input(tmp_path, n_rows=120)
    result = _run_stratified(tmp_path, input_path)

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))

    assert train_df.height + val_df.height + test_df.height == 120
    # Approximate expected sizes (sklearn rounding may vary by 1-2)
    assert abs(train_df.height - 84) <= 2
    assert abs(val_df.height - 18) <= 2
    assert abs(test_df.height - 18) <= 2


def test_class_distribution_preserved(tmp_path: Path):
    """Class ratios should be approximately preserved across all 3 splits."""
    # Imbalanced: 80% class-0, 20% class-1
    input_path = _make_input(tmp_path, class_dist={0: 160, 1: 40})
    result = _run_stratified(tmp_path, input_path)

    original_ratio = 40 / 200  # 0.2

    for split_name in ("train", "val", "test"):
        split_df = pl.read_parquet(Path(result[split_name]))
        split_ratio = split_df.filter(pl.col("target") == 1).height / split_df.height
        assert abs(split_ratio - original_ratio) < 0.05, (
            f"{split_name} split ratio {split_ratio:.3f} deviates from original {original_ratio:.3f}"
        )


def test_multiclass_distribution_preserved(tmp_path: Path):
    """Stratification works for 3+ classes."""
    input_path = _make_input(tmp_path, class_dist={0: 60, 1: 30, 2: 10})
    result = _run_stratified(tmp_path, input_path)

    total = 100
    expected_ratios = {0: 60 / total, 1: 30 / total, 2: 10 / total}

    for split_name in ("train", "val", "test"):
        split_df = pl.read_parquet(Path(result[split_name]))
        for cls, expected in expected_ratios.items():
            actual = split_df.filter(pl.col("target") == cls).height / split_df.height
            assert abs(actual - expected) < 0.1, (
                f"{split_name}: class {cls} ratio {actual:.3f} vs expected {expected:.3f}"
            )


def test_custom_ratios(tmp_path: Path):
    """Custom 60/20/20 split."""
    input_path = _make_input(tmp_path, n_rows=100)
    result = _run_stratified(tmp_path, input_path, train_ratio=0.6, val_ratio=0.2, test_ratio=0.2)

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))

    assert train_df.height + val_df.height + test_df.height == 100
    assert abs(train_df.height - 60) <= 2
    assert abs(val_df.height - 20) <= 2
    assert abs(test_df.height - 20) <= 2


def test_reproducible_with_same_seed(tmp_path: Path):
    """Same seed produces identical splits."""
    input_path = _make_input(tmp_path, n_rows=100)

    out1 = tmp_path / "run1"
    out1.mkdir()
    result1 = _run_stratified(out1, input_path, seed="123")

    out2 = tmp_path / "run2"
    out2.mkdir()
    result2 = _run_stratified(out2, input_path, seed="123")

    for split in ("train", "val", "test"):
        df1 = pl.read_parquet(Path(result1[split]))
        df2 = pl.read_parquet(Path(result2[split]))
        assert df1.equals(df2), f"{split} splits differ with same seed"


def test_total_rows_preserved_odd_count(tmp_path: Path):
    """Train + val + test == original for odd row count."""
    input_path = _make_input(tmp_path, n_rows=137)
    result = _run_stratified(tmp_path, input_path)

    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height + val_df.height + test_df.height == 137


# ── .meta.json sidecar tests ─────────────────────────────────────


def test_meta_json_written_for_each_split(tmp_path: Path):
    """Each output should have a .meta.json sidecar with split identity and seed."""
    input_path = _make_input(tmp_path, n_rows=100)
    result = _run_stratified(tmp_path, input_path)

    for split_name in ("train", "val", "test"):
        meta_path = Path(result[split_name]).with_suffix(".meta.json")
        assert meta_path.exists(), f"Missing .meta.json for {split_name}"
        meta = json.loads(meta_path.read_text())
        assert meta["split"] == split_name
        assert meta["seed"] == 42
        assert "target" in meta
        assert meta["target"] == "target"


# ── Validation / error tests ─────────────────────────────────────


def test_ratios_not_summing_to_one(tmp_path: Path):
    """Should fail clearly when ratios don't sum to 1.0."""
    input_path = _make_input(tmp_path, n_rows=100)
    with pytest.raises(ValueError, match="must equal 1.0"):
        _run_stratified(tmp_path, input_path, train_ratio=0.5, val_ratio=0.2, test_ratio=0.2)


def test_no_meta_json(tmp_path: Path):
    """Should fail clearly when .meta.json is missing."""
    df = pl.DataFrame({"feature1": [1, 2, 3], "target": [0, 1, 0]})
    input_path = tmp_path / "no_meta.parquet"
    df.write_parquet(input_path)
    # No .meta.json written

    with pytest.raises(FileNotFoundError, match="No .meta.json sidecar found"):
        _run_stratified(tmp_path, input_path)


def test_no_target_in_meta(tmp_path: Path):
    """Should fail clearly when .meta.json has no target column."""
    df = pl.DataFrame({"feature1": list(range(20)), "col2": list(range(20))})
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {
            "feature1": {"dtype": "int64", "role": "feature"},
            "col2": {"dtype": "int64", "role": "feature"},
        },
        "row_count": 20,
    }
    (tmp_path / "input.meta.json").write_text(json.dumps(meta))

    with pytest.raises(ValueError, match="No target column found"):
        _run_stratified(tmp_path, input_path)


def test_continuous_float_target_rejected(tmp_path: Path):
    """Should fail clearly if target is continuous float."""
    df = pl.DataFrame({
        "feature1": list(range(50)),
        "target": [float(i) * 0.1 for i in range(50)],
    })
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {
            "feature1": {"dtype": "int64", "role": "feature"},
            "target": {"dtype": "float64", "semantic_type": "continuous", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "input.meta.json").write_text(json.dumps(meta))

    with pytest.raises(ValueError, match="continuous float values"):
        _run_stratified(tmp_path, input_path)


def test_too_few_samples_per_class(tmp_path: Path):
    """Should fail clearly if any class has < 3 samples."""
    input_path = _make_input(tmp_path, class_dist={0: 50, 1: 2})

    with pytest.raises(ValueError, match="at least 3 samples per class"):
        _run_stratified(tmp_path, input_path)


def test_target_column_not_in_dataframe(tmp_path: Path):
    """Should fail clearly if target column from meta doesn't exist in data."""
    df = pl.DataFrame({"feature1": list(range(20)), "other": list(range(20))})
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {"feature1": {"dtype": "int64", "role": "feature"}},
        "target": "nonexistent_column",
    }
    (tmp_path / "input.meta.json").write_text(json.dumps(meta))

    with pytest.raises(ValueError, match="not found in DataFrame"):
        _run_stratified(tmp_path, input_path)


def test_integer_valued_float_target_accepted(tmp_path: Path):
    """Float column with integer values (e.g. 0.0, 1.0) should be accepted."""
    df = pl.DataFrame({
        "feature1": list(range(100)),
        "target": [float(i % 3) for i in range(100)],
    })
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {
            "feature1": {"dtype": "int64", "role": "feature"},
            "target": {"dtype": "float64", "semantic_type": "categorical", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "input.meta.json").write_text(json.dumps(meta))

    result = _run_stratified(tmp_path, input_path)
    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height + val_df.height + test_df.height == 100


def test_string_target_accepted(tmp_path: Path):
    """String/categorical target should work fine."""
    labels = ["cat"] * 40 + ["dog"] * 35 + ["bird"] * 25
    df = pl.DataFrame({
        "feature1": list(range(100)),
        "target": labels,
    })
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {
            "feature1": {"dtype": "int64", "role": "feature"},
            "target": {"dtype": "str", "semantic_type": "categorical", "role": "target"},
        },
        "target": "target",
    }
    (tmp_path / "input.meta.json").write_text(json.dumps(meta))

    result = _run_stratified(tmp_path, input_path)
    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height + val_df.height + test_df.height == 100

    # Check distribution preserved
    for split_name in ("train", "val", "test"):
        split_df = pl.read_parquet(Path(result[split_name]))
        cat_ratio = split_df.filter(pl.col("target") == "cat").height / split_df.height
        assert abs(cat_ratio - 0.4) < 0.1


def test_fallback_to_role_target_in_columns(tmp_path: Path):
    """If top-level 'target' key is missing, find column with role=target."""
    df = pl.DataFrame({
        "feature1": list(range(60)),
        "my_label": [i % 2 for i in range(60)],
    })
    input_path = tmp_path / "input.parquet"
    df.write_parquet(input_path)

    meta = {
        "columns": {
            "feature1": {"dtype": "int64", "role": "feature"},
            "my_label": {"dtype": "int64", "semantic_type": "binary", "role": "target"},
        },
        "row_count": 60,
    }
    (tmp_path / "input.meta.json").write_text(json.dumps(meta))

    result = _run_stratified(tmp_path, input_path)
    train_df = pl.read_parquet(Path(result["train"]))
    val_df = pl.read_parquet(Path(result["val"]))
    test_df = pl.read_parquet(Path(result["test"]))
    assert train_df.height + val_df.height + test_df.height == 60
