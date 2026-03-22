from pathlib import Path

from ml_toolbox.protocol import PortType, Slider, Text, Toggle, node


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


@node(
    inputs={"df": PortType.TABLE},
    outputs={"train": PortType.TABLE, "val": PortType.TABLE, "test": PortType.TABLE},
    params={
        "test_size": Slider(min=0.05, max=0.5, step=0.05, default=0.2,
                           description="Fraction of data reserved for the test set"),
        "val_size": Slider(min=0.0, max=0.4, step=0.05, default=0.1,
                          description="Fraction of data reserved for the validation set (0 = no validation split)"),
        "random_seed": Text(default="42",
                           description="Random seed for reproducible splits (any integer)",
                           placeholder="42"),
        "stratify_column": Text(default="",
                               description="Column to stratify by (preserves class ratios across all splits)",
                               placeholder="target"),
        "shuffle": Toggle(default=True,
                         description="Shuffle data before splitting"),
    },
    label="Random Hold-out",
    category="Split",
    description="Split a DataFrame into train, validation, and test sets using random hold-out.",
    allowed_upstream={
        "df": ["csv_reader", "parquet_reader"],
    },
    guide="""## Random Hold-out Split

The simplest data splitting strategy: randomly partition your dataset into three sets.

### How it works
```
two-step split: first separate test, then split remainder into train + val
```

Your data is shuffled (unless disabled) and split into three sets:
- **Train set** (default 70%) — used to fit the model
- **Validation set** (default 10%) — used for hyperparameter tuning and model selection
- **Test set** (default 20%) — held out for final, unbiased evaluation

Set `val_size = 0` to skip validation and get a simple train/test split.

### When to use
- Dataset is **large** (typically 50,000+ rows) — single split gives stable estimates
- Model selection is already decided — you just need a final performance number
- You need **speed** — one split is faster than k-fold cross-validation

### When NOT to use
- Dataset is **small** (< 5,000 rows) — single split has high variance, use K-Fold instead
- You need to **compare models** — use cross-validation for reliable comparison
- Dataset has **temporal ordering** — use Time Series Split to prevent data leakage

### Common pitfalls
- **Random seed sensitivity**: different seeds → different results. Always set a fixed seed for reproducibility
- **Class imbalance**: if your target has rare classes (e.g. 5% positive), use `stratify_column` to preserve ratios across all splits
- **Data leakage**: any preprocessing that uses statistics (mean, std, vocabulary) must be fit on train only, then applied to val and test

### Parameters
| Parameter | Purpose |
|-----------|--------|
| `test_size` | Fraction held out for test (0.2 = 20%) |
| `val_size` | Fraction for validation (0 = skip, get train/test only) |
| `random_seed` | Fix this for reproducible results |
| `stratify_column` | Preserves class distribution across splits |
| `shuffle` | Disable for ordered data (but consider Time Series Split) |
""",
)
def random_holdout(inputs: dict, params: dict) -> dict:
    """Split a DataFrame into train, validation, and test sets using random hold-out."""
    import polars as pl
    from sklearn.model_selection import train_test_split

    df = pl.read_parquet(inputs["df"])

    test_size = float(params.get("test_size", 0.2))
    val_size = float(params.get("val_size", 0.1))
    random_seed = int(params.get("random_seed", "42"))
    stratify_col = params.get("stratify_column", "")
    shuffle = params.get("shuffle", True)

    indices = list(range(len(df)))
    stratify = df[stratify_col].to_list() if stratify_col else None

    # Step 1: split off test set
    train_val_idx, test_idx = train_test_split(
        indices,
        test_size=test_size,
        random_state=random_seed,
        shuffle=shuffle,
        stratify=stratify,
    )

    # Step 2: split train_val into train + val (if val_size > 0)
    if val_size > 0:
        # val_size is relative to original data, so adjust for remaining fraction
        val_fraction = val_size / (1 - test_size)
        stratify_val = [stratify[i] for i in train_val_idx] if stratify else None
        train_idx, val_idx = train_test_split(
            train_val_idx,
            test_size=val_fraction,
            random_state=random_seed,
            shuffle=shuffle,
            stratify=stratify_val,
        )
    else:
        train_idx = train_val_idx
        val_idx = []

    train_df = df[train_idx]
    val_df = df[val_idx] if val_idx else pl.DataFrame(schema=df.schema)
    test_df = df[test_idx]

    train_path = _get_output_path("train")
    val_path = _get_output_path("val")
    test_path = _get_output_path("test")
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    return {"train": str(train_path), "val": str(val_path), "test": str(test_path)}



@node(
    inputs={"df": PortType.TABLE},
    outputs={"train": PortType.TABLE, "val": PortType.TABLE, "test": PortType.TABLE},
    params={
        "train_ratio": Slider(min=0.5, max=0.9, step=0.05, default=0.7,
                              description="Fraction of data for the training set"),
        "val_ratio": Slider(min=0.0, max=0.3, step=0.05, default=0.15,
                            description="Fraction of data for the validation set"),
        "test_ratio": Slider(min=0.05, max=0.3, step=0.05, default=0.15,
                             description="Fraction of data for the test set"),
        "seed": Text(default="42",
                     description="Random seed for reproducible splits (any integer)",
                     placeholder="42"),
        "target_column": Text(default="", description="Target column for stratification (auto-detected from schema)"),
    },
    label="Stratified Hold-out",
    category="Split",
    description="Split a DataFrame into train/val/test sets preserving class distribution of the target column.",
    allowed_upstream={
        "df": ["csv_reader", "parquet_reader"],
    },
    guide="""## Stratified Hold-out Split

Like Random Hold-out, but **preserves the class distribution** of your target column across all three splits.

### When to use (instead of Random Hold-out)
- Your target column has **imbalanced classes** (e.g. 95% negative, 5% positive) — random splitting could leave a split with zero positive samples
- You want **consistent class ratios** in train, val, and test for fair evaluation
- Your dataset is small-to-medium and class counts are uneven

### How it works
1. Uses the `target_column` parameter (auto-configured from schema) for stratification
2. Validates the target is **categorical or integer** (not continuous float)
3. Validates every class has **at least 3 samples** (needed for a 3-way split)
4. Uses sklearn `train_test_split` with `stratify` — two-step split (test first, then train/val)

### Minimum sample requirements
Stratification needs at least **2 samples per class per split**. With a 3-way split, that means each class needs a minimum of ~3 samples total. Classes with fewer samples will cause a clear error message.

### Relationship to target column
The target column is set via the `target_column` parameter, which is auto-configured from the upstream schema.

### Parameters
| Parameter | Purpose |
|-----------|--------|
| `train_ratio` | Fraction for training (default 0.7 = 70%) |
| `val_ratio` | Fraction for validation (default 0.15 = 15%) |
| `test_ratio` | Fraction for test (default 0.15 = 15%) |
| `seed` | Fix this for reproducible results |

**Note:** Ratios must sum to 1.0. The node will fail with a clear message if they don't.
""",
)
def stratified_holdout(inputs: dict, params: dict) -> dict:
    """Split a DataFrame into train/val/test sets preserving class distribution."""
    import polars as pl
    from sklearn.model_selection import train_test_split

    df = pl.read_parquet(inputs["df"])

    train_ratio = float(params.get("train_ratio", 0.7))
    val_ratio = float(params.get("val_ratio", 0.15))
    test_ratio = float(params.get("test_ratio", 0.15))
    seed = int(params.get("seed", "42"))

    # ── Validate ratios sum to 1.0 ────────────────────────────────
    ratio_sum = round(train_ratio + val_ratio + test_ratio, 10)
    if abs(ratio_sum - 1.0) > 1e-6:
        raise ValueError(
            f"train_ratio + val_ratio + test_ratio must equal 1.0, got {ratio_sum:.4f} "
            f"({train_ratio} + {val_ratio} + {test_ratio})"
        )

    # ── Determine target column from params ────────────────────────
    target_col = params.get("target_column", "")
    if not target_col:
        raise ValueError(
            "No target column specified. Stratified split requires a target column "
            "for stratification. Set the 'target_column' parameter."
        )

    if target_col not in df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found in DataFrame. "
            f"Available columns: {df.columns}"
        )

    # ── Validate target is categorical/integer (not continuous) ───
    target_dtype = df[target_col].dtype
    is_float = target_dtype in (pl.Float32, pl.Float64)
    if is_float:
        # Check if it's actually integer-valued floats
        col = df[target_col].drop_nulls()
        if len(col) > 0 and not (col == col.cast(pl.Int64).cast(pl.Float64)).all():
            raise ValueError(
                f"Target column '{target_col}' has continuous float values (dtype={target_dtype}). "
                "Stratified splitting requires a categorical or integer target. "
                "Consider binning continuous targets before stratified splitting."
            )

    # ── Validate minimum 3 samples per class ──────────────────────
    class_counts = df[target_col].value_counts()
    # value_counts returns a DataFrame with columns [target_col, "count"]
    min_class = class_counts["count"].min()
    if min_class is not None and min_class < 3:
        small_classes = class_counts.filter(pl.col("count") < 3)
        raise ValueError(
            f"Stratified split requires at least 3 samples per class, but found classes "
            f"with fewer: {small_classes.to_dicts()}. Consider merging rare classes or "
            "using Random Hold-out instead."
        )

    # ── Two-step stratified split ─────────────────────────────────
    indices = list(range(len(df)))
    stratify = df[target_col].to_list()

    # Step 1: split off test set
    train_val_idx, test_idx = train_test_split(
        indices,
        test_size=test_ratio,
        random_state=seed,
        shuffle=True,
        stratify=stratify,
    )

    # Step 2: split train_val into train + val (if val_ratio > 0)
    if val_ratio > 0:
        val_fraction = val_ratio / (train_ratio + val_ratio)
        stratify_val = [stratify[i] for i in train_val_idx]
        train_idx, val_idx = train_test_split(
            train_val_idx,
            test_size=val_fraction,
            random_state=seed,
            shuffle=True,
            stratify=stratify_val,
        )
    else:
        train_idx = train_val_idx
        val_idx = []

    train_df = df[train_idx]
    val_df = df[val_idx] if val_idx else pl.DataFrame(schema=df.schema)
    test_df = df[test_idx]

    # ── Write outputs ─────────────────────────────────────────────
    train_path = _get_output_path("train")
    val_path = _get_output_path("val")
    test_path = _get_output_path("test")
    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    return {"train": str(train_path), "val": str(val_path), "test": str(test_path)}
