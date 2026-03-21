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
        "random_seed": Slider(min=0, max=100, step=1, default=42,
                             description="Random seed for reproducible splits"),
        "stratify_column": Text(default="",
                               description="Column to stratify by (preserves class ratios across all splits)",
                               placeholder="target"),
        "shuffle": Toggle(default=True,
                         description="Shuffle data before splitting"),
    },
    label="Random Hold-out",
    category="Preprocessing",
    description="Split a DataFrame into train, validation, and test sets using random hold-out.",
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
    random_seed = int(params.get("random_seed", 42))
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
