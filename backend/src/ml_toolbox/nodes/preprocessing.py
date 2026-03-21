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
    outputs={"train": PortType.TABLE, "test": PortType.TABLE},
    params={
        "test_size": Slider(min=0.05, max=0.5, step=0.05, default=0.2,
                           description="Fraction of data reserved for the test set"),
        "random_seed": Slider(min=0, max=100, step=1, default=42,
                             description="Random seed for reproducible splits"),
        "stratify_column": Text(default="",
                               description="Column to stratify by (preserves class ratios in both splits)",
                               placeholder="target"),
        "shuffle": Toggle(default=True,
                         description="Shuffle data before splitting"),
    },
    label="Random Hold-out",
    category="Preprocessing",
    description="Split a DataFrame into train and test sets using random hold-out.",
    guide="""## Random Hold-out Split

The simplest data splitting strategy: randomly partition your dataset into two sets.

### How it works
```
train_test_split(X, y, test_size=0.2, random_state=42)
```

Your data is shuffled (unless disabled) and split into:
- **Train set** — used to fit the model
- **Test set** — held out for final evaluation

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
- **Class imbalance**: if your target has rare classes (e.g. 5% positive), use `stratify_column` to preserve ratios in both splits
- **Data leakage**: any preprocessing that uses statistics (mean, std, vocabulary) must be fit on train only, then applied to test

### Parameters
| Parameter | Purpose |
|-----------|--------|
| `test_size` | Fraction held out (0.2 = 20% test, 80% train) |
| `random_seed` | Fix this for reproducible results |
| `stratify_column` | Preserves class distribution across splits |
| `shuffle` | Disable for ordered data (but consider Time Series Split) |
""",
)
def random_holdout(inputs: dict, params: dict) -> dict:
    """Split a DataFrame into train and test sets using random hold-out."""
    import polars as pl
    from sklearn.model_selection import train_test_split

    df = pl.read_parquet(inputs["df"])

    test_size = float(params.get("test_size", 0.2))
    random_seed = int(params.get("random_seed", 42))
    stratify_col = params.get("stratify_column", "")
    shuffle = params.get("shuffle", True)

    stratify = df[stratify_col].to_list() if stratify_col else None

    train_idx, test_idx = train_test_split(
        range(len(df)),
        test_size=test_size,
        random_state=random_seed,
        shuffle=shuffle,
        stratify=stratify,
    )

    train_df = df[train_idx]
    test_df = df[test_idx]

    train_path = _get_output_path("train")
    test_path = _get_output_path("test")
    train_df.write_parquet(train_path)
    test_df.write_parquet(test_path)

    return {"train": str(train_path), "test": str(test_path)}
