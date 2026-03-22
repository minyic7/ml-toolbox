import json
from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Text, node


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
    inputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    outputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    params={
        "columns_to_drop": Text(
            default="",
            description="Comma-separated list of columns to remove from all splits",
            placeholder="id, name, timestamp",
        ),
    },
    label="Column Dropper",
    description="Drop selected columns from train/val/test splits. Target column is protected.",
    allowed_upstream={
        "train": ["random_holdout", "stratified_holdout"],
        "val": ["random_holdout", "stratified_holdout"],
        "test": ["random_holdout", "stratified_holdout"],
    },
    guide="""## Column Dropper

Remove unwanted columns from your dataset across all splits (train, validation, test).

### What it does
- Drops the specified columns from every connected split
- Updates `.meta.json` so downstream nodes see the correct schema
- **Protects the target column** — if you accidentally select the target, it is kept and a warning is printed

### When to use
- **Remove ID / index columns** that would leak row identity to the model
- **Drop high-cardinality categoricals** (e.g. names, free-text) that add noise
- **Remove redundant features** you identified during EDA (e.g. highly correlated pairs)
- **Exclude date/time columns** that need dedicated feature engineering first

### Inputs / Outputs
| Port | Required | Description |
|------|----------|-------------|
| train | Yes | Training split — always required |
| val | No | Validation split — processed identically if connected |
| test | No | Test split — processed identically if connected |

### Parameters
| Parameter | Description |
|-----------|-------------|
| `columns_to_drop` | Comma-separated column names (e.g. `id, name, timestamp`) |

### Target protection
The target column (read from `.meta.json`) is **never dropped**, even if listed in
`columns_to_drop`. A warning is printed instead. This prevents accidentally removing
the variable you are trying to predict.
""",
)
def column_dropper(inputs: dict, params: dict) -> dict:
    """Drop selected columns from train/val/test splits."""
    import json
    import warnings
    from pathlib import Path

    import polars as pl

    # ── Parse columns_to_drop ────────────────────────────────────
    raw = params.get("columns_to_drop", "")
    columns_to_drop = [c.strip() for c in raw.split(",") if c.strip()]

    if not columns_to_drop:
        raise ValueError("columns_to_drop is empty — select at least one column to drop.")

    # ── Read train (mandatory) ───────────────────────────────────
    train_path = Path(inputs["train"])
    train_df = pl.read_parquet(train_path)

    # ── Read .meta.json for target column ────────────────────────
    meta_path = train_path.with_suffix(".meta.json")
    meta = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
    target_col = meta.get("target", "")

    # ── Validate columns exist in schema ─────────────────────────
    schema_cols = set(train_df.columns)
    missing = [c for c in columns_to_drop if c not in schema_cols]
    if missing:
        raise ValueError(
            f"Columns not found in schema: {missing}. "
            f"Available columns: {sorted(schema_cols)}"
        )

    # ── Protect target column ────────────────────────────────────
    actual_drop = []
    for col in columns_to_drop:
        if col == target_col:
            warnings.warn(
                f"Target column '{target_col}' cannot be dropped — skipping it.",
                stacklevel=1,
            )
        else:
            actual_drop.append(col)

    if not actual_drop:
        raise ValueError("No columns to drop after excluding the protected target column.")

    # ── Helper: drop columns + write output + update meta ────────
    def _process_split(df: pl.DataFrame, split_name: str) -> str:
        out_df = df.drop(actual_drop)
        out_path = _get_output_path(split_name)
        out_df.write_parquet(out_path)

        # Write updated .meta.json (remove dropped columns)
        if meta:
            updated_meta = dict(meta)
            if "columns" in updated_meta:
                updated_meta["columns"] = {
                    k: v for k, v in updated_meta["columns"].items()
                    if k not in actual_drop
                }
            meta_out = Path(str(out_path)).with_suffix(".meta.json")
            meta_out.write_text(json.dumps(updated_meta, indent=2))

        return str(out_path)

    result: dict[str, str] = {}

    # ── Process train (mandatory) ────────────────────────────────
    result["train"] = _process_split(train_df, "train")

    # ── Process val (optional) ───────────────────────────────────
    if "val" in inputs:
        val_df = pl.read_parquet(inputs["val"])
        result["val"] = _process_split(val_df, "val")

    # ── Process test (optional) ──────────────────────────────────
    if "test" in inputs:
        test_df = pl.read_parquet(inputs["test"])
        result["test"] = _process_split(test_df, "test")

    return result


# ─────────────────────────────────────────────────────────────────────
# Missing Value Imputer
# ─────────────────────────────────────────────────────────────────────


@node(
    inputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    outputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    params={
        "strategy": Select(
            ["mean", "median", "mode", "constant"],
            default="mean",
            description="Imputation strategy: mean/median for numeric, mode for any type, constant for a fixed value",
        ),
        "constant_value": Text(
            default="",
            description="Fill value when strategy=constant",
            placeholder="0",
        ),
        "columns": Text(
            default="",
            description="Comma-separated columns to impute (empty = all columns with missing values)",
            placeholder="col1, col2, col3",
        ),
    },
    label="Missing Value Imputer",
    category="Transform",
    description="Fill missing values using statistics fitted on the train split only.",
    allowed_upstream={
        "train": ["random_holdout", "stratified_holdout", "column_dropper", "missing_value_imputer"],
        "val": ["random_holdout", "stratified_holdout", "column_dropper", "missing_value_imputer"],
        "test": ["random_holdout", "stratified_holdout", "column_dropper", "missing_value_imputer"],
    },
    guide="""## Missing Value Imputer

Fill missing (null) values so downstream models can train without errors.

### Why impute on train only?
Fill values (mean, median, mode) are **computed from the train split only**, then applied identically to val and test. This prevents **data leakage** — if you computed the mean using test data, your model would indirectly "see" test information during training, giving overly optimistic evaluation results.

### Choosing a strategy

| Strategy | Best for | Notes |
|----------|----------|-------|
| **mean** | Numeric columns with roughly symmetric distributions | Sensitive to outliers |
| **median** | Numeric columns with skewed distributions | Robust to outliers — usually the safer default |
| **mode** | Categorical columns (or any type) | Uses the most frequent value |
| **constant** | Known fill values (e.g. 0 for missing payments) | You control exactly what goes in |

### Edge cases
- Columns where **all** values are null are skipped (can't compute a statistic from nothing)
- Using mean/median on a non-numeric column is skipped with a warning
- The **target column** is never imputed (detected from .meta.json)
""",
)
def missing_value_imputer(inputs: dict, params: dict) -> dict:
    """Fill missing values using statistics fitted on the train split only."""
    import json
    import warnings

    import polars as pl

    strategy = params.get("strategy", "mean")
    constant_value = params.get("constant_value", "")
    columns_param = params.get("columns", "")

    # ── Read train split (mandatory) ─────────────────────────────
    train_path = Path(inputs["train"])
    train_df = pl.read_parquet(train_path)

    # ── Read .meta.json for target column ────────────────────────
    meta_path = train_path.with_suffix(".meta.json")
    meta: dict = {}
    target_column: str = ""
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            target_column = meta.get("target", "")
        except Exception:
            pass

    # ── Validate constant strategy ───────────────────────────────
    if strategy == "constant" and not constant_value:
        raise ValueError("constant_value must be provided when strategy=constant")

    # ── Determine columns to impute ──────────────────────────────
    if columns_param and columns_param.strip():
        candidate_cols = [c.strip() for c in columns_param.split(",") if c.strip()]
        candidate_cols = [c for c in candidate_cols if c in train_df.columns]
    else:
        candidate_cols = [
            c for c in train_df.columns if train_df[c].null_count() > 0
        ]

    # Exclude target column
    if target_column:
        candidate_cols = [c for c in candidate_cols if c != target_column]

    # ── Fit on train: compute fill values ────────────────────────
    fill_values: dict[str, object] = {}
    numeric_dtypes = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    for col in candidate_cols:
        series = train_df[col]
        non_null = series.drop_nulls()

        # Skip columns that are ALL null
        if len(non_null) == 0:
            warnings.warn(
                f"Column '{col}' has all null values in train — skipping imputation",
                stacklevel=2,
            )
            continue

        is_numeric = isinstance(series.dtype, numeric_dtypes)

        if strategy == "mean":
            if not is_numeric:
                warnings.warn(
                    f"Column '{col}' is not numeric — skipping mean imputation",
                    stacklevel=2,
                )
                continue
            fill_values[col] = non_null.mean()

        elif strategy == "median":
            if not is_numeric:
                warnings.warn(
                    f"Column '{col}' is not numeric — skipping median imputation",
                    stacklevel=2,
                )
                continue
            fill_values[col] = non_null.median()

        elif strategy == "mode":
            mode_val = non_null.mode()
            if len(mode_val) > 0:
                fill_values[col] = mode_val[0]

        elif strategy == "constant":
            if is_numeric:
                try:
                    fill_values[col] = float(constant_value)
                except ValueError:
                    fill_values[col] = constant_value
            else:
                fill_values[col] = constant_value

    # ── Transform helper ─────────────────────────────────────────
    def _apply_fill(df: pl.DataFrame) -> pl.DataFrame:
        if not fill_values or df.height == 0:
            return df
        exprs = []
        for col_name, val in fill_values.items():
            if col_name in df.columns:
                exprs.append(pl.col(col_name).fill_null(value=val))
        if exprs:
            df = df.with_columns(exprs)
        return df

    # ── Helper: write output + pass-through .meta.json ───────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        # Pass-through .meta.json (schema unchanged by imputation)
        if meta:
            meta_out = out_path.with_suffix(".meta.json")
            meta_out.write_text(json.dumps(meta, indent=2))
        return str(out_path)

    # ── Transform all splits ─────────────────────────────────────
    result: dict[str, str] = {}

    # Train (mandatory)
    result["train"] = _write_split(_apply_fill(train_df), "train")

    # Val (optional)
    if "val" in inputs and inputs["val"]:
        val_df = pl.read_parquet(inputs["val"])
        result["val"] = _write_split(_apply_fill(val_df), "val")

    # Test (optional)
    if "test" in inputs and inputs["test"]:
        test_df = pl.read_parquet(inputs["test"])
        result["test"] = _write_split(_apply_fill(test_df), "test")

    return result
