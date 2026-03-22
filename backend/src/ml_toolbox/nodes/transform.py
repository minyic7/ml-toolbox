"""Transform nodes — data transformation operations."""

from __future__ import annotations

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


# ─── Column Dropper ─────────────────────────────────────────────────

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
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
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
The target column (from `target_column` param, auto-configured) is **never dropped**, even if listed in
`columns_to_drop`. A warning is printed instead. This prevents accidentally removing
the variable you are trying to predict.
""",
)
def column_dropper(inputs: dict, params: dict) -> dict:
    """Drop selected columns from train/val/test splits."""
    import warnings

    import polars as pl

    # ── Parse columns_to_drop ────────────────────────────────────
    raw = params.get("columns_to_drop", "")
    columns_to_drop = [c.strip() for c in raw.split(",") if c.strip()]

    if not columns_to_drop:
        raise ValueError("columns_to_drop is empty — select at least one column to drop.")

    # ── Read train (mandatory) ───────────────────────────────────
    train_df = pl.read_parquet(inputs["train"])

    # ── Target column from params (auto-configured upstream) ─────
    target_col = params.get("target_column", "")

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

    # ── Helper: drop columns + write output ──────────────────────
    def _process_split(df: pl.DataFrame, split_name: str) -> str:
        out_df = df.drop(actual_drop)
        out_path = _get_output_path(split_name)
        out_df.write_parquet(out_path)
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
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
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
- The **target column** is never imputed (from `target_column` param, auto-configured)
""",
)
def missing_value_imputer(inputs: dict, params: dict) -> dict:
    """Fill missing values using statistics fitted on the train split only."""
    import warnings

    import polars as pl

    strategy = params.get("strategy", "mean")
    constant_value = params.get("constant_value", "")
    columns_param = params.get("columns", "")

    # ── Read train split (mandatory) ─────────────────────────────
    train_df = pl.read_parquet(inputs["train"])

    # ── Target column from params (auto-configured upstream) ─────
    target_column = params.get("target_column", "")

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

    # ── Helper: write output ─────────────────────────────────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
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


# ─── Category Encoder ───────────────────────────────────────────────

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
        "method": Select(
            ["label", "ordinal", "one_hot"],
            default="label",
            description="Encoding method: label (arbitrary ints), ordinal (ordered ints), one-hot (binary columns)",
        ),
        "columns": Text(
            default="",
            description="Comma-separated columns to encode (empty = auto-detect by dtype)",
            placeholder="color, size, brand",
        ),
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
        ),
        "handle_unknown": Select(
            ["encode_as_unknown", "error"],
            default="encode_as_unknown",
            description="How to handle unseen categories in val/test sets",
        ),
    },
    label="Category Encoder",
    category="Transform",
    description="Encode categorical columns using label, ordinal, or one-hot encoding.",
    allowed_upstream={
        "train": ["random_holdout", "stratified_holdout", "clean_data", "feature_engineering",
                  "category_encoder", "column_dropper", "compute_stats",
                  "missing_value_imputer", "scaler"],
        "val": ["random_holdout", "stratified_holdout", "clean_data", "feature_engineering",
                "category_encoder", "column_dropper", "compute_stats",
                "missing_value_imputer", "scaler"],
        "test": ["random_holdout", "stratified_holdout", "clean_data", "feature_engineering",
                 "category_encoder", "column_dropper", "compute_stats",
                 "missing_value_imputer", "scaler"],
    },
    guide="""## Category Encoder

Encode categorical (string) columns into numeric values so ML models can use them.

### Methods

| Method | Output | Best for |
|--------|--------|----------|
| **Label** | Single int column per feature | Tree models (RF, XGBoost) — no ordering assumed |
| **Ordinal** | Single int column per feature | Ordered categories (size: S=0, M=1, L=2) |
| **One-hot** | N binary columns per feature | Linear models, neural nets — no ordinal bias |

### How it works
1. **Fit on train**: learn the mapping (value → int or value → column)
2. **Transform all splits**: apply the same mapping to train, val, and test
3. **Unseen categories**: val/test may have values not seen in train
   - `encode_as_unknown`: maps to -1 (label/ordinal) or all-zero row (one-hot)
   - `error`: raises an error with the unseen value — useful for catching data issues

### When to use what
- **Label encoding** for tree-based models — they split on thresholds, so arbitrary ints are fine
- **Ordinal encoding** when categories have a natural order (e.g., education level, size)
- **One-hot encoding** for linear models — avoids implying false ordinal relationships

### Edge cases
- **Target column** is automatically skipped (never encoded)
- **Empty category set** (column has no values) → skipped
- **High cardinality** with one-hot creates many columns — consider label encoding instead

### Parameters
| Parameter | Purpose |
|-----------|---------|
| `method` | Encoding strategy |
| `columns` | Which columns to encode (empty = auto-detect by dtype) |
| `handle_unknown` | What to do with unseen categories in val/test |
""",
)
def category_encoder(inputs: dict, params: dict) -> dict:
    """Encode categorical columns using label, ordinal, or one-hot encoding."""
    import polars as pl

    def encode_label_ordinal(
        inputs: dict, train_df: pl.DataFrame, cat_columns: list[str], handle_unknown: str,
    ) -> dict:
        mappings: dict[str, dict[str, int]] = {}
        for col in cat_columns:
            unique_vals = sorted(train_df[col].drop_nulls().unique().to_list(), key=str)
            mappings[col] = {str(v): i for i, v in enumerate(unique_vals)}

        def apply_mapping(df: pl.DataFrame, split_name: str) -> pl.DataFrame:
            for col in cat_columns:
                mapping = mappings[col]
                col_values = df[col].cast(pl.Utf8)
                if handle_unknown == "error" and split_name != "train":
                    for v in col_values.drop_nulls().unique().to_list():
                        if v not in mapping:
                            raise ValueError(
                                f"Unseen category '{v}' in column '{col}' of {split_name} set"
                            )
                old = pl.Series(list(mapping.keys()))
                new = pl.Series(list(mapping.values()))
                encoded = col_values.replace_strict(old, new, default=-1).cast(pl.Int64)
                df = df.with_columns(encoded.alias(col))
            return df

        train_encoded = apply_mapping(train_df, "train")
        train_path = _get_output_path("train")
        train_encoded.write_parquet(train_path)
        result: dict[str, str] = {"train": str(train_path)}

        if "val" in inputs:
            val_df = pl.read_parquet(inputs["val"])
            val_encoded = apply_mapping(val_df, "val") if val_df.height > 0 else val_df
            val_path = _get_output_path("val")
            val_encoded.write_parquet(val_path)
            result["val"] = str(val_path)

        if "test" in inputs:
            test_df = pl.read_parquet(inputs["test"])
            test_encoded = apply_mapping(test_df, "test") if test_df.height > 0 else test_df
            test_path = _get_output_path("test")
            test_encoded.write_parquet(test_path)
            result["test"] = str(test_path)

        return result

    def encode_onehot(
        inputs: dict, train_df: pl.DataFrame, cat_columns: list[str], handle_unknown: str,
    ) -> dict:
        category_sets: dict[str, list[str]] = {}
        for col in cat_columns:
            category_sets[col] = sorted(
                [str(v) for v in train_df[col].drop_nulls().unique().to_list()]
            )

        def apply_onehot(df: pl.DataFrame, split_name: str) -> pl.DataFrame:
            for col in cat_columns:
                categories = category_sets[col]
                col_values = df[col].cast(pl.Utf8)
                if handle_unknown == "error" and split_name != "train":
                    for v in col_values.drop_nulls().unique().to_list():
                        if v not in categories:
                            raise ValueError(
                                f"Unseen category '{v}' in column '{col}' of {split_name} set"
                            )
                new_cols = [
                    (pl.col(col).cast(pl.Utf8) == cat_value)
                    .cast(pl.Int64)
                    .alias(f"{col}_{cat_value}")
                    for cat_value in categories
                ]
                df = df.with_columns(new_cols).drop(col)
            return df

        train_encoded = apply_onehot(train_df, "train")
        train_path = _get_output_path("train")
        train_encoded.write_parquet(train_path)
        result: dict[str, str] = {"train": str(train_path)}

        if "val" in inputs:
            val_df = pl.read_parquet(inputs["val"])
            val_encoded = apply_onehot(val_df, "val") if val_df.height > 0 else val_df
            val_path = _get_output_path("val")
            val_encoded.write_parquet(val_path)
            result["val"] = str(val_path)

        if "test" in inputs:
            test_df = pl.read_parquet(inputs["test"])
            test_encoded = apply_onehot(test_df, "test") if test_df.height > 0 else test_df
            test_path = _get_output_path("test")
            test_encoded.write_parquet(test_path)
            result["test"] = str(test_path)

        return result

    method = params.get("method", "label")
    columns_param = params.get("columns", "")
    handle_unknown = params.get("handle_unknown", "encode_as_unknown")
    target_col = params.get("target_column", "")

    train_df = pl.read_parquet(inputs["train"])

    if columns_param and columns_param.strip():
        cat_columns = [c.strip() for c in columns_param.split(",") if c.strip()]
        cat_columns = [c for c in cat_columns if c in train_df.columns and c != target_col]
    else:
        cat_dtypes = ("Object", "String", "Utf8", "Categorical", "Enum")
        cat_columns = [
            c for c in train_df.columns
            if str(train_df[c].dtype) in cat_dtypes and c != target_col
        ]

    cat_columns = [c for c in cat_columns if train_df[c].drop_nulls().n_unique() > 0]

    if not cat_columns:
        train_path = _get_output_path("train")
        train_df.write_parquet(train_path)
        result: dict[str, str] = {"train": str(train_path)}
        if "val" in inputs:
            val_path = _get_output_path("val")
            pl.read_parquet(inputs["val"]).write_parquet(val_path)
            result["val"] = str(val_path)
        if "test" in inputs:
            test_path = _get_output_path("test")
            pl.read_parquet(inputs["test"]).write_parquet(test_path)
            result["test"] = str(test_path)
        return result

    if method in ("label", "ordinal"):
        return encode_label_ordinal(inputs, train_df, cat_columns, handle_unknown)
    else:
        return encode_onehot(inputs, train_df, cat_columns, handle_unknown)
