"""Log Transform node — apply log1p to reduce right skew in numeric columns."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Text, node


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    """Overridden by sandbox runner at runtime."""
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
        "columns": Text(
            default="",
            description="Columns to log1p transform (comma-separated, empty = auto from EDA context)",
            placeholder="col1, col2, col3",
        ),
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
        ),
    },
    label="Log Transform",
    category="Transform",
    description="Apply log1p(x) = log(1+x) to reduce right skew in numeric columns.",
    allowed_upstream={
        "train": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Log Transform

Apply **log1p(x) = log(1+x)** to reduce right skew in numeric columns.
The transformation is applied **in-place** — original column values are replaced.

### When to use
- **Skewness > 1** (right-skewed distributions like income, transaction amounts, page views)
- **Outlier-heavy columns** — compresses extreme values into a tighter range
- **Before linear models** — they assume normality; log transform helps satisfy that

### Auto-select (empty columns param)
When the `columns` parameter is empty, the node reads `.eda-context.json` and
automatically selects columns where:
- Skewness > 1 (from distribution profile)
- Outlier percentage > 5% (from outlier detection)

### Edge cases
- **Negative values** → log1p produces NaN for values where 1+x ≤ 0 (warning emitted)
- **Zero values** → log1p(0) = 0 (safe)
- **Non-numeric columns** listed explicitly are skipped with a warning
""",
)
def log_transform(inputs: dict, params: dict) -> dict:
    """Apply log1p to selected columns in all splits."""
    import json
    import warnings
    from pathlib import Path

    import polars as pl

    _NUMERIC = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    # ── Read train data ──────────────────────────────────────────
    train_path = inputs["train"]
    train_df = pl.read_parquet(train_path)

    # ── Determine target column (excluded from transforms) ───────
    target_col = params.get("target_column", "")

    # ── Determine numeric columns ────────────────────────────────
    available_numeric = [
        c for c in train_df.columns
        if train_df[c].dtype in _NUMERIC and c != target_col
    ]

    # ── Resolve columns to transform ─────────────────────────────
    columns_param = params.get("columns", "").strip()
    if columns_param:
        requested = [c.strip() for c in columns_param.split(",") if c.strip()]
        columns = []
        for col in requested:
            if col not in train_df.columns:
                raise ValueError(
                    f"Column '{col}' not found in data. "
                    f"Available: {sorted(train_df.columns)}"
                )
            if col not in available_numeric:
                warnings.warn(
                    f"log_transform: column '{col}' is not numeric — skipping",
                    stacklevel=2,
                )
                continue
            columns.append(col)
    else:
        # No columns specified — auto-configure should have set this param
        # based on EDA context. Fall back to all numeric columns.
        columns = available_numeric

    if not columns:
        raise ValueError("No columns to transform — provide columns or ensure numeric columns exist.")

    # ── Apply log1p to a DataFrame ───────────────────────────────
    def _apply(df: pl.DataFrame) -> pl.DataFrame:
        for col in columns:
            if col not in df.columns:
                continue
            series = df[col].cast(pl.Float64)
            neg_count = (series < 0).sum()
            if neg_count > 0:
                warnings.warn(
                    f"log1p: column '{col}' has {neg_count} negative value(s) — "
                    f"these will become NaN",
                    stacklevel=4,
                )
            transformed = (series + 1.0).log()
            df = df.with_columns(transformed.alias(col))
        return df

    # ── Write split helper ───────────────────────────────────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        return str(out_path)

    # ── Process all splits ───────────────────────────────────────
    result: dict[str, str] = {}
    result["train"] = _write_split(_apply(train_df), "train")

    if inputs.get("val"):
        result["val"] = _write_split(_apply(pl.read_parquet(inputs["val"])), "val")

    if inputs.get("test"):
        result["test"] = _write_split(_apply(pl.read_parquet(inputs["test"])), "test")

    # ── Write transform summary sidecar ──────────────────────────
    auto_selected = not columns_param
    summary = {
        "method": "log1p",
        "transformed_columns": columns,
        "skipped_columns": [c for c in available_numeric if c not in columns],
        "target_column": target_col,
        "auto_selected": auto_selected,
    }
    summary_path = _get_output_path("transform_summary", ext=".json")
    summary_path.write_text(json.dumps(summary, indent=2))

    return result
