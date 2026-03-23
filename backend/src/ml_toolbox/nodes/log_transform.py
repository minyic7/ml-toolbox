"""Log Transform node — apply log1p / signed-log / Yeo-Johnson to numeric columns."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Text, node


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
        "method": Select(
            options=["log1p", "signed_log", "yeo_johnson"],
            default="log1p",
            description="log1p: log(1+x), safe for x>=0. signed_log: sign(x)*log1p(|x|), handles negatives. yeo_johnson: auto power transform for near-normal output.",
        ),
        "columns": Text(
            default="",
            description="Columns to transform (comma-separated, empty = auto from EDA context)",
            placeholder="col1, col2, col3",
        ),
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
        ),
    },
    label="Log Transform",
    category="Transform",
    description="Apply log1p, signed-log, or Yeo-Johnson transform to numeric columns.",
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

Apply a log-family transform to reduce skew in numeric columns. Three methods available:

### Methods
| Method | Formula | Handles negatives? | Use case |
|--------|---------|-------------------|----------|
| `log1p` | log(1+x) | No (NaN for x<0) | Positive-only data (counts, amounts) |
| `signed_log` | sign(x) * log1p(|x|) | Yes | Data with negatives that carry meaning (e.g. credit card overpayments) |
| `yeo_johnson` | sklearn PowerTransformer | Yes | Auto-tune for near-normal output |

### When to use
- **Skewness > 1** (right-skewed distributions like income, transaction amounts)
- **Outlier-heavy columns** — compresses extreme values
- **Before linear models** — they assume normality

### Choosing a method
- **Only positive values** → `log1p` (simplest, most interpretable)
- **Has negative values with business meaning** → `signed_log` (preserves sign direction)
- **Pure modeling optimization** → `yeo_johnson` (auto-selects best power transform)

### Edge cases
- **log1p + negative values** → NaN (use signed_log or yeo_johnson instead)
- **Zero values** → log1p(0) = 0, signed_log(0) = 0 (both safe)
- **yeo_johnson** fits on train, applies same transform to val/test (no leakage)
""",
)
def log_transform(inputs: dict, params: dict) -> dict:
    """Apply log1p, signed-log, or Yeo-Johnson to selected columns in all splits."""
    import json
    import warnings
    from pathlib import Path

    import polars as pl

    _NUMERIC = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    method = params.get("method", "log1p")

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
        columns = available_numeric

    if not columns:
        raise ValueError("No columns to transform — provide columns or ensure numeric columns exist.")

    # ── Yeo-Johnson: fit on train, apply to all splits ───────────
    if method == "yeo_johnson":
        from sklearn.preprocessing import PowerTransformer

        pt = PowerTransformer(method="yeo-johnson", standardize=False)

        train_vals = train_df.select(columns).to_pandas()
        pt.fit(train_vals)

        def _apply_yj(df: pl.DataFrame) -> pl.DataFrame:
            vals = df.select(columns).to_pandas()
            transformed = pt.transform(vals)
            for i, col in enumerate(columns):
                df = df.with_columns(
                    pl.Series(name=col, values=transformed[:, i])
                )
            return df

        apply_fn = _apply_yj
    else:
        # ── log1p / signed_log ───────────────────────────────────
        def _apply_log(df: pl.DataFrame) -> pl.DataFrame:
            for col in columns:
                if col not in df.columns:
                    continue
                series = df[col].cast(pl.Float64)
                if method == "signed_log":
                    # sign(x) * log1p(|x|)
                    sign = series.sign()
                    transformed = sign * (series.abs() + 1.0).log()
                else:
                    # log1p
                    neg_count = (series < 0).sum()
                    if neg_count > 0:
                        warnings.warn(
                            f"log1p: column '{col}' has {neg_count} negative value(s) — "
                            f"these will become NaN. Consider using signed_log or yeo_johnson method.",
                            stacklevel=4,
                        )
                    transformed = (series + 1.0).log()
                df = df.with_columns(transformed.alias(col))
            return df

        apply_fn = _apply_log

    # ── Write split helper ───────────────────────────────────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        return str(out_path)

    # ── Process all splits ───────────────────────────────────────
    result: dict[str, str] = {}
    result["train"] = _write_split(apply_fn(train_df), "train")

    if inputs.get("val"):
        result["val"] = _write_split(apply_fn(pl.read_parquet(inputs["val"])), "val")

    if inputs.get("test"):
        result["test"] = _write_split(apply_fn(pl.read_parquet(inputs["test"])), "test")

    # ── Write transform summary sidecar ──────────────────────────
    auto_selected = not columns_param
    summary = {
        "method": method,
        "transformed_columns": columns,
        "skipped_columns": [c for c in available_numeric if c not in columns],
        "target_column": target_col,
        "auto_selected": auto_selected,
    }
    summary_path = _get_output_path("transform_summary", ext=".json")
    summary_path.write_text(json.dumps(summary, indent=2))

    return result
