"""DateTime Encoder node — decompose datetime columns into numeric components."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Text, Toggle, node


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
        "column": Text(
            default="",
            description="DateTime column to decompose (empty = auto-detect from dtype)",
            placeholder="date_column",
        ),
        "components": Text(
            default="year,month,day,weekday",
            description="Components to extract (year, month, day, weekday, hour, minute)",
            placeholder="year,month,day,weekday",
        ),
        "drop_original": Toggle(
            default=True,
            description="Drop the original datetime column after decomposition",
        ),
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
        ),
    },
    label="DateTime Encoder",
    category="Transform",
    description="Decompose datetime columns into numeric components (year, month, day, weekday, hour, minute).",
    allowed_upstream={
        "train": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer",
            "scaler_transform", "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer",
            "scaler_transform", "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer",
            "scaler_transform", "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## DateTime Encoder

Decompose datetime columns into numeric components that ML models can use.

### Components
| Component | Description | Example |
|-----------|-------------|---------|
| **year** | Calendar year | 2023 |
| **month** | Month (1-12) | 6 |
| **day** | Day of month (1-31) | 15 |
| **weekday** | Day of week (1=Mon, 7=Sun) | 4 |
| **hour** | Hour (0-23) | 14 |
| **minute** | Minute (0-59) | 30 |

### When to use
- **Temporal patterns** — weekday captures weekly cycles, month captures seasonality
- **Before any ML model** — models can't use raw datetime objects
- **String date columns** — auto-parses ISO format strings

### Auto-detect (empty column param)
When the `column` parameter is empty, the node auto-detects datetime columns from:
- DataFrame columns with `Date` or `Datetime` dtype
- String columns with datetime-like names (date, datetime, timestamp, etc.)

### Edge cases
- **String columns** are auto-parsed as datetime (ISO format)
- **Non-datetime columns** raise an error
- **drop_original=True** removes the source column after extraction
""",
)
def datetime_encoder(inputs: dict, params: dict) -> dict:
    """Decompose datetime columns into numeric components."""
    from pathlib import Path

    import polars as pl

    _VALID = {"year", "month", "day", "weekday", "hour", "minute"}
    _DATETIME_NAME_HINTS = {"date", "datetime", "timestamp", "time", "created", "updated"}

    def _auto_detect_datetime_columns(df: pl.DataFrame) -> list[str]:
        cols: list[str] = []
        for col_name in df.columns:
            if df[col_name].dtype in (pl.Date, pl.Datetime):
                cols.append(col_name)
            elif df[col_name].dtype in (pl.Utf8, pl.String):
                if any(hint in col_name.lower() for hint in _DATETIME_NAME_HINTS):
                    cols.append(col_name)
        return cols

    # ── Read train data ──────────────────────────────────────────
    train_path = inputs["train"]
    train_df = pl.read_parquet(train_path)
    target_col = params.get("target_column", "")
    drop_original = params.get("drop_original", True)

    # ── Parse components ─────────────────────────────────────────
    components_raw = params.get("components", "year,month,day,weekday")
    components = [c.strip().lower() for c in components_raw.split(",") if c.strip()]
    for comp in components:
        if comp not in _VALID:
            raise ValueError(
                f"Unknown component '{comp}'. Valid: {sorted(_VALID)}"
            )

    # ── Resolve target columns ───────────────────────────────────
    column_param = params.get("column", "").strip()
    if column_param:
        target_columns = [column_param]
        # Validate
        if column_param not in train_df.columns:
            raise ValueError(
                f"Column '{column_param}' not found in data. "
                f"Available: {sorted(train_df.columns)}"
            )
    else:
        target_columns = _auto_detect_datetime_columns(train_df)
        if not target_columns:
            raise ValueError(
                "No datetime columns found — provide a column name explicitly."
            )

    # ── Apply decomposition to a DataFrame ───────────────────────
    def _apply(df: pl.DataFrame) -> pl.DataFrame:
        for col in target_columns:
            if col not in df.columns:
                continue

            series = df[col]

            # Parse string columns to datetime
            if series.dtype == pl.Utf8 or series.dtype == pl.String:
                try:
                    series = series.str.to_datetime()
                except Exception:
                    raise ValueError(
                        f"datetime_encoder: column '{col}' could not be parsed as datetime"
                    )
            elif series.dtype not in (pl.Date, pl.Datetime):
                raise ValueError(
                    f"datetime_encoder: column '{col}' is not a date/datetime type "
                    f"(got {series.dtype})"
                )

            # Cast Date to Datetime for dt accessor
            if series.dtype == pl.Date:
                series = series.cast(pl.Datetime)

            for comp in components:
                new_name = f"{col}_{comp}"
                if comp == "year":
                    df = df.with_columns(series.dt.year().cast(pl.Int32).alias(new_name))
                elif comp == "month":
                    df = df.with_columns(series.dt.month().cast(pl.Int32).alias(new_name))
                elif comp == "day":
                    df = df.with_columns(series.dt.day().cast(pl.Int32).alias(new_name))
                elif comp == "weekday":
                    df = df.with_columns(series.dt.weekday().cast(pl.Int32).alias(new_name))
                elif comp == "hour":
                    df = df.with_columns(series.dt.hour().cast(pl.Int32).alias(new_name))
                elif comp == "minute":
                    df = df.with_columns(series.dt.minute().cast(pl.Int32).alias(new_name))

            if drop_original:
                df = df.drop(col)

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

    return result
