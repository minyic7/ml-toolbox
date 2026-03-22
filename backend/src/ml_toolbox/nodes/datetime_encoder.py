"""DateTime Encoder node — decompose datetime columns into numeric components."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

from ml_toolbox.protocol import PortType, Text, Toggle, node

logger = logging.getLogger(__name__)


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    """Return the output path for a node artifact."""
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


def _read_meta(parquet_path: str) -> dict:
    """Read .meta.json sidecar for a parquet file, return {} on failure."""
    meta_path = Path(parquet_path).with_suffix(".meta.json")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except Exception:
            pass
    return {}


def _write_meta(parquet_path: str, metadata: dict) -> None:
    """Write .meta.json sidecar alongside a parquet file."""
    meta_path = Path(parquet_path).with_suffix(".meta.json")
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))


_VALID_COMPONENTS = {"year", "month", "day", "weekday", "hour", "minute"}


def _auto_detect_datetime_columns(meta: dict, df: pl.DataFrame) -> list[str]:
    """Find datetime columns from metadata semantic_type or DataFrame dtype."""
    cols: list[str] = []

    # Check metadata first
    col_meta = meta.get("columns", {})
    for col_name, info in col_meta.items():
        if col_name in df.columns and info.get("semantic_type") == "datetime":
            cols.append(col_name)

    # Also check DataFrame dtypes
    for col_name in df.columns:
        if col_name not in cols and df[col_name].dtype in (pl.Date, pl.Datetime):
            cols.append(col_name)

    return cols


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
            description="DateTime column to decompose (empty = auto-detect from metadata)",
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
- `.meta.json` where `semantic_type` is `"datetime"`
- DataFrame columns with `Date` or `Datetime` dtype

### Edge cases
- **String columns** are auto-parsed as datetime (ISO format)
- **Non-datetime columns** raise an error
- **drop_original=True** removes the source column after extraction
""",
)
def datetime_encoder(inputs: dict, params: dict) -> dict:
    """Decompose datetime columns into numeric components."""

    # ── Read train data ──────────────────────────────────────────
    train_path = inputs["train"]
    train_df = pl.read_parquet(train_path)
    meta = _read_meta(train_path)
    drop_original = params.get("drop_original", True)

    # ── Parse components ─────────────────────────────────────────
    components_raw = params.get("components", "year,month,day,weekday")
    components = [c.strip().lower() for c in components_raw.split(",") if c.strip()]
    for comp in components:
        if comp not in _VALID_COMPONENTS:
            raise ValueError(
                f"Unknown component '{comp}'. Valid: {sorted(_VALID_COMPONENTS)}"
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
        target_columns = _auto_detect_datetime_columns(meta, train_df)
        if not target_columns:
            raise ValueError(
                "No datetime columns found — provide a column name explicitly."
            )

    # ── Apply decomposition to a DataFrame ───────────────────────
    new_col_meta: list[dict] = []
    columns_to_drop: list[str] = []

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

    # Build metadata for new columns (compute once)
    for col in target_columns:
        for comp in components:
            new_col_meta.append({
                "name": f"{col}_{comp}",
                "dtype": "Int32",
                "semantic_type": "ordinal",
                "role": "feature",
            })
        if drop_original:
            columns_to_drop.append(col)

    # ── Write split helper ───────────────────────────────────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        if meta:
            updated = dict(meta)
            cols = dict(updated.get("columns", {}))
            # Add new component columns
            for cm in new_col_meta:
                cols[cm["name"]] = {
                    "dtype": cm["dtype"],
                    "semantic_type": cm["semantic_type"],
                    "role": cm["role"],
                }
            # Remove dropped columns from metadata
            for dropped in columns_to_drop:
                cols.pop(dropped, None)
            updated["columns"] = cols
            updated["generated_by"] = "datetime_encoder"
            _write_meta(str(out_path), updated)
        return str(out_path)

    # ── Process all splits ───────────────────────────────────────
    result: dict[str, str] = {}
    result["train"] = _write_split(_apply(train_df), "train")

    if inputs.get("val"):
        result["val"] = _write_split(_apply(pl.read_parquet(inputs["val"])), "val")

    if inputs.get("test"):
        result["test"] = _write_split(_apply(pl.read_parquet(inputs["test"])), "test")

    return result
