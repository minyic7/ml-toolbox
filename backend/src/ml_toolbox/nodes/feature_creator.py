"""Feature Creator transform node — deterministic feature engineering operations."""

from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path

import polars as pl

from ml_toolbox.protocol import PortType, Text, node

logger = logging.getLogger(__name__)


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


# ── Operation types ─────────────────────────────────────────────────

_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)


def _parse_operations(raw: str) -> list[dict]:
    """Parse the DSL string into a list of operation dicts.

    Supported formats:
        log1p:COL
        ratio:COL_A:COL_B
        poly:COL:N
        interaction:COL_A:COL_B
        date:COL
    """
    ops: list[dict] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split(":")
        op_type = parts[0].strip().lower()

        if op_type == "log1p":
            if len(parts) < 2 or not parts[1].strip():
                raise ValueError(f"log1p requires a column name: 'log1p:COL', got '{token}'")
            ops.append({"type": "log1p", "col": parts[1].strip()})

        elif op_type == "ratio":
            if len(parts) < 3 or not parts[1].strip() or not parts[2].strip():
                raise ValueError(
                    f"ratio requires two column names: 'ratio:COL_A:COL_B', got '{token}'"
                )
            ops.append({"type": "ratio", "col_a": parts[1].strip(), "col_b": parts[2].strip()})

        elif op_type == "poly":
            if len(parts) < 3 or not parts[1].strip() or not parts[2].strip():
                raise ValueError(f"poly requires column and power: 'poly:COL:N', got '{token}'")
            try:
                power = int(parts[2].strip())
            except ValueError:
                raise ValueError(
                    f"poly power must be an integer: 'poly:COL:N', got '{token}'"
                )
            if power < 2:
                raise ValueError(f"poly power must be >= 2, got {power} in '{token}'")
            ops.append({"type": "poly", "col": parts[1].strip(), "power": power})

        elif op_type == "interaction":
            if len(parts) < 3 or not parts[1].strip() or not parts[2].strip():
                raise ValueError(
                    f"interaction requires two column names: 'interaction:COL_A:COL_B', got '{token}'"
                )
            ops.append({
                "type": "interaction",
                "col_a": parts[1].strip(),
                "col_b": parts[2].strip(),
            })

        elif op_type == "date":
            if len(parts) < 2 or not parts[1].strip():
                raise ValueError(f"date requires a column name: 'date:COL', got '{token}'")
            ops.append({"type": "date", "col": parts[1].strip()})

        else:
            raise ValueError(
                f"Unknown operation type '{op_type}'. "
                f"Supported: log1p, ratio, poly, interaction, date"
            )

    return ops


def _validate_columns(ops: list[dict], available: set[str]) -> None:
    """Raise ValueError if any referenced column is missing from the DataFrame."""
    for op in ops:
        cols: list[str] = []
        if "col" in op:
            cols.append(op["col"])
        if "col_a" in op:
            cols.append(op["col_a"])
        if "col_b" in op:
            cols.append(op["col_b"])
        for col in cols:
            if col not in available:
                raise ValueError(
                    f"Column '{col}' not found in data. "
                    f"Available columns: {sorted(available)}"
                )


def _apply_operations(
    df: pl.DataFrame,
    ops: list[dict],
) -> tuple[pl.DataFrame, list[dict]]:
    """Apply all operations to a DataFrame.

    Returns (transformed_df, new_column_metadata) where each metadata entry
    is {"name": ..., "dtype": ..., "source_op": ...}.
    """
    new_col_meta: list[dict] = []

    for op in ops:
        op_type = op["type"]

        if op_type == "log1p":
            col = op["col"]
            new_name = f"{col}_log1p"
            series = df[col].cast(pl.Float64)
            # Warn if any values are negative
            neg_count = (series < 0).sum()
            if neg_count > 0:
                warnings.warn(
                    f"log1p: column '{col}' has {neg_count} negative value(s) — "
                    f"these will become NaN",
                    stacklevel=3,
                )
            # log1p: for negative values, result is NaN
            # Polars log(1+x) for x < -1 produces NaN, for -1 < x < 0 produces negative
            # We use the natural log: ln(1 + x). Values where 1+x <= 0 become NaN.
            expr = (series + 1.0).log()
            df = df.with_columns(expr.alias(new_name))
            new_col_meta.append({
                "name": new_name, "dtype": "Float64",
                "semantic_type": "continuous", "role": "feature",
            })

        elif op_type == "ratio":
            col_a, col_b = op["col_a"], op["col_b"]
            new_name = f"{col_a}_div_{col_b}"
            a = df[col_a].cast(pl.Float64)
            b = df[col_b].cast(pl.Float64)
            # Division by zero produces inf/-inf in Polars; convert to NaN
            zero_count = (b == 0).sum()
            if zero_count > 0:
                warnings.warn(
                    f"ratio: column '{col_b}' has {zero_count} zero value(s) — "
                    f"division by zero will produce NaN",
                    stacklevel=3,
                )
            result = a / b
            # Replace inf/-inf with NaN
            result = (
                pl.when(result.is_infinite())
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(result)
                .alias(new_name)
            )
            df = df.with_columns(result)
            new_col_meta.append({
                "name": new_name, "dtype": "Float64",
                "semantic_type": "continuous", "role": "feature",
            })

        elif op_type == "poly":
            col = op["col"]
            power = op["power"]
            new_name = f"{col}_pow{power}"
            df = df.with_columns(
                (pl.col(col).cast(pl.Float64) ** power).alias(new_name)
            )
            new_col_meta.append({
                "name": new_name, "dtype": "Float64",
                "semantic_type": "continuous", "role": "feature",
            })

        elif op_type == "interaction":
            col_a, col_b = op["col_a"], op["col_b"]
            new_name = f"{col_a}_x_{col_b}"
            df = df.with_columns(
                (pl.col(col_a).cast(pl.Float64) * pl.col(col_b).cast(pl.Float64))
                .alias(new_name)
            )
            new_col_meta.append({
                "name": new_name, "dtype": "Float64",
                "semantic_type": "continuous", "role": "feature",
            })

        elif op_type == "date":
            col = op["col"]
            # Try to cast to datetime if not already
            series = df[col]
            if series.dtype == pl.Utf8:
                try:
                    series = series.str.to_datetime()
                except Exception:
                    raise ValueError(
                        f"date: column '{col}' could not be parsed as datetime"
                    )
            elif series.dtype not in (pl.Date, pl.Datetime):
                raise ValueError(
                    f"date: column '{col}' is not a date/datetime type "
                    f"(got {series.dtype})"
                )
            # If it's a Date type, temporarily cast to Datetime for dt accessor
            if series.dtype == pl.Date:
                series = series.cast(pl.Datetime)

            df = df.with_columns([
                series.dt.year().cast(pl.Int32).alias(f"{col}_year"),
                series.dt.month().cast(pl.Int32).alias(f"{col}_month"),
                series.dt.day().cast(pl.Int32).alias(f"{col}_day"),
                series.dt.weekday().cast(pl.Int32).alias(f"{col}_weekday"),
                series.dt.hour().cast(pl.Int32).alias(f"{col}_hour"),
            ])
            for suffix, stype in [
                ("year", "ordinal"), ("month", "ordinal"), ("day", "ordinal"),
                ("weekday", "ordinal"), ("hour", "ordinal"),
            ]:
                new_col_meta.append({
                    "name": f"{col}_{suffix}", "dtype": "Int32",
                    "semantic_type": stype, "role": "feature",
                })

    return df, new_col_meta


# ── Node registration ──────────────────────────────────────────────

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
        "operations": Text(
            default="",
            description=(
                "Comma-separated feature operations. Formats: "
                "log1p:COL, ratio:COL_A:COL_B, poly:COL:N, "
                "interaction:COL_A:COL_B, date:COL"
            ),
            placeholder="log1p:BILL_AMT1, ratio:BILL_AMT1:LIMIT_BAL, poly:AGE:2",
        ),
    },
    label="Feature Creator",
    category="Transform",
    description="Create new features using log, ratio, polynomial, interaction, and date decomposition operations.",
    allowed_upstream={
        "train": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer",
            "scaler_transform", "feature_creator",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer",
            "scaler_transform", "feature_creator",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer",
            "scaler_transform", "feature_creator",
        ],
    },
    guide="""## Feature Creator

Create new features from existing columns using common feature engineering operations.
All operations are **deterministic** and applied identically to every split — no fitting is needed.

### Operation types

| Operation | Syntax | Output column | Description |
|-----------|--------|---------------|-------------|
| **log1p** | `log1p:COL` | `COL_log1p` | Natural log of (1 + value). Useful for right-skewed distributions (e.g. monetary amounts, counts) |
| **ratio** | `ratio:COL_A:COL_B` | `COL_A_div_COL_B` | Division of two columns. Great for utilisation rates, per-unit metrics |
| **poly** | `poly:COL:N` | `COL_powN` | Raise column to the Nth power. Captures non-linear relationships |
| **interaction** | `interaction:COL_A:COL_B` | `COL_A_x_COL_B` | Multiply two columns. Models combined effects of features |
| **date** | `date:COL` | `COL_year`, `COL_month`, `COL_day`, `COL_weekday`, `COL_hour` | Decompose a datetime column into numeric parts |

### Example expressions
```
log1p:BILL_AMT1, log1p:BILL_AMT2, ratio:BILL_AMT1:LIMIT_BAL, poly:AGE:2
```

### When to use
- **log1p** — right-skewed data (income, transaction amounts, page views). Compresses the long tail so linear models can learn from it
- **ratio** — whenever the relationship between features matters more than their absolute values (e.g. debt-to-income ratio)
- **poly** — when you suspect a non-linear (quadratic, cubic) relationship with the target
- **interaction** — when two features have a combined effect (e.g. age × income)
- **date** — extract temporal patterns from datetime columns (seasonality, day-of-week effects)

### Edge cases
- **Division by zero** → NaN + warning
- **log of negative values** → NaN + warning
- **Column not found** → error
- Original columns are always preserved; new columns are appended
""",
)
def feature_creator(inputs: dict, params: dict) -> dict:
    """Create new features using log, ratio, polynomial, interaction, and date operations."""

    # ── Parse and validate operations ─────────────────────────────
    raw = params.get("operations", "")
    if not raw or not raw.strip():
        raise ValueError(
            "operations is empty — provide at least one operation "
            "(e.g. 'log1p:COL, ratio:COL_A:COL_B')"
        )

    ops = _parse_operations(raw)
    if not ops:
        raise ValueError("No valid operations parsed from the operations parameter.")

    # ── Read train (mandatory) ────────────────────────────────────
    train_path = Path(inputs["train"])
    train_df = pl.read_parquet(train_path)

    # ── Validate all referenced columns exist ─────────────────────
    _validate_columns(ops, set(train_df.columns))

    # ── Read .meta.json sidecar ───────────────────────────────────
    meta_path = train_path.with_suffix(".meta.json")
    meta: dict = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except Exception:
            pass

    # ── Apply operations to train to discover new column metadata ─
    train_out, new_col_meta = _apply_operations(train_df, ops)

    # ── Helper: write split + updated .meta.json ──────────────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)

        if meta:
            updated_meta = dict(meta)
            cols = dict(updated_meta.get("columns", {}))
            for cm in new_col_meta:
                cols[cm["name"]] = {
                    "dtype": cm["dtype"],
                    "semantic_type": cm.get("semantic_type", "continuous"),
                    "role": cm.get("role", "feature"),
                }
            updated_meta["columns"] = cols
            updated_meta["generated_by"] = "feature_creator"
            meta_out = out_path.with_suffix(".meta.json")
            meta_out.write_text(json.dumps(updated_meta, indent=2))

        return str(out_path)

    # ── Process all splits ────────────────────────────────────────
    result: dict[str, str] = {}
    result["train"] = _write_split(train_out, "train")

    if "val" in inputs and inputs["val"]:
        val_df = pl.read_parquet(inputs["val"])
        val_out, _ = _apply_operations(val_df, ops)
        result["val"] = _write_split(val_out, "val")

    if "test" in inputs and inputs["test"]:
        test_df = pl.read_parquet(inputs["test"])
        test_out, _ = _apply_operations(test_df, ops)
        result["test"] = _write_split(test_out, "test")

    return result
