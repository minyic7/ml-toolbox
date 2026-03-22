"""Interaction Creator node — create new features from column pairs."""

from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path

import polars as pl

from ml_toolbox.protocol import PortType, Select, Text, node

logger = logging.getLogger(__name__)

_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)


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


def _parse_pairs(raw: str) -> list[tuple[str, str]]:
    """Parse 'A:B, C:D' format into list of column pairs."""
    pairs: list[tuple[str, str]] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split(":")
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(
                f"Invalid pair format '{token}' — expected 'COL_A:COL_B'"
            )
        pairs.append((parts[0].strip(), parts[1].strip()))
    return pairs


_OP_LABELS = {
    "multiply": "x",
    "ratio": "div",
    "add": "plus",
    "subtract": "minus",
}


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
        "pairs": Text(
            default="",
            description="Column pairs to combine (format: A:B, C:D — empty = auto from EDA context)",
            placeholder="col_a:col_b, col_c:col_d",
        ),
        "operation": Select(
            ["multiply", "ratio", "add", "subtract"],
            default="multiply",
            description="Operation to apply to each pair",
        ),
    },
    label="Interaction Creator",
    category="Transform",
    description="Create new features from column pairs using arithmetic operations.",
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
    guide="""## Interaction Creator

Create new features from column pairs using arithmetic operations.
New columns are **appended** with the naming pattern `{col_a}_{op}_{col_b}`.

### Operations
| Operation | Formula | Output name |
|-----------|---------|-------------|
| **multiply** | `A × B` | `A_x_B` |
| **ratio** | `A / B` | `A_div_B` |
| **add** | `A + B` | `A_plus_B` |
| **subtract** | `A - B` | `A_minus_B` |

### When to use
- When you suspect **non-linear relationships** between features
- **High correlation pairs** that might have predictive interactions
- Creating **ratio features** (e.g. debt-to-income, utilisation rate)

### Auto-select (empty pairs param)
When the `pairs` parameter is empty, the node reads `.eda-context.json` and
automatically selects high-correlation pairs from the correlation analysis.

### Edge cases
- **Division by zero** (ratio) → NaN + warning
- **Column not found** → error
""",
)
def interaction_creator(inputs: dict, params: dict) -> dict:
    """Create interaction features from column pairs."""
    import json
    import warnings
    from pathlib import Path

    import polars as pl

    _NUMERIC = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    _OP_MAP = {
        "multiply": "x",
        "ratio": "div",
        "add": "plus",
        "subtract": "minus",
    }

    def _read_meta(parquet_path: str) -> dict:
        meta_path = Path(parquet_path).with_suffix(".meta.json")
        if meta_path.exists():
            try:
                return json.loads(meta_path.read_text())
            except Exception:
                pass
        return {}

    def _write_meta(parquet_path: str, metadata: dict) -> None:
        meta_path = Path(parquet_path).with_suffix(".meta.json")
        meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

    def _parse_pairs(raw: str) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for token in raw.split(","):
            token = token.strip()
            if not token:
                continue
            parts = token.split(":")
            if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
                raise ValueError(
                    f"Invalid pair format '{token}' — expected 'COL_A:COL_B'"
                )
            pairs.append((parts[0].strip(), parts[1].strip()))
        return pairs

    # ── Read train data ──────────────────────────────────────────
    train_path = inputs["train"]
    train_df = pl.read_parquet(train_path)
    meta = _read_meta(train_path)
    operation = params.get("operation", "multiply")
    op_label = _OP_MAP[operation]

    # ── Determine target column ──────────────────────────────────
    target_col = None
    for _col_name, _col_meta in meta.get("columns", {}).items():
        if isinstance(_col_meta, dict) and _col_meta.get("role") == "target":
            target_col = _col_name
            break

    # ── Determine numeric columns ────────────────────────────────
    available_numeric = {
        c for c in train_df.columns
        if train_df[c].dtype in _NUMERIC and c != target_col
    }

    # ── Resolve pairs ────────────────────────────────────────────
    pairs_param = params.get("pairs", "").strip()
    if pairs_param:
        pairs = _parse_pairs(pairs_param)
        # Validate columns exist
        for col_a, col_b in pairs:
            for col in (col_a, col_b):
                if col not in train_df.columns:
                    raise ValueError(
                        f"Column '{col}' not found in data. "
                        f"Available: {sorted(train_df.columns)}"
                    )
    else:
        # No pairs specified — auto-configure should have set this param
        # based on EDA context (correlation analysis). Fail if still empty.
        raise ValueError(
            "No pairs specified. Run Correlation Matrix EDA first or "
            "provide pairs explicitly (format: A:B, C:D)."
        )

    # ── Apply interactions to a DataFrame ────────────────────────
    new_col_meta: list[dict] = []

    def _apply(df: pl.DataFrame) -> pl.DataFrame:
        for col_a, col_b in pairs:
            new_name = f"{col_a}_{op_label}_{col_b}"
            a = pl.col(col_a).cast(pl.Float64)
            b = pl.col(col_b).cast(pl.Float64)

            if operation == "multiply":
                expr = (a * b).alias(new_name)
            elif operation == "ratio":
                # Check for zeros
                zero_count = (df[col_b].cast(pl.Float64) == 0).sum()
                if zero_count > 0:
                    warnings.warn(
                        f"ratio: column '{col_b}' has {zero_count} zero value(s) — "
                        f"division by zero will produce NaN",
                        stacklevel=4,
                    )
                result = a / b
                expr = (
                    pl.when(result.is_infinite())
                    .then(pl.lit(None, dtype=pl.Float64))
                    .otherwise(result)
                    .alias(new_name)
                )
            elif operation == "add":
                expr = (a + b).alias(new_name)
            elif operation == "subtract":
                expr = (a - b).alias(new_name)
            else:
                raise ValueError(f"Unknown operation: {operation}")

            df = df.with_columns(expr)
        return df

    # Build metadata for new columns (only once, from pairs)
    for col_a, col_b in pairs:
        new_name = f"{col_a}_{op_label}_{col_b}"
        new_col_meta.append({
            "name": new_name,
            "dtype": "Float64",
            "semantic_type": "continuous",
            "role": "feature",
        })

    # ── Write split helper ───────────────────────────────────────
    def _write_split(df: pl.DataFrame, split_name: str) -> str:
        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        if meta:
            updated = dict(meta)
            cols = dict(updated.get("columns", {}))
            for cm in new_col_meta:
                cols[cm["name"]] = {
                    "dtype": cm["dtype"],
                    "semantic_type": cm["semantic_type"],
                    "role": cm["role"],
                }
            updated["columns"] = cols
            updated["generated_by"] = "interaction_creator"
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
