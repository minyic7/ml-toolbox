"""Scaler Transform node — fit on train, transform all splits."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

from ml_toolbox.protocol import PortType, Select, Text, node

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


@node(
    inputs={"train": PortType.TABLE, "val": PortType.TABLE, "test": PortType.TABLE},
    outputs={"train": PortType.TABLE, "val": PortType.TABLE, "test": PortType.TABLE},
    params={
        "method": Select(
            options=["StandardScaler", "MinMaxScaler", "RobustScaler"],
            default="StandardScaler",
            description="Scaling method to apply",
        ),
        "columns": Text(
            default="",
            description="Comma-separated column names to scale (empty = all numeric columns)",
            placeholder="col1, col2, col3",
        ),
    },
    label="Scaler Transform",
    category="Transform",
    description="Scale numeric features using StandardScaler, MinMaxScaler, or RobustScaler. Fits on train only, transforms all splits.",
    allowed_upstream={
        "train": [
            "random_holdout", "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Scaler Transform

Scale numeric features so they share a common range or distribution. **Always fits on the training set only** and applies the same transformation to validation and test sets to prevent data leakage.

### Methods

| Method | Formula | Best for |
|--------|---------|----------|
| **StandardScaler** | `(x - mean) / std` | Normally distributed data; most ML algorithms |
| **MinMaxScaler** | `(x - min) / (max - min)` | Bounded features; neural networks expecting [0, 1] input |
| **RobustScaler** | `(x - median) / IQR` | Data with outliers; median and IQR are robust to extremes |

### When to use
- **Before distance-based models** (KNN, SVM, k-means) — unscaled features with larger ranges dominate
- **Before gradient-based models** (neural nets, logistic regression) — scaling speeds convergence
- **Not needed for tree-based models** (random forest, XGBoost) — splits are invariant to monotonic transforms

### Parameters
| Parameter | Purpose |
|-----------|---------|
| `method` | Scaling algorithm to apply |
| `columns` | Which columns to scale (empty = all numeric columns, target excluded automatically) |

### Edge cases
- **Zero variance columns** are skipped (StandardScaler would produce NaN)
- **Non-numeric columns** listed in `columns` are skipped with a warning
- **MinMaxScaler with min=max** skips the column (division by zero)
""",
)
def scaler_transform(inputs: dict, params: dict) -> dict:
    """Scale numeric features — fit on train, transform all splits."""
    import json
    import logging
    from pathlib import Path

    import polars as pl

    _logger = logging.getLogger(__name__)

    numeric_dtypes = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    def _get_numeric_cols(df: pl.DataFrame, col_metadata: dict) -> list[str]:
        if col_metadata:
            numeric = []
            for name, meta in col_metadata.items():
                if name not in df.columns:
                    continue
                role = meta.get("role", "")
                if role in ("ignore", "identifier"):
                    continue
                dtype_str = meta.get("dtype", "").lower()
                if any(t in dtype_str for t in ("int", "float", "numeric", "decimal")):
                    numeric.append(name)
                elif df[name].dtype in numeric_dtypes:
                    numeric.append(name)
            return numeric
        return [name for name, dtype in zip(df.columns, df.dtypes) if dtype in numeric_dtypes]

    def _fit(train_df: pl.DataFrame, scale_cols: list[str], method: str) -> dict[str, dict]:
        fit: dict[str, dict] = {}
        for col in scale_cols:
            series = train_df[col].drop_nulls().cast(float)
            if method == "StandardScaler":
                mean = series.mean()
                std = series.std()
                if std is None or std == 0:
                    _logger.warning("Column '%s' has zero variance — skipping (StandardScaler)", col)
                    continue
                fit[col] = {"mean": float(mean), "std": float(std)}  # type: ignore[arg-type]
            elif method == "MinMaxScaler":
                mn = series.min()
                mx = series.max()
                if mn == mx:
                    _logger.warning("Column '%s' has min==max — skipping (MinMaxScaler)", col)
                    continue
                fit[col] = {"min": float(mn), "max": float(mx)}  # type: ignore[arg-type]
            elif method == "RobustScaler":
                median = series.median()
                q25 = series.quantile(0.25, interpolation="linear")
                q75 = series.quantile(0.75, interpolation="linear")
                iqr = float(q75) - float(q25)  # type: ignore[arg-type]
                if iqr == 0:
                    _logger.warning("Column '%s' has zero IQR — skipping (RobustScaler)", col)
                    continue
                fit[col] = {"median": float(median), "iqr": iqr}  # type: ignore[arg-type]
        return fit

    def _transform_df(df: pl.DataFrame, fit_params: dict[str, dict], method: str) -> pl.DataFrame:
        exprs: list[pl.Expr] = []
        for col, fp in fit_params.items():
            if col not in df.columns:
                continue
            if method == "StandardScaler":
                exprs.append(
                    ((pl.col(col).cast(pl.Float64) - fp["mean"]) / fp["std"]).alias(col)
                )
            elif method == "MinMaxScaler":
                exprs.append(
                    ((pl.col(col).cast(pl.Float64) - fp["min"]) / (fp["max"] - fp["min"])).alias(col)
                )
            elif method == "RobustScaler":
                exprs.append(
                    ((pl.col(col).cast(pl.Float64) - fp["median"]) / fp["iqr"]).alias(col)
                )
        if exprs:
            df = df.with_columns(exprs)
        return df

    def _write_meta_sidecar(
        output_path: Path, col_metadata: dict, fit_params: dict[str, dict], target_col: str | None,
    ) -> None:
        if not col_metadata:
            return
        updated = {}
        for name, meta in col_metadata.items():
            entry = dict(meta)
            if name in fit_params:
                entry["dtype"] = "Float64"
            updated[name] = entry
        sidecar = {"columns": updated, "generated_by": "scaler_transform"}
        if target_col:
            sidecar["target"] = target_col
        meta_out = output_path.with_suffix(".meta.json")
        meta_out.write_text(json.dumps(sidecar, indent=2))

    method = params.get("method", "StandardScaler")
    columns_param = params.get("columns", "")

    # ── Read train data ──────────────────────────────────────────
    train_df = pl.read_parquet(inputs["train"])

    # ── Read .meta.json sidecar ──────────────────────────────────
    meta_path = Path(inputs["train"]).with_suffix(".meta.json")
    col_metadata: dict = {}
    target_col: str | None = None
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            col_metadata = meta.get("columns", {})
            for _cn, _cm in col_metadata.items():
                if isinstance(_cm, dict) and _cm.get("role") == "target":
                    target_col = _cn
                    break
        except Exception:
            pass

    # ── Determine columns to scale ───────────────────────────────
    if columns_param.strip():
        requested = [c.strip() for c in columns_param.split(",") if c.strip()]
    else:
        requested = _get_numeric_cols(train_df, col_metadata)

    # Filter out target column and non-numeric columns
    scale_cols: list[str] = []
    for col in requested:
        if col == target_col:
            continue
        if col not in train_df.columns:
            _logger.warning("Column '%s' not found in data — skipping", col)
            continue
        if train_df[col].dtype not in numeric_dtypes:
            _logger.warning("Column '%s' is not numeric (%s) — skipping", col, train_df[col].dtype)
            continue
        scale_cols.append(col)

    # ── Fit on train only ────────────────────────────────────────
    fit_params = _fit(train_df, scale_cols, method)

    # ── Transform all splits ─────────────────────────────────────
    results: dict[str, str] = {}
    for split_name in ("train", "val", "test"):
        input_path = inputs.get(split_name)
        if not input_path:
            continue

        split_path = Path(input_path)
        if not split_path.exists():
            continue

        df = pl.read_parquet(split_path) if split_name != "train" else train_df
        if df.height > 0:
            df = _transform_df(df, fit_params, method)

        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        results[split_name] = str(out_path)

        _write_meta_sidecar(out_path, col_metadata, fit_params, target_col)

    return results


def _get_numeric_columns(
    df: pl.DataFrame,
    col_metadata: dict,
) -> list[str]:
    """Return numeric column names from metadata or DataFrame schema."""
    numeric_dtypes = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    if col_metadata:
        # Use metadata to identify numeric columns
        numeric = []
        for name, meta in col_metadata.items():
            if name not in df.columns:
                continue
            role = meta.get("role", "")
            if role in ("ignore", "identifier"):
                continue
            dtype_str = meta.get("dtype", "").lower()
            if any(t in dtype_str for t in ("int", "float", "numeric", "decimal")):
                numeric.append(name)
            elif df[name].dtype in numeric_dtypes:
                numeric.append(name)
        return numeric

    return [name for name, dtype in zip(df.columns, df.dtypes) if dtype in numeric_dtypes]


def _fit(
    train_df: pl.DataFrame,
    scale_cols: list[str],
    method: str,
) -> dict[str, dict]:
    """Compute scaling parameters from the training set.

    Returns a dict mapping column name → fit params.
    Columns that would cause division by zero are excluded with a warning.
    """
    fit: dict[str, dict] = {}

    for col in scale_cols:
        series = train_df[col].drop_nulls().cast(float)

        if method == "StandardScaler":
            mean = series.mean()
            std = series.std()
            if std is None or std == 0:
                logger.warning("Column '%s' has zero variance — skipping (StandardScaler)", col)
                continue
            fit[col] = {"mean": float(mean), "std": float(std)}  # type: ignore[arg-type]

        elif method == "MinMaxScaler":
            mn = series.min()
            mx = series.max()
            if mn == mx:
                logger.warning("Column '%s' has min==max — skipping (MinMaxScaler)", col)
                continue
            fit[col] = {"min": float(mn), "max": float(mx)}  # type: ignore[arg-type]

        elif method == "RobustScaler":
            median = series.median()
            q25 = series.quantile(0.25, interpolation="linear")
            q75 = series.quantile(0.75, interpolation="linear")
            iqr = float(q75) - float(q25)  # type: ignore[arg-type]
            if iqr == 0:
                logger.warning("Column '%s' has zero IQR — skipping (RobustScaler)", col)
                continue
            fit[col] = {"median": float(median), "iqr": iqr}  # type: ignore[arg-type]

    return fit


def _transform(
    df: pl.DataFrame,
    fit_params: dict[str, dict],
    method: str,
) -> pl.DataFrame:
    """Apply scaling to a DataFrame using pre-computed fit parameters."""
    exprs: list[pl.Expr] = []
    for col, fp in fit_params.items():
        if col not in df.columns:
            continue

        if method == "StandardScaler":
            exprs.append(
                ((pl.col(col).cast(pl.Float64) - fp["mean"]) / fp["std"]).alias(col)
            )
        elif method == "MinMaxScaler":
            exprs.append(
                ((pl.col(col).cast(pl.Float64) - fp["min"]) / (fp["max"] - fp["min"])).alias(col)
            )
        elif method == "RobustScaler":
            exprs.append(
                ((pl.col(col).cast(pl.Float64) - fp["median"]) / fp["iqr"]).alias(col)
            )

    if exprs:
        df = df.with_columns(exprs)

    return df


def _write_meta_sidecar(
    output_path: Path,
    col_metadata: dict,
    fit_params: dict[str, dict],
    target_col: str | None,
) -> None:
    """Write .meta.json sidecar, updating dtype to Float64 for scaled columns."""
    if not col_metadata:
        return

    updated = {}
    for name, meta in col_metadata.items():
        entry = dict(meta)
        if name in fit_params:
            entry["dtype"] = "Float64"
        updated[name] = entry

    sidecar = {
        "columns": updated,
        "generated_by": "scaler_transform",
    }
    if target_col:
        sidecar["target"] = target_col

    meta_out = output_path.with_suffix(".meta.json")
    meta_out.write_text(json.dumps(sidecar, indent=2))
