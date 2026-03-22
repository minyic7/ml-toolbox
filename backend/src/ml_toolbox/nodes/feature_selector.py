"""Feature Selector transform node — fit on train, apply to all splits."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ml_toolbox.protocol import PortType, Select, Slider, Text, node

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
            options=["variance_threshold", "correlation_with_target", "mutual_information"],
            default="variance_threshold",
            description="Feature selection method",
        ),
        "threshold": Slider(
            min=0.0,
            max=1.0,
            step=0.01,
            default=0.01,
            description="Selection threshold — features scoring below this are removed",
        ),
        "target_column": Text(
            default="",
            description="Target column (auto-detected from schema)",
        ),
    },
    label="Feature Selector",
    category="Transform",
    description="Select features using variance, correlation, or mutual information. Fits on train only.",
    allowed_upstream={
        "train": [
            "random_holdout", "stratified_holdout", "column_dropper",
            "missing_value_imputer", "category_encoder", "scaler_transform",
            "feature_selector",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout", "column_dropper",
            "missing_value_imputer", "category_encoder", "scaler_transform",
            "feature_selector",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout", "column_dropper",
            "missing_value_imputer", "category_encoder", "scaler_transform",
            "feature_selector",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Feature Selector

Remove low-value features to reduce noise and speed up training. **Fits on the training set only** and drops the same columns from all splits.

### Methods

| Method | What it measures | Best for |
|--------|-----------------|----------|
| **Variance Threshold** | Column variance — removes near-constant features | Cleaning up after scaling/encoding; fast, no target needed |
| **Correlation with Target** | |Pearson correlation| with the target column | Quick linear-relationship filter; works best on numeric features |
| **Mutual Information** | MI score (entropy-based) with the target column | Capturing non-linear relationships; more robust than correlation |

### When to use
- **After Scaler/Encoder** — variance threshold on unscaled data gives misleading results (a feature in [0,1] looks low-variance vs one in [0,1000])
- **Before training** — fewer features = faster training, less overfitting, easier interpretation

### Parameters
| Parameter | Purpose |
|-----------|---------|
| `method` | Selection algorithm |
| `threshold` | Features scoring **below** this value are dropped (0–1, default 0.01) |

### Edge cases
- **Target column** is never removed, even if it scores below the threshold
- **All features below threshold** → error (can't remove everything)
- **Correlation / MI methods require a target column** — fails fast if `target_column` param is not set
""",
)
def feature_selector(inputs: dict, params: dict) -> dict:
    """Select features — fit on train, drop from all splits."""
    import logging
    import math
    from pathlib import Path

    import polars as pl

    _logger = logging.getLogger(__name__)

    numeric_dtypes = (
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
    )

    def _pearson_corr(df: pl.DataFrame, col_a: str, col_b: str) -> float:
        clean = df.select([col_a, col_b]).drop_nulls()
        if clean.height < 2:
            return 0.0
        corr = clean.select(pl.corr(col_a, col_b)).item()
        if corr is None or math.isnan(corr):
            return 0.0
        return float(corr)

    def _mutual_information(x: object, y: object) -> float:
        import numpy as np

        x_arr = np.asarray(x, dtype=np.float64)
        y_arr = np.asarray(y, dtype=np.float64)
        valid = np.isfinite(x_arr) & np.isfinite(y_arr)
        x_arr = x_arr[valid]
        y_arr = y_arr[valid]
        if len(x_arr) < 2:
            return 0.0
        n_bins = min(20, max(2, int(np.sqrt(len(x_arr)))))
        try:
            x_binned = np.digitize(x_arr, np.unique(np.quantile(x_arr, np.linspace(0, 1, n_bins + 1)[1:-1])))
        except Exception:
            return 0.0
        try:
            y_binned = np.digitize(y_arr, np.unique(np.quantile(y_arr, np.linspace(0, 1, n_bins + 1)[1:-1])))
        except Exception:
            return 0.0
        n = len(x_arr)
        joint: dict[tuple[int, int], int] = {}
        for xi, yi in zip(x_binned, y_binned):
            key = (int(xi), int(yi))
            joint[key] = joint.get(key, 0) + 1
        x_marginal: dict[int, int] = {}
        y_marginal: dict[int, int] = {}
        for (xi, yi), count in joint.items():
            x_marginal[xi] = x_marginal.get(xi, 0) + count
            y_marginal[yi] = y_marginal.get(yi, 0) + count
        mi = 0.0
        for (xi, yi), count in joint.items():
            p_xy = count / n
            p_x = x_marginal[xi] / n
            p_y = y_marginal[yi] / n
            if p_xy > 0 and p_x > 0 and p_y > 0:
                mi += p_xy * np.log(p_xy / (p_x * p_y))
        return max(0.0, float(mi))

    def _fit_selector(
        train_df: pl.DataFrame, feature_cols: list[str],
        target_col: str, method: str, threshold: float,
    ) -> list[str]:
        cols_to_drop: list[str] = []
        if method == "variance_threshold":
            for col in feature_cols:
                series = train_df[col].drop_nulls().cast(pl.Float64)
                if len(series) == 0:
                    cols_to_drop.append(col)
                    continue
                variance = float(series.var())  # type: ignore[arg-type]
                if variance <= threshold:
                    _logger.info("Feature '%s' variance=%.6f <= threshold=%.4f — dropping", col, variance or 0, threshold)
                    cols_to_drop.append(col)
        elif method == "correlation_with_target":
            for col in feature_cols:
                series = train_df[col].drop_nulls().cast(pl.Float64)
                if len(series) == 0:
                    cols_to_drop.append(col)
                    continue
                corr = abs(_pearson_corr(train_df, col, target_col))
                if corr < threshold:
                    _logger.info("Feature '%s' |corr|=%.6f < threshold=%.4f — dropping", col, corr, threshold)
                    cols_to_drop.append(col)
        elif method == "mutual_information":
            for col in feature_cols:
                series = train_df[col].cast(pl.Float64)
                mask = series.is_not_null() & train_df[target_col].is_not_null()
                feat_clean = series.filter(mask).to_numpy()
                targ_clean = train_df[target_col].cast(pl.Float64).filter(mask).to_numpy()
                if len(feat_clean) == 0:
                    cols_to_drop.append(col)
                    continue
                mi = _mutual_information(feat_clean, targ_clean)
                if mi < threshold:
                    _logger.info("Feature '%s' MI=%.6f < threshold=%.4f — dropping", col, mi, threshold)
                    cols_to_drop.append(col)
        return cols_to_drop

    method = params.get("method", "variance_threshold")
    threshold = float(params.get("threshold", 0.01))

    # ── Read train data ──────────────────────────────────────────
    train_df = pl.read_parquet(inputs["train"])
    target_col = params.get("target_column", "")

    # ── Identify numeric feature columns ─────────────────────────
    feature_cols = [
        c for c in train_df.columns
        if c != target_col and train_df[c].dtype in numeric_dtypes
    ]

    # ── Validate target for methods that need it ─────────────────
    if method in ("correlation_with_target", "mutual_information"):
        if not target_col:
            raise ValueError(
                f"Method '{method}' requires a target column, but none was provided. "
                "Set the 'target_column' parameter or ensure auto-configure detects it."
            )
        if target_col not in train_df.columns:
            raise ValueError(
                f"Target column '{target_col}' not found in data."
            )

    # ── Fit: compute scores on train ─────────────────────────────
    cols_to_drop = _fit_selector(train_df, feature_cols, target_col, method, threshold)

    # ── Validate: can't drop everything ──────────────────────────
    remaining_features = [c for c in feature_cols if c not in cols_to_drop]
    if not remaining_features:
        raise ValueError(
            f"All {len(feature_cols)} feature columns scored below threshold {threshold}. "
            "Lower the threshold or choose a different method."
        )

    # ── Transform all splits ─────────────────────────────────────
    results: dict[str, str] = {}

    for split_name in ("train", "val", "test"):
        input_path = inputs.get(split_name)
        if not input_path:
            continue

        df = train_df if split_name == "train" else pl.read_parquet(input_path)
        df = df.drop(cols_to_drop)

        out_path = _get_output_path(split_name)
        df.write_parquet(out_path)
        results[split_name] = str(out_path)

    return results


def _fit_selector(
    train_df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    method: str,
    threshold: float,
) -> list[str]:
    """Compute feature scores and return list of columns to drop."""
    cols_to_drop: list[str] = []

    if method == "variance_threshold":
        for col in feature_cols:
            series = train_df[col].drop_nulls().cast(pl.Float64)
            if len(series) == 0:
                cols_to_drop.append(col)
                continue
            variance = float(series.var())  # type: ignore[arg-type]
            if variance <= threshold:
                logger.info("Feature '%s' variance=%.6f <= threshold=%.4f — dropping", col, variance or 0, threshold)
                cols_to_drop.append(col)

    elif method == "correlation_with_target":
        target_series = train_df[target_col].drop_nulls().cast(pl.Float64)
        for col in feature_cols:
            series = train_df[col].drop_nulls().cast(pl.Float64)
            if len(series) == 0:
                cols_to_drop.append(col)
                continue
            corr = abs(_pearson_corr(train_df, col, target_col))
            if corr < threshold:
                logger.info("Feature '%s' |corr|=%.6f < threshold=%.4f — dropping", col, corr, threshold)
                cols_to_drop.append(col)

    elif method == "mutual_information":
        target_series = train_df[target_col].cast(pl.Float64).to_numpy()
        for col in feature_cols:
            series = train_df[col].cast(pl.Float64)
            # Drop rows where either feature or target is null
            mask = series.is_not_null() & train_df[target_col].is_not_null()
            feat_clean = series.filter(mask).to_numpy()
            targ_clean = train_df[target_col].cast(pl.Float64).filter(mask).to_numpy()
            if len(feat_clean) == 0:
                cols_to_drop.append(col)
                continue
            mi = _mutual_information(feat_clean, targ_clean)
            if mi < threshold:
                logger.info("Feature '%s' MI=%.6f < threshold=%.4f — dropping", col, mi, threshold)
                cols_to_drop.append(col)

    return cols_to_drop


def _pearson_corr(df: pl.DataFrame, col_a: str, col_b: str) -> float:
    """Compute Pearson correlation between two columns, handling nulls."""
    clean = df.select([col_a, col_b]).drop_nulls()
    if clean.height < 2:
        return 0.0
    corr = clean.select(pl.corr(col_a, col_b)).item()
    if corr is None:
        return 0.0
    import math
    if math.isnan(corr):
        return 0.0
    return float(corr)


def _mutual_information(x: object, y: object) -> float:
    """Estimate mutual information between continuous feature and target.

    Uses a simple histogram-based approach (no sklearn dependency).
    Bins the feature into quantile-based buckets and computes MI from
    the joint and marginal probability distributions.
    """
    import numpy as np

    x_arr = np.asarray(x, dtype=np.float64)
    y_arr = np.asarray(y, dtype=np.float64)

    # Remove any remaining NaN pairs
    valid = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_arr = x_arr[valid]
    y_arr = y_arr[valid]

    if len(x_arr) < 2:
        return 0.0

    # Bin feature into ~20 quantile bins (adaptive to distribution)
    n_bins = min(20, max(2, int(np.sqrt(len(x_arr)))))
    try:
        x_binned = np.digitize(x_arr, np.unique(np.quantile(x_arr, np.linspace(0, 1, n_bins + 1)[1:-1])))
    except Exception:
        return 0.0

    # Bin target similarly
    try:
        y_binned = np.digitize(y_arr, np.unique(np.quantile(y_arr, np.linspace(0, 1, n_bins + 1)[1:-1])))
    except Exception:
        return 0.0

    # Compute joint and marginal distributions
    n = len(x_arr)
    joint = {}
    for xi, yi in zip(x_binned, y_binned):
        key = (int(xi), int(yi))
        joint[key] = joint.get(key, 0) + 1

    x_marginal: dict[int, int] = {}
    y_marginal: dict[int, int] = {}
    for (xi, yi), count in joint.items():
        x_marginal[xi] = x_marginal.get(xi, 0) + count
        y_marginal[yi] = y_marginal.get(yi, 0) + count

    # MI = sum p(x,y) * log(p(x,y) / (p(x)*p(y)))
    mi = 0.0
    for (xi, yi), count in joint.items():
        p_xy = count / n
        p_x = x_marginal[xi] / n
        p_y = y_marginal[yi] / n
        if p_xy > 0 and p_x > 0 and p_y > 0:
            mi += p_xy * np.log(p_xy / (p_x * p_y))

    return max(0.0, float(mi))


