"""Linear Regression training node — regression-only counterpart to Logistic Regression."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from ml_toolbox.protocol import PortType, Text, Toggle, node

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
    inputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    outputs={
        "predictions": PortType.TABLE,
        "model": PortType.MODEL,
        "metrics": PortType.METRICS,
    },
    params={
        "target_column": Text(default="", description="Target column (auto-detected from schema)"),
        "fit_intercept": Toggle(
            default=True,
            description="Whether to calculate the intercept. Set to false if data is already centred.",
        ),
        "normalize": Toggle(
            default=False,
            description="Standardise features (zero mean, unit variance) before fitting. Useful when features have very different scales.",
        ),
    },
    label="Linear Regression",
    category="Training",
    description="Train a Linear Regression model (sklearn). Target must be numeric/continuous.",
    allowed_upstream={
        "train": [
            "random_holdout",
            "stratified_holdout",
            "column_dropper",
            "missing_value_imputer",
            "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout",
            "stratified_holdout",
            "column_dropper",
            "missing_value_imputer",
            "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout",
            "stratified_holdout",
            "column_dropper",
            "missing_value_imputer",
            "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Linear Regression

**Linear Regression** fits a straight-line (or hyperplane) relationship between features and a continuous target by minimising the sum of squared residuals (Ordinary Least Squares).

### When to Use
- Target variable is **numeric and continuous** (e.g. price, temperature, duration)
- You expect an approximately **linear** relationship between features and target
- You want an **interpretable** model — coefficients directly show feature effects

### Assumptions
| Assumption | What it means | What to check |
|------------|---------------|---------------|
| **Linearity** | Target is a linear combination of features | Residual plots should show no pattern |
| **Independence** | Observations are independent | No time-series autocorrelation |
| **Homoscedasticity** | Constant variance of residuals | Residuals vs fitted should be a flat band |
| **Normality** | Residuals are roughly normal | QQ-plot or histogram of residuals |

Violations don't make the model useless — predictions may still be useful — but confidence intervals and p-values become unreliable.

### Key Parameters

| Parameter | Effect |
|-----------|--------|
| **fit_intercept** | When true, the model learns a bias term (y = w*x + **b**). Disable only if your data is already centred around zero. |
| **normalize** | Standardises each feature to zero mean and unit variance before fitting. Helpful when features have very different scales (e.g. age vs income). Does not affect predictions (coefficients are back-transformed), but improves numerical stability. |

### Normalize vs External Scaling
The `normalize` toggle applies **StandardScaler** inside this node before fitting. If you already have a **Scaler Transform** node upstream, leave normalize off to avoid double-scaling.

### Metrics
| Metric | Meaning |
|--------|---------|
| **MAE** | Mean Absolute Error — average magnitude of errors, in target units |
| **RMSE** | Root Mean Squared Error — like MAE but penalises large errors more |
| **R\u00b2** | Coefficient of determination — 1.0 = perfect, 0.0 = predicts the mean, negative = worse than mean |

### Inputs / Outputs
| Port | Type | Required | Description |
|------|------|----------|-------------|
| train | TABLE | Yes | Training data with features + target |
| val | TABLE | No | Validation split — predictions + metrics computed if connected |
| test | TABLE | No | Test split — predictions + metrics computed if connected |
| predictions | TABLE | Out | Predictions for all connected splits (stacked) |
| model | MODEL | Out | Trained sklearn model (joblib-serialized) |
| metrics | METRICS | Out | MAE, RMSE, R\u00b2 per split + model coefficients |
""",
)
def linear_regression(inputs: dict, params: dict) -> dict:
    """Train a Linear Regression model on numeric/continuous targets."""
    import numpy as np
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.preprocessing import StandardScaler

    fit_intercept = bool(params.get("fit_intercept", True))
    normalize = bool(params.get("normalize", False))

    # ── Read train data ──────────────────────────────────────────
    train_df = pd.read_parquet(inputs["train"])

    # ── Read target column from params ───────────────────────────
    target_col = params.get("target_column", "")

    if not target_col or target_col not in train_df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            "Target column not specified. Run auto-configure or set target_column manually."
        )

    # ── Prepare features and target ──────────────────────────────
    feature_cols = [c for c in train_df.columns if c != target_col]
    X_train = train_df[feature_cols].values
    y_train = train_df[target_col].values

    # ── Optional normalisation ───────────────────────────────────
    scaler: StandardScaler | None = None
    if normalize:
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)

    # ── Train model ──────────────────────────────────────────────
    model = LinearRegression(fit_intercept=fit_intercept)
    model.fit(X_train, y_train)

    # ── Helper: evaluate a split ─────────────────────────────────
    def _evaluate(df: pd.DataFrame, split_name: str) -> tuple[dict, pd.DataFrame]:
        X = df[feature_cols].values
        if scaler is not None:
            X = scaler.transform(X)
        y_true = df[target_col].values
        y_pred = model.predict(X)

        pred_df = df.copy()
        pred_df["prediction"] = y_pred
        pred_df["split"] = split_name

        mae = float(mean_absolute_error(y_true, y_pred))
        rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
        r2 = float(r2_score(y_true, y_pred))
        return {"mae": mae, "rmse": rmse, "r2": r2}, pred_df

    # ── Compute metrics per split ────────────────────────────────
    all_predictions: list[pd.DataFrame] = []
    split_metrics: dict[str, dict] = {}

    train_metrics, train_preds = _evaluate(train_df, "train")
    split_metrics["train_metrics"] = train_metrics
    all_predictions.append(train_preds)

    if inputs.get("val"):
        val_path = Path(inputs["val"])
        if val_path.exists():
            val_df = pd.read_parquet(val_path)
            val_m, val_preds = _evaluate(val_df, "val")
            split_metrics["val_metrics"] = val_m
            all_predictions.append(val_preds)

    if inputs.get("test"):
        test_path = Path(inputs["test"])
        if test_path.exists():
            test_df = pd.read_parquet(test_path)
            test_m, test_preds = _evaluate(test_df, "test")
            split_metrics["test_metrics"] = test_m
            all_predictions.append(test_preds)

    # ── Coefficients (analogous to feature importances) ──────────
    coefficients = sorted(
        [
            {"feature": f, "coefficient": float(c)}
            for f, c in zip(feature_cols, model.coef_)
        ],
        key=lambda x: abs(x["coefficient"]),
        reverse=True,
    )

    # ── Build summary ────────────────────────────────────────────
    summary_source = split_metrics.get("val_metrics", split_metrics["train_metrics"])

    metrics_output = {
        "report_type": "linear_regression",
        "task": "regression",
        "summary": summary_source,
        **split_metrics,
        "coefficients": coefficients,
        "intercept": float(model.intercept_) if fit_intercept else None,
        "params": {
            "fit_intercept": fit_intercept,
            "normalize": normalize,
        },
    }

    # ── Write outputs ────────────────────────────────────────────
    predictions_df = pd.concat(all_predictions, ignore_index=True)
    pred_path = _get_output_path("predictions", ".parquet")
    predictions_df.to_parquet(pred_path, index=False)

    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics_output, indent=2))

    return {
        "predictions": str(pred_path),
        "model": model,
        "metrics": str(metrics_path),
    }
