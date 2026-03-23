"""Linear Regression training node — regression-only counterpart to Logistic Regression."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Text, Toggle, node


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
        "train_predictions": PortType.TABLE,
        "val_predictions": PortType.TABLE,
        "test_predictions": PortType.TABLE,
        "model": PortType.MODEL,
        "report": PortType.METRICS,
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

### Outputs
| Port | Type | Description |
|------|------|-------------|
| train_predictions | TABLE | y_true, y_pred on train split |
| val_predictions | TABLE | y_true, y_pred on validation split (if connected) |
| test_predictions | TABLE | y_true, y_pred on test split (if connected) |
| model | MODEL | Trained sklearn model (joblib-serialized) |
| report | METRICS | Training metadata: coefficients, intercept, feature list, sample counts |
""",
)
def linear_regression(inputs: dict, params: dict) -> dict:
    """Train a Linear Regression model on numeric/continuous targets."""
    import json
    from pathlib import Path

    import pandas as pd
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler

    fit_intercept = bool(params.get("fit_intercept", True))
    normalize = bool(params.get("normalize", False))

    # ── Read train data ──────────────────────────────────────────
    train_df = pd.read_parquet(inputs["train"])

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

    # ── Helper: build prediction DataFrame ─────────────────────────
    def _build_predictions(df: pd.DataFrame) -> pd.DataFrame:
        X = df[feature_cols].values
        if scaler is not None:
            X = scaler.transform(X)
        y_true = df[target_col].values
        y_pred = model.predict(X)
        return pd.DataFrame({"y_true": y_true, "y_pred": y_pred})

    # ── Write predictions per split ────────────────────────────────
    result: dict = {}
    sample_counts: dict = {"train": len(train_df)}

    train_preds = _build_predictions(train_df)
    p = _get_output_path("train_predictions", ".parquet")
    train_preds.to_parquet(p, index=False)
    result["train_predictions"] = str(p)

    for split_name in ("val", "test"):
        if inputs.get(split_name):
            split_path = Path(inputs[split_name])
            if split_path.exists():
                split_df = pd.read_parquet(split_path)
                if len(split_df) > 0:
                    preds = _build_predictions(split_df)
                    p = _get_output_path(f"{split_name}_predictions", ".parquet")
                    preds.to_parquet(p, index=False)
                    result[f"{split_name}_predictions"] = str(p)
                    sample_counts[split_name] = len(split_df)

    # ── Coefficients ─────────────────────────────────────────────
    coefficients = sorted(
        [{"feature": f, "coefficient": round(float(c), 6)}
         for f, c in zip(feature_cols, model.coef_)],
        key=lambda x: abs(x["coefficient"]),
        reverse=True,
    )

    # ── Training report ──────────────────────────────────────────
    report = {
        "report_type": "training_report",
        "model_type": "linear_regression",
        "task_type": "regression",
        "target_column": target_col,
        "feature_columns": feature_cols,
        "sample_counts": sample_counts,
        "params": {
            "fit_intercept": fit_intercept,
            "normalize": normalize,
        },
        "coefficients": coefficients,
        "intercept": round(float(model.intercept_), 6) if fit_intercept else None,
    }

    report_path = _get_output_path("report", ".json")
    report_path.write_text(json.dumps(report, indent=2))
    result["report"] = str(report_path)

    result["model"] = model
    return result
