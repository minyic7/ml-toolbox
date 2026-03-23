"""Logistic Regression training node."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Slider, Text, node


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
        "C": Slider(
            min=0.001,
            max=100.0,
            step=0.001,
            default=1.0,
            description="Inverse regularization strength — smaller values = stronger regularization",
        ),
        "max_iter": Slider(
            min=100,
            max=5000,
            step=100,
            default=1000,
            description="Maximum iterations for the solver to converge",
        ),
        "solver": Select(
            options=["lbfgs", "saga", "liblinear"],
            default="lbfgs",
            description="Optimization algorithm",
        ),
        "penalty": Select(
            options=["l2", "l1", "none"],
            default="l2",
            description="Regularization penalty type",
        ),
        "multi_class": Select(
            options=["auto", "ovr", "multinomial"],
            default="auto",
            description="Multi-class strategy: 'ovr' fits one binary classifier per class, 'multinomial' fits a single classifier over all classes, 'auto' picks based on solver and data",
        ),
    },
    label="Logistic Regression",
    category="Training",
    description="Train a scikit-learn LogisticRegression classifier. Outputs per-split predictions, model (.joblib), and training report (.json).",
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
    guide="""## Logistic Regression

A linear classifier that models the probability of class membership using the logistic (sigmoid) function. Despite its name, it is used for **classification**, not regression.

### How it works

Logistic regression fits a linear decision boundary by minimizing a log-loss objective. For input features **x**, it predicts:

`P(y=1|x) = 1 / (1 + exp(-(w·x + b)))`

The model outputs calibrated probabilities, making it ideal when you need confidence scores alongside predictions.

### Parameters

| Parameter | Purpose |
|-----------|---------|
| **C** | Inverse regularization strength. Lower values (e.g. 0.01) = stronger regularization = simpler model. Higher values (e.g. 100) = less regularization = more complex model. |
| **max_iter** | Maximum solver iterations. Increase if you see convergence warnings. |
| **solver** | Optimization algorithm. `lbfgs` is a good default. `saga` handles large datasets. `liblinear` works well for small datasets and L1 penalty. |
| **penalty** | `l2` (Ridge) shrinks all coefficients. `l1` (Lasso) can zero out features — useful for feature selection. `none` disables regularization. |
| **multi_class** | Strategy reference: `auto` selects automatically, `ovr` (one-vs-rest) fits one binary classifier per class, `multinomial` fits over all classes. Note: sklearn ≥1.7 selects the optimal strategy automatically. |

### When to use
- **Binary or multi-class classification** with mostly numeric features
- **Interpretability matters** — coefficients show feature importance and direction
- **Calibrated probabilities** — output probabilities are well-calibrated by design
- **Baseline model** — fast to train, easy to understand, hard to beat on clean data

### Solver–penalty compatibility
| Solver | l1 | l2 | none |
|--------|----|----|------|
| lbfgs | — | yes | yes |
| saga | yes | yes | yes |
| liblinear | yes | yes | — |

### Outputs
| Port | Type | Description |
|------|------|-------------|
| train_predictions | TABLE | y_true, y_pred, y_prob per class on train split |
| val_predictions | TABLE | Same format on validation split (if connected) |
| test_predictions | TABLE | Same format on test split (if connected) |
| model | MODEL | Trained sklearn model (joblib-serialized) |
| report | METRICS | Training metadata: classes, coefficients, target distribution, sample counts |
""",
)
def logistic_regression(inputs: dict, params: dict) -> dict:
    """Train a LogisticRegression classifier and produce per-split predictions + report."""
    import json
    from pathlib import Path

    import pandas as pd
    from sklearn.linear_model import LogisticRegression

    # ── Parse params ──────────────────────────────────────────────
    C = float(params.get("C", 1.0))
    max_iter = int(params.get("max_iter", 1000))
    solver = params.get("solver", "lbfgs")
    penalty_raw = params.get("penalty", "l2")
    penalty_val: str | None = None if penalty_raw == "none" else penalty_raw

    # ── Read train data ────────────────────────────────────────────
    train_df = pd.read_parquet(inputs["train"])

    target_col = params.get("target_column", "")
    if not target_col or target_col not in train_df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            "Target column not specified. Run auto-configure or set target_column manually."
        )

    # ── X/y split ─────────────────────────────────────────────────
    feature_cols = [c for c in train_df.columns if c != target_col]
    X_train = train_df[feature_cols]
    y_train = train_df[target_col]

    # ── Train model ───────────────────────────────────────────────
    model = LogisticRegression(
        C=C,
        max_iter=max_iter,
        solver=solver,
        penalty=penalty_val,  # type: ignore[arg-type]
        random_state=42,
    )
    model.fit(X_train, y_train)

    classes = model.classes_

    # ── Helper: build prediction DataFrame ─────────────────────────
    def _build_predictions(df: pd.DataFrame) -> pd.DataFrame:
        X = df[feature_cols]
        y_true = df[target_col].values
        y_pred = model.predict(X)
        y_prob = model.predict_proba(X)
        pred_data: dict = {"y_true": y_true, "y_pred": y_pred}
        for i, cls in enumerate(classes):
            pred_data[f"y_prob_{cls}"] = y_prob[:, i]
        return pd.DataFrame(pred_data)

    # ── Write predictions per split ────────────────────────────────
    result: dict = {}
    sample_counts: dict = {"train": len(train_df)}

    train_preds = _build_predictions(train_df)
    p = _get_output_path("train_predictions", ".parquet")
    train_preds.to_parquet(p, index=False)
    result["train_predictions"] = str(p)

    for split_name in ("val", "test"):
        input_path = inputs.get(split_name)
        if not input_path:
            continue
        split_path = Path(input_path)
        if not split_path.exists():
            continue
        split_df = pd.read_parquet(split_path)
        if split_df.empty:
            continue
        preds = _build_predictions(split_df)
        p = _get_output_path(f"{split_name}_predictions", ".parquet")
        preds.to_parquet(p, index=False)
        result[f"{split_name}_predictions"] = str(p)
        sample_counts[split_name] = len(split_df)

    # ── Coefficients ─────────────────────────────────────────────
    import numpy as np
    coef = np.asarray(model.coef_, dtype=float)
    if coef.ndim > 1:
        avg_coef = np.mean(np.abs(coef), axis=0)
    else:
        avg_coef = np.abs(coef)
    coefficients = sorted(
        [{"feature": f, "coefficient": round(float(c), 6)}
         for f, c in zip(feature_cols, avg_coef)],
        key=lambda x: abs(x["coefficient"]),
        reverse=True,
    )

    # ── Training report ──────────────────────────────────────────
    report = {
        "report_type": "training_report",
        "model_type": "logistic_regression",
        "task_type": "classification",
        "target_column": target_col,
        "feature_columns": feature_cols,
        "sample_counts": sample_counts,
        "classes": classes.tolist(),
        "target_distribution": {
            str(cls): int((train_df[target_col] == cls).sum())
            for cls in classes
        },
        "params": {
            "C": C,
            "max_iter": max_iter,
            "solver": solver,
            "penalty": penalty_raw,
        },
        "coefficients": coefficients,
    }

    report_path = _get_output_path("report", ".json")
    report_path.write_text(json.dumps(report, indent=2))
    result["report"] = str(report_path)

    result["model"] = model
    return result
