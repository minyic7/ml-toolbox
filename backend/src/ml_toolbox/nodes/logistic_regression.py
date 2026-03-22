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
        "predictions": PortType.TABLE,
        "model": PortType.MODEL,
        "metrics": PortType.METRICS,
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
    description="Train a scikit-learn LogisticRegression classifier. Outputs predictions, model (.joblib), and metrics (.json).",
    allowed_upstream={
        "train": [
            "random_holdout",
            "scaler_transform",
            "column_dropper",
            "missing_value_imputer",
            "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout",
            "scaler_transform",
            "column_dropper",
            "missing_value_imputer",
            "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout",
            "scaler_transform",
            "column_dropper",
            "missing_value_imputer",
            "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
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
- **predictions** — DataFrame with `y_pred`, `y_prob_<class>` columns, and a `split` column (train/val/test)
- **model** — Trained sklearn model saved as `.joblib`
- **metrics** — JSON with accuracy, F1, precision, recall, and AUC per split
""",
)
def logistic_regression(inputs: dict, params: dict) -> dict:
    """Train a LogisticRegression classifier and produce predictions + metrics."""
    import json
    from pathlib import Path

    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    # ── Parse params ──────────────────────────────────────────────
    C = float(params.get("C", 1.0))
    max_iter = int(params.get("max_iter", 1000))
    solver = params.get("solver", "lbfgs")
    penalty_raw = params.get("penalty", "l2")

    # sklearn uses None instead of "none" string
    penalty_val: str | None = None if penalty_raw == "none" else penalty_raw

    # ── Read train data ────────────────────────────────────────────
    train_df = pd.read_parquet(inputs["train"])

    # ── Read target column from params ───────────────────────────
    target_col = params.get("target_column", "")

    if not target_col or target_col not in train_df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            "Target column not specified. Run auto-configure or set target_column manually."
        )

    # ── X/y split ─────────────────────────────────────────────────
    X_train = train_df.drop(columns=[target_col])
    y_train = train_df[target_col]

    # ── Train model ───────────────────────────────────────────────
    # multi_class param is exposed in the UI for documentation purposes but
    # sklearn >=1.7 removed it (the solver picks the optimal strategy automatically).
    model = LogisticRegression(
        C=C,
        max_iter=max_iter,
        solver=solver,
        penalty=penalty_val,  # type: ignore[arg-type]
        random_state=42,
    )
    model.fit(X_train, y_train)

    # ── Helper: compute metrics for a split ───────────────────────
    classes = model.classes_

    def _compute_metrics(y_true: pd.Series, y_pred, y_prob) -> dict:  # type: ignore[type-arg]
        is_binary = len(classes) == 2
        average = "binary" if is_binary else "weighted"

        metrics: dict = {
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "f1": float(f1_score(y_true, y_pred, average=average, zero_division=0.0)),  # type: ignore[arg-type]
            "precision": float(
                precision_score(y_true, y_pred, average=average, zero_division=0.0)  # type: ignore[arg-type]
            ),
            "recall": float(
                recall_score(y_true, y_pred, average=average, zero_division=0.0)  # type: ignore[arg-type]
            ),
        }

        # AUC — needs probability columns
        try:
            if is_binary:
                metrics["auc"] = float(roc_auc_score(y_true, y_prob[:, 1]))
            else:
                metrics["auc"] = float(
                    roc_auc_score(y_true, y_prob, multi_class="ovr", average="weighted")
                )
        except (ValueError, IndexError):
            # AUC can fail if only one class is present in the split
            pass

        return metrics

    # ── Generate predictions + metrics per split ──────────────────
    all_predictions: list[pd.DataFrame] = []
    metrics_report: dict = {"report_type": "training_metrics"}

    for split_name in ("train", "val", "test"):
        input_path = inputs.get(split_name)
        if not input_path:
            continue

        split_path = Path(input_path)
        if not split_path.exists():
            continue

        split_df = train_df if split_name == "train" else pd.read_parquet(split_path)
        if split_df.empty:
            continue

        X_split = split_df.drop(columns=[target_col])
        y_split: pd.Series = split_df[target_col]  # type: ignore[assignment]

        y_pred = model.predict(X_split)
        y_prob = model.predict_proba(X_split)

        # Build predictions DataFrame
        pred_df = pd.DataFrame({"y_pred": y_pred})
        for i, cls in enumerate(classes):
            pred_df[f"y_prob_{cls}"] = y_prob[:, i]
        pred_df["split"] = split_name
        all_predictions.append(pred_df)

        # Compute metrics
        metrics_report[split_name] = _compute_metrics(y_split, y_pred, y_prob)

    # ── Save predictions ──────────────────────────────────────────
    predictions_df = pd.concat(all_predictions, ignore_index=True)
    predictions_path = _get_output_path("predictions", ".parquet")
    predictions_df.to_parquet(predictions_path, index=False)

    # ── Save metrics ──────────────────────────────────────────────
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics_report, indent=2))

    # ── Return results ────────────────────────────────────────────
    # MODEL output: return the raw model object — the sandbox runner
    # auto-serializes it to .joblib based on the output port type.
    return {
        "predictions": str(predictions_path),
        "model": model,
        "metrics": str(metrics_path),
    }
