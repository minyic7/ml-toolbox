"""Evaluation nodes for model performance assessment."""

from __future__ import annotations

import json
from pathlib import Path

from ml_toolbox.protocol import PortType, node


def _get_output_path(name: str = "output", ext: str = ".json") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


def _read_meta(parquet_path: str) -> dict:
    """Read the .meta.json sidecar for a parquet file, if it exists."""
    meta_path = Path(parquet_path).with_suffix(".meta.json")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except Exception:
            pass
    return {}


def _find_target_column(df: "pd.DataFrame", meta: dict) -> str:  # type: ignore[name-defined]  # noqa: F821
    """Determine the target column from metadata or convention."""
    # 1. From metadata
    if meta.get("target"):
        return str(meta["target"])
    # 2. Common convention names
    for name in ("y_true", "target", "label"):
        if name in df.columns:
            return name
    raise ValueError(
        "Cannot determine target column. Provide a .meta.json sidecar "
        "with a 'target' field, or include a column named 'y_true' or 'target'."
    )


def _find_prediction_column(df: "pd.DataFrame") -> str:  # type: ignore[name-defined]  # noqa: F821
    """Determine the prediction column from convention."""
    for name in ("y_pred", "prediction", "predicted"):
        if name in df.columns:
            return name
    raise ValueError(
        "Cannot find prediction column. Include a column named 'y_pred' or 'prediction'."
    )


def _find_split_column(df: "pd.DataFrame") -> str | None:  # type: ignore[name-defined]  # noqa: F821
    """Find the split indicator column, if present."""
    for name in ("__split__", "split"):
        if name in df.columns:
            return name
    return None


def _compute_classification_metrics(
    y_true: "pd.Series",  # type: ignore[type-arg]  # noqa: F821
    y_pred: "pd.Series",  # type: ignore[type-arg]  # noqa: F821
    y_prob: "pd.DataFrame | None",  # type: ignore[name-defined]  # noqa: F821
) -> dict:
    """Compute classification metrics for a single split."""
    from sklearn.metrics import (  # pyright: ignore[reportMissingImports]
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    metrics: dict = {
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 6),
        "f1_macro": round(float(f1_score(y_true, y_pred, average="macro", zero_division=0)), 6),  # pyright: ignore[reportArgumentType]
        "precision_macro": round(
            float(precision_score(y_true, y_pred, average="macro", zero_division=0)), 6  # pyright: ignore[reportArgumentType]
        ),
        "recall_macro": round(
            float(recall_score(y_true, y_pred, average="macro", zero_division=0)), 6  # pyright: ignore[reportArgumentType]
        ),
    }

    # AUC — requires probability columns
    if y_prob is not None and len(y_prob.columns) > 0:
        try:
            classes = sorted(y_true.unique())
            if len(classes) == 2:
                # Binary: use the probability of the positive class
                prob_col = y_prob.columns[-1]  # last column = positive class
                metrics["auc"] = round(
                    float(roc_auc_score(y_true, y_prob[prob_col])), 6
                )
            elif len(classes) > 2:
                # Multiclass: OVR
                metrics["auc"] = round(
                    float(
                        roc_auc_score(
                            y_true, y_prob.values, multi_class="ovr", average="macro"
                        )
                    ),
                    6,
                )
        except (ValueError, TypeError):
            pass  # AUC not computable (e.g. single class in split)

    metrics["support"] = int(len(y_true))
    return metrics


def _compute_regression_metrics(
    y_true: "pd.Series",  # type: ignore[type-arg]  # noqa: F821
    y_pred: "pd.Series",  # type: ignore[type-arg]  # noqa: F821
) -> dict:
    """Compute regression metrics for a single split."""
    from sklearn.metrics import (  # pyright: ignore[reportMissingImports]
        mean_absolute_error,
        mean_squared_error,
        r2_score,
    )
    import numpy as np  # pyright: ignore[reportMissingImports]

    mse = float(mean_squared_error(y_true, y_pred))
    metrics: dict = {
        "mae": round(float(mean_absolute_error(y_true, y_pred)), 6),
        "rmse": round(float(np.sqrt(mse)), 6),
        "r2": round(float(r2_score(y_true, y_pred)), 6),
        "support": int(len(y_true)),
    }
    return metrics


# ── ROC & PR Curves ────────────────────────────────────────────


@node(
    inputs={"predictions": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={},
    label="ROC & PR Curves",
    category="Evaluation",
    description="Compute ROC and Precision-Recall curves with AUC scores from predicted probabilities.",
    allowed_upstream={
        "predictions": [
            "train_sklearn_model",
            "train_xgboost",
        ],
    },
    guide=(
        "## ROC & PR Curves\n\n"
        "Evaluate binary classifier quality using two complementary curves.\n\n"
        "### What it does\n"
        "- **ROC curve** (Receiver Operating Characteristic): plots True Positive Rate vs "
        "False Positive Rate at every probability threshold. AUC-ROC = 1.0 is perfect, 0.5 is random.\n"
        "- **PR curve** (Precision-Recall): plots Precision vs Recall at every threshold. "
        "Average Precision (AP) summarises the curve — higher is better.\n\n"
        "### ROC vs PR — when to use which\n"
        "- **Balanced classes**: ROC is a good default — it shows trade-offs across all thresholds.\n"
        "- **Imbalanced classes** (e.g. fraud, rare disease): prefer PR. ROC can look "
        "deceptively good because TNR stays high when negatives dominate. PR focuses on "
        "the positive class and exposes weak precision.\n\n"
        "### AUC interpretation\n"
        "- **AUC-ROC > 0.9**: excellent discrimination\n"
        "- **AUC-ROC 0.7–0.9**: good, usable in most applications\n"
        "- **AUC-ROC 0.5–0.7**: weak — model barely beats random\n"
        "- **AP (Average Precision)**: context-dependent — compare against the positive class "
        "prevalence (a random classifier achieves AP ≈ prevalence).\n\n"
        "### Why probabilities, not hard predictions?\n"
        "ROC and PR curves sweep across all thresholds — they need the continuous probability "
        "output (`y_prob`), not a binary `y_pred`. If your upstream node only outputs hard "
        "predictions, this node will fail with a clear error. Make sure your training node "
        "is configured to output probabilities."
    ),
)
def roc_pr_curves(inputs: dict, params: dict) -> dict:
    """Compute ROC and Precision-Recall curves with AUC values."""
    import numpy as np
    import pandas as pd
    from sklearn.metrics import (
        auc,
        average_precision_score,
        precision_recall_curve,
        roc_auc_score,
        roc_curve,
    )

    df = pd.read_parquet(inputs["predictions"])

    # Detect y_true column
    y_true_col = None
    for candidate in ("y_true", "y_test", "target"):
        if candidate in df.columns:
            y_true_col = candidate
            break
    if y_true_col is None:
        raise ValueError(
            "Missing ground-truth column. Expected one of: y_true, y_test, target. "
            f"Got columns: {list(df.columns)}"
        )

    y_true = df[y_true_col].values

    # Detect probability columns (y_prob_{class})
    prob_cols = sorted([c for c in df.columns if c.startswith("y_prob_")])
    if not prob_cols:
        raise ValueError(
            "No probability columns found (expected y_prob_<class> columns). "
            "ROC/PR curves require predicted probabilities, not hard predictions. "
            f"Got columns: {list(df.columns)}"
        )

    # Determine unique classes
    classes = sorted(np.unique(np.asarray(y_true)).tolist())
    n_classes = len(classes)

    if n_classes < 2:
        raise ValueError(
            f"Need at least 2 classes for ROC/PR curves, got {n_classes} "
            f"(unique values in {y_true_col}: {classes})"
        )

    # Binary classification
    if n_classes == 2:
        positive_class = classes[1]
        prob_col = f"y_prob_{positive_class}"
        if prob_col not in df.columns:
            # Try the first prob column as fallback
            prob_col = prob_cols[-1]
        y_score = df[prob_col].values

        # ROC curve
        fpr, tpr, roc_thresholds = roc_curve(y_true, y_score, pos_label=positive_class)
        roc_auc = float(roc_auc_score(y_true, y_score))

        # PR curve
        precision, recall, pr_thresholds = precision_recall_curve(
            y_true, y_score, pos_label=positive_class
        )
        ap_score = float(average_precision_score(y_true, y_score))

        # Downsample curve points if too many (keep first, last, and evenly spaced)
        max_points = 200

        def _downsample(x: np.ndarray, y: np.ndarray) -> tuple[list[float], list[float]]:
            if len(x) <= max_points:
                return [round(float(v), 6) for v in x], [round(float(v), 6) for v in y]
            indices = np.linspace(0, len(x) - 1, max_points, dtype=int)
            indices = np.unique(indices)
            return (
                [round(float(x[i]), 6) for i in indices],
                [round(float(y[i]), 6) for i in indices],
            )

        roc_fpr, roc_tpr = _downsample(fpr, tpr)
        pr_recall, pr_precision = _downsample(recall, precision)

        # Prevalence of positive class
        prevalence = float(np.mean(y_true == positive_class))

        report = {
            "report_type": "roc_pr_curves",
            "task": "binary",
            "positive_class": _to_json_safe(positive_class),
            "classes": [_to_json_safe(c) for c in classes],
            "summary": {
                "roc_auc": round(roc_auc, 4),
                "average_precision": round(ap_score, 4),
                "prevalence": round(prevalence, 4),
                "n_samples": len(y_true),
            },
            "roc_curve": {
                "fpr": roc_fpr,
                "tpr": roc_tpr,
            },
            "pr_curve": {
                "recall": pr_recall,
                "precision": pr_precision,
            },
            "warnings": _build_warnings(roc_auc, ap_score, prevalence),
        }
    else:
        # Multi-class: One-vs-Rest
        per_class: list[dict] = []
        warnings: list[dict] = []

        for cls in classes:
            prob_col = f"y_prob_{cls}"
            if prob_col not in df.columns:
                warnings.append({
                    "type": "missing_prob_column",
                    "message": f"No probability column for class {cls} (expected {prob_col})",
                })
                continue

            y_binary = (y_true == cls).astype(int)
            y_score = df[prob_col].values

            # Skip if only one class present in binary view
            if y_binary.sum() == 0 or y_binary.sum() == len(y_binary):
                continue

            fpr, tpr, _ = roc_curve(y_binary, y_score)
            roc_auc_val = float(roc_auc_score(y_binary, y_score))

            precision, recall, _ = precision_recall_curve(y_binary, y_score)
            ap_val = float(average_precision_score(y_binary, y_score))

            max_points = 200

            def _ds(x: np.ndarray, y: np.ndarray) -> tuple[list[float], list[float]]:
                if len(x) <= max_points:
                    return [round(float(v), 6) for v in x], [round(float(v), 6) for v in y]
                indices = np.unique(np.linspace(0, len(x) - 1, max_points, dtype=int))
                return (
                    [round(float(x[i]), 6) for i in indices],
                    [round(float(y[i]), 6) for i in indices],
                )

            roc_fpr, roc_tpr = _ds(fpr, tpr)
            pr_recall, pr_precision = _ds(recall, precision)

            prevalence = float(np.mean(y_binary))

            per_class.append({
                "class": _to_json_safe(cls),
                "roc_auc": round(roc_auc_val, 4),
                "average_precision": round(ap_val, 4),
                "prevalence": round(prevalence, 4),
                "roc_curve": {"fpr": roc_fpr, "tpr": roc_tpr},
                "pr_curve": {"recall": pr_recall, "precision": pr_precision},
            })

        macro_auc = float(np.mean([c["roc_auc"] for c in per_class])) if per_class else 0.0
        macro_ap = float(np.mean([c["average_precision"] for c in per_class])) if per_class else 0.0

        report = {
            "report_type": "roc_pr_curves",
            "task": "multiclass",
            "classes": [_to_json_safe(c) for c in classes],
            "summary": {
                "macro_roc_auc": round(macro_auc, 4),
                "macro_average_precision": round(macro_ap, 4),
                "n_classes": n_classes,
                "n_samples": len(y_true),
            },
            "per_class": per_class,
            "warnings": warnings,
        }

    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report))
    return {"report": str(out)}


def _to_json_safe(val: object) -> object:
    """Convert numpy types to JSON-serializable Python types."""
    import numpy as np

    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    return val


def _build_warnings(roc_auc: float, ap: float, prevalence: float) -> list[dict]:
    """Generate interpretation warnings for binary classification."""
    warnings: list[dict] = []

    if roc_auc < 0.6:
        warnings.append({
            "type": "low_roc_auc",
            "message": (
                f"AUC-ROC {roc_auc:.3f} — model barely beats random (0.5). "
                "Consider better features or a different model."
            ),
        })
    elif roc_auc < 0.7:
        warnings.append({
            "type": "medium_roc_auc",
            "message": (
                f"AUC-ROC {roc_auc:.3f} — weak discrimination. "
                "May be acceptable for some tasks but investigate improvements."
            ),
        })

    if prevalence < 0.1:
        warnings.append({
            "type": "high_imbalance",
            "message": (
                f"Positive class prevalence {prevalence:.1%} — highly imbalanced. "
                f"Focus on PR curve (AP={ap:.3f}) rather than ROC for evaluation."
            ),
        })

    if ap < prevalence * 1.5 and prevalence < 0.5:
        warnings.append({
            "type": "low_average_precision",
            "message": (
                f"AP {ap:.3f} is close to prevalence {prevalence:.3f} — "
                "model precision is barely above random. Consider rebalancing or better features."
            ),
        })

    return warnings


@node(
    inputs={"model": PortType.MODEL},
    outputs={"report": PortType.METRICS},
    params={},
    label="Feature Importance",
    category="Evaluation",
    description="Extract and rank feature importances from a trained model.",
    allowed_upstream={
        "model": ["train_sklearn_model", "train_xgboost"],
    },
    guide=(
        "## Feature Importance\n\n"
        "Rank features by how much they contribute to model predictions.\n\n"
        "### Tree-based models (Random Forest, Gradient Boosting, XGBoost)\n"
        "Uses `model.feature_importances_` — Gini importance (classification) "
        "or variance reduction (regression). Measures how much each feature "
        "reduces impurity across all splits.\n\n"
        "**Limitations:** biased toward high-cardinality features and correlated "
        "features split importance arbitrarily. Consider permutation importance "
        "for a more robust estimate.\n\n"
        "### Linear models (Logistic Regression, Linear Regression, Ridge, Lasso)\n"
        "Uses `np.abs(model.coef_)` — magnitude of learned coefficients. "
        "Only meaningful when features are on the same scale (use Scaler first).\n\n"
        "**Limitations:** coefficients reflect linear relationships only. "
        "Correlated features share coefficient weight unpredictably.\n\n"
        "### Remember\n"
        "Single-model importance is a starting point, not ground truth. "
        "Different models may rank features differently."
    ),
)
def feature_importance(inputs: dict, params: dict) -> dict:
    """Extract and rank feature importances from a trained model."""
    import json
    from pathlib import Path

    import joblib
    import numpy as np

    model = joblib.load(inputs["model"])

    # Try to get feature names from the model
    feature_names: list[str] | None = None
    if hasattr(model, "feature_names_in_"):
        feature_names = list(model.feature_names_in_)

    # Extract importances based on model type
    importances: np.ndarray
    method: str

    if hasattr(model, "feature_importances_"):
        # Tree-based: RF, GBT, XGBoost, etc.
        importances = np.asarray(model.feature_importances_, dtype=float)
        method = "tree_importance"
    elif hasattr(model, "coef_"):
        # Linear: LogisticRegression, LinearRegression, Ridge, Lasso, etc.
        coef = np.asarray(model.coef_, dtype=float)
        # For multi-class logistic regression, coef_ is 2D — average across classes
        if coef.ndim > 1:
            importances = np.mean(np.abs(coef), axis=0)  # pyright: ignore[reportAssignmentType]
        else:
            importances = np.abs(coef)
        method = "coefficient_magnitude"
    else:
        # Model type not supported
        report = {
            "report_type": "feature_importance",
            "method": "unsupported",
            "summary": {"feature_count": 0, "model_type": type(model).__name__},
            "features": [],
            "warnings": [
                {
                    "type": "unsupported_model",
                    "message": (
                        f"Model type {type(model).__name__} does not expose "
                        f"feature_importances_ or coef_"
                    ),
                }
            ],
        }
        out = _get_output_path("report", ext=".json")
        out.write_text(json.dumps(report))
        return {"report": str(out)}

    n_features = len(importances)

    # Generate feature names if not available
    if feature_names is None:
        feature_names = [f"feature_{i}" for i in range(n_features)]

    # Normalize importances to sum to 1 (for tree-based, they usually already do)
    total = float(np.sum(importances))
    if total > 0:
        normalized = importances / total
    else:
        normalized = importances

    # Build sorted feature list (descending by importance)
    features = []
    for idx in np.argsort(importances)[::-1]:
        features.append({
            "name": feature_names[int(idx)],
            "importance": round(float(normalized[int(idx)]), 6),
            "raw_importance": round(float(importances[int(idx)]), 6),
        })

    # Warnings
    warnings: list[dict] = []

    # Warn about dominant features
    if len(features) >= 2 and features[0]["importance"] > 0.5:
        warnings.append({
            "type": "dominant_feature",
            "column": features[0]["name"],
            "message": (
                f"{features[0]['name']} accounts for "
                f"{features[0]['importance'] * 100:.1f}% of total importance "
                f"— check for target leakage"
            ),
        })

    # Warn about negligible features
    negligible = [f for f in features if f["importance"] < 0.01]
    if negligible and len(negligible) < len(features):
        warnings.append({
            "type": "negligible_features",
            "message": (
                f"{len(negligible)} feature(s) contribute < 1% each "
                f"— candidates for removal"
            ),
        })

    report = {
        "report_type": "feature_importance",
        "method": method,
        "summary": {
            "feature_count": n_features,
            "model_type": type(model).__name__,
            "top_feature": features[0]["name"] if features else "",
            "top_importance": features[0]["importance"] if features else 0,
        },
        "features": features,
        "warnings": warnings,
    }

    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report))
    return {"report": str(out)}


# ── Classification Metrics ──────────────────────────────────────


@node(
    inputs={"predictions": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={},
    label="Classification Metrics",
    category="Evaluation",
    description="Compute accuracy, F1, precision, recall, and AUC per split.",
    guide=(
        "## Classification Metrics\n\n"
        "Evaluate a classifier's predictions against ground truth, per data split.\n\n"
        "### Input format\n"
        "A single TABLE with columns:\n"
        "- **y_true** (or **target**): actual class labels\n"
        "- **y_pred** (or **prediction**): predicted class labels\n"
        "- **y_prob_{class}** (optional): predicted probabilities per class (needed for AUC)\n"
        "- **__split__** (optional): split indicator (train/val/test)\n\n"
        "### Metrics computed\n"
        "- **Accuracy**: fraction of correct predictions\n"
        "- **F1 (macro)**: harmonic mean of precision and recall, averaged across classes\n"
        "- **Precision (macro)**: fraction of positive predictions that were correct\n"
        "- **Recall (macro)**: fraction of actual positives that were found\n"
        "- **AUC**: area under ROC curve (requires y_prob columns)\n\n"
        "### Comparing splits\n"
        "- **Train >> Val**: model is overfitting — consider regularization or more data\n"
        "- **Train ≈ Val ≈ Test**: good generalization\n"
        "- **All splits low**: model is underfitting — try a more complex model"
    ),
)
def classification_metrics(inputs: dict, params: dict) -> dict:
    """Compute classification metrics per split."""
    import pandas as pd

    df = pd.read_parquet(inputs["predictions"])
    meta = _read_meta(inputs["predictions"])

    target_col = _find_target_column(df, meta)
    pred_col = _find_prediction_column(df)
    split_col = _find_split_column(df)

    # Detect probability columns (y_prob_*)
    prob_cols = [c for c in df.columns if c.startswith("y_prob_")]

    # Build per-split metrics
    splits: dict[str, dict] = {}
    if split_col:
        for split_name in df[split_col].unique():
            mask = df[split_col] == split_name
            split_df = df[mask]
            y_prob = split_df[prob_cols] if prob_cols else None
            splits[str(split_name)] = _compute_classification_metrics(
                split_df[target_col], split_df[pred_col], y_prob
            )
    else:
        y_prob = df[prob_cols] if prob_cols else None
        splits["all"] = _compute_classification_metrics(
            df[target_col], df[pred_col], y_prob
        )

    # Determine split ordering for display
    split_order = ["train", "val", "test"]
    ordered_splits = [s for s in split_order if s in splits]
    ordered_splits += [s for s in splits if s not in ordered_splits]

    # Detect overfitting warnings
    warnings: list[dict] = []
    if "train" in splits and "val" in splits:
        train_acc = splits["train"]["accuracy"]
        val_acc = splits["val"]["accuracy"]
        gap = train_acc - val_acc
        if gap > 0.05:
            warnings.append({
                "type": "overfitting",
                "message": (
                    f"Train accuracy ({train_acc:.4f}) is {gap:.4f} higher than "
                    f"val accuracy ({val_acc:.4f}) — possible overfitting"
                ),
            })

    # Metric descriptions for UI
    metric_info = {
        "accuracy": "Fraction of correct predictions",
        "f1_macro": "Harmonic mean of precision & recall (macro-averaged)",
        "precision_macro": "Fraction of positive predictions that were correct",
        "recall_macro": "Fraction of actual positives that were found",
        "auc": "Area under the ROC curve",
        "support": "Number of samples in this split",
    }

    report = {
        "report_type": "training_metrics",
        "task_type": "classification",
        "splits": splits,
        "split_order": ordered_splits,
        "metric_info": metric_info,
        "warnings": warnings,
    }

    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report, indent=2))
    return {"report": str(out)}


# ── Regression Metrics ──────────────────────────────────────────


@node(
    inputs={"predictions": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={},
    label="Regression Metrics",
    category="Evaluation",
    description="Compute MAE, RMSE, and R² per split.",
    guide=(
        "## Regression Metrics\n\n"
        "Evaluate a regressor's predictions against ground truth, per data split.\n\n"
        "### Input format\n"
        "A single TABLE with columns:\n"
        "- **y_true** (or **target**): actual numeric values\n"
        "- **y_pred** (or **prediction**): predicted numeric values\n"
        "- **__split__** (optional): split indicator (train/val/test)\n\n"
        "### Metrics computed\n"
        "- **MAE**: mean absolute error — average magnitude of errors\n"
        "- **RMSE**: root mean squared error — penalizes large errors more\n"
        "- **R²**: coefficient of determination — 1.0 is perfect, 0.0 is baseline mean\n\n"
        "### Comparing splits\n"
        "- **Train RMSE << Val RMSE**: model is overfitting\n"
        "- **R² close to 0**: model explains little variance — try better features\n"
        "- **R² negative**: model is worse than predicting the mean"
    ),
)
def regression_metrics(inputs: dict, params: dict) -> dict:
    """Compute regression metrics per split."""
    import pandas as pd

    df = pd.read_parquet(inputs["predictions"])
    meta = _read_meta(inputs["predictions"])

    target_col = _find_target_column(df, meta)
    pred_col = _find_prediction_column(df)
    split_col = _find_split_column(df)

    # Build per-split metrics
    splits: dict[str, dict] = {}
    if split_col:
        for split_name in df[split_col].unique():
            mask = df[split_col] == split_name
            split_df = df[mask]
            splits[str(split_name)] = _compute_regression_metrics(
                split_df[target_col], split_df[pred_col]
            )
    else:
        splits["all"] = _compute_regression_metrics(
            df[target_col], df[pred_col]
        )

    # Determine split ordering
    split_order = ["train", "val", "test"]
    ordered_splits = [s for s in split_order if s in splits]
    ordered_splits += [s for s in splits if s not in ordered_splits]

    # Detect overfitting warnings
    warnings: list[dict] = []
    if "train" in splits and "val" in splits:
        train_rmse = splits["train"]["rmse"]
        val_rmse = splits["val"]["rmse"]
        if train_rmse > 0:
            ratio = val_rmse / train_rmse
            if ratio > 1.3:
                warnings.append({
                    "type": "overfitting",
                    "message": (
                        f"Val RMSE ({val_rmse:.4f}) is {ratio:.2f}x train RMSE "
                        f"({train_rmse:.4f}) — possible overfitting"
                    ),
                })
        elif val_rmse > 0:
            # Perfect train but imperfect val — clear overfitting
            warnings.append({
                "type": "overfitting",
                "message": (
                    f"Train RMSE is 0 but val RMSE is {val_rmse:.4f} "
                    f"— model memorised training data"
                ),
            })

    metric_info = {
        "mae": "Mean absolute error",
        "rmse": "Root mean squared error",
        "r2": "Coefficient of determination (1.0 = perfect)",
        "support": "Number of samples in this split",
    }

    report = {
        "report_type": "training_metrics",
        "task_type": "regression",
        "splits": splits,
        "split_order": ordered_splits,
        "metric_info": metric_info,
        "warnings": warnings,
    }

    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report, indent=2))
    return {"report": str(out)}
