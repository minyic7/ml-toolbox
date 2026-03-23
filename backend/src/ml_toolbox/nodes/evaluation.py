"""Evaluation nodes for model performance assessment."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Text, Toggle, node


def _get_output_path(name: str = "output", ext: str = ".json") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


# ── ROC & PR Curves ────────────────────────────────────────────


@node(
    inputs={"predictions": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Target column (auto-detected from schema)"),
    },
    label="ROC & PR Curves",
    category="Evaluation",
    description="Compute ROC and Precision-Recall curves with AUC scores from predicted probabilities.",
    allowed_upstream={
        "predictions": [
            "decision_tree", "random_forest",
            "linear_regression", "logistic_regression", "gradient_boosting_train",
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
    import json

    import numpy as np
    import pandas as pd
    from sklearn.metrics import (
        auc,
        average_precision_score,
        precision_recall_curve,
        roc_auc_score,
        roc_curve,
    )

    def to_json_safe(val: object) -> object:
        """Convert numpy types to JSON-serializable Python types."""
        if isinstance(val, (np.integer,)):
            return int(val)
        if isinstance(val, (np.floating,)):
            return float(val)
        if isinstance(val, np.ndarray):
            return val.tolist()
        return val

    def build_warnings(roc_auc: float, ap: float, prevalence: float) -> list[dict]:
        """Generate interpretation warnings for binary classification."""
        warns: list[dict] = []
        if roc_auc < 0.6:
            warns.append({
                "type": "low_roc_auc",
                "message": (
                    f"AUC-ROC {roc_auc:.3f} — model barely beats random (0.5). "
                    "Consider better features or a different model."
                ),
            })
        elif roc_auc < 0.7:
            warns.append({
                "type": "medium_roc_auc",
                "message": (
                    f"AUC-ROC {roc_auc:.3f} — weak discrimination. "
                    "May be acceptable for some tasks but investigate improvements."
                ),
            })
        if prevalence < 0.1:
            warns.append({
                "type": "high_imbalance",
                "message": (
                    f"Positive class prevalence {prevalence:.1%} — highly imbalanced. "
                    f"Focus on PR curve (AP={ap:.3f}) rather than ROC for evaluation."
                ),
            })
        if ap < prevalence * 1.5 and prevalence < 0.5:
            warns.append({
                "type": "low_average_precision",
                "message": (
                    f"AP {ap:.3f} is close to prevalence {prevalence:.3f} — "
                    "model precision is barely above random. Consider rebalancing or better features."
                ),
            })
        return warns

    df = pd.read_parquet(inputs["predictions"])

    # Detect y_true column — prefer param, then convention-based fallback
    y_true_col = params.get("target_column", "")
    if not y_true_col:
        for candidate in ("y_true", "y_test", "target"):
            if candidate in df.columns:
                y_true_col = candidate
                break
    if not y_true_col:
        raise ValueError(
            "Missing ground-truth column. Set 'target_column' param or include "
            "a column named y_true, y_test, or target. "
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
            "positive_class": to_json_safe(positive_class),
            "classes": [to_json_safe(c) for c in classes],
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
            "warnings": build_warnings(roc_auc, ap_score, prevalence),
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
                "class": to_json_safe(cls),
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
            "classes": [to_json_safe(c) for c in classes],
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



@node(
    inputs={"model": PortType.MODEL},
    outputs={"report": PortType.METRICS},
    params={},
    label="Feature Importance",
    category="Evaluation",
    description="Extract and rank feature importances from a trained model.",
    allowed_upstream={
        "model": [
            "decision_tree", "random_forest",
            "linear_regression", "logistic_regression", "gradient_boosting_train",
        ],
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
    params={
        "target_column": Text(default="", description="Target column (auto-detected from schema)"),
    },
    allowed_upstream={
        "predictions": [
            "decision_tree", "random_forest",
            "linear_regression", "logistic_regression", "gradient_boosting_train",
        ],
    },
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
    import json

    import pandas as pd
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    def find_prediction_column(df: pd.DataFrame) -> str:
        for name in ("y_pred", "prediction", "predicted"):
            if name in df.columns:
                return name
        raise ValueError(
            "Cannot find prediction column. Include a column named 'y_pred' or 'prediction'."
        )

    def find_split_column(df: pd.DataFrame) -> str | None:
        for name in ("__split__", "split"):
            if name in df.columns:
                return name
        return None

    def compute_cls_metrics(y_true, y_pred, y_prob) -> dict:
        m: dict = {
            "accuracy": round(float(accuracy_score(y_true, y_pred)), 6),
            "f1_macro": round(float(f1_score(y_true, y_pred, average="macro", zero_division=0)), 6),
            "precision_macro": round(float(precision_score(y_true, y_pred, average="macro", zero_division=0)), 6),
            "recall_macro": round(float(recall_score(y_true, y_pred, average="macro", zero_division=0)), 6),
        }
        if y_prob is not None and len(y_prob.columns) > 0:
            try:
                classes = sorted(y_true.unique())
                if len(classes) == 2:
                    prob_col = y_prob.columns[-1]
                    m["auc"] = round(float(roc_auc_score(y_true, y_prob[prob_col])), 6)
                elif len(classes) > 2:
                    m["auc"] = round(float(roc_auc_score(y_true, y_prob.values, multi_class="ovr", average="macro")), 6)
            except (ValueError, TypeError):
                pass
        m["support"] = int(len(y_true))
        return m

    df = pd.read_parquet(inputs["predictions"])

    target_col = params.get("target_column", "")
    if not target_col:
        for name in ("y_true", "target", "label"):
            if name in df.columns:
                target_col = name
                break
    if not target_col:
        raise ValueError(
            "Cannot determine target column. Set 'target_column' param or include "
            "a column named 'y_true', 'target', or 'label'. "
            f"Got columns: {list(df.columns)}"
        )

    pred_col = find_prediction_column(df)
    split_col = find_split_column(df)

    prob_cols = [c for c in df.columns if c.startswith("y_prob_")]

    splits: dict[str, dict] = {}
    if split_col:
        for split_name in df[split_col].unique():
            mask = df[split_col] == split_name
            split_df = df[mask]
            y_prob = split_df[prob_cols] if prob_cols else None
            splits[str(split_name)] = compute_cls_metrics(
                split_df[target_col], split_df[pred_col], y_prob
            )
    else:
        y_prob = df[prob_cols] if prob_cols else None
        splits["all"] = compute_cls_metrics(df[target_col], df[pred_col], y_prob)

    split_order = ["train", "val", "test"]
    ordered_splits = [s for s in split_order if s in splits]
    ordered_splits += [s for s in splits if s not in ordered_splits]

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
    params={
        "target_column": Text(default="", description="Target column (auto-detected from schema)"),
    },
    allowed_upstream={
        "predictions": [
            "decision_tree", "random_forest",
            "linear_regression", "logistic_regression", "gradient_boosting_train",
        ],
    },
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
    import json

    import numpy as np
    import pandas as pd
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    def find_prediction_column(df: pd.DataFrame) -> str:
        for name in ("y_pred", "prediction", "predicted"):
            if name in df.columns:
                return name
        raise ValueError(
            "Cannot find prediction column. Include a column named 'y_pred' or 'prediction'."
        )

    def find_split_column(df: pd.DataFrame) -> str | None:
        for name in ("__split__", "split"):
            if name in df.columns:
                return name
        return None

    def compute_reg_metrics(y_true, y_pred) -> dict:
        mse = float(mean_squared_error(y_true, y_pred))
        return {
            "mae": round(float(mean_absolute_error(y_true, y_pred)), 6),
            "rmse": round(float(np.sqrt(mse)), 6),
            "r2": round(float(r2_score(y_true, y_pred)), 6),
            "support": int(len(y_true)),
        }

    df = pd.read_parquet(inputs["predictions"])

    target_col = params.get("target_column", "")
    if not target_col:
        for name in ("y_true", "target", "label"):
            if name in df.columns:
                target_col = name
                break
    if not target_col:
        raise ValueError(
            "Cannot determine target column. Set 'target_column' param or include "
            "a column named 'y_true', 'target', or 'label'. "
            f"Got columns: {list(df.columns)}"
        )

    pred_col = find_prediction_column(df)
    split_col = find_split_column(df)

    splits: dict[str, dict] = {}
    if split_col:
        for split_name in df[split_col].unique():
            mask = df[split_col] == split_name
            split_df = df[mask]
            splits[str(split_name)] = compute_reg_metrics(
                split_df[target_col], split_df[pred_col]
            )
    else:
        splits["all"] = compute_reg_metrics(df[target_col], df[pred_col])

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


# ── Confusion Matrix ──────────────────────────────────────────


@node(
    inputs={"predictions": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={
        "normalize": Toggle(
            default=False,
            description="Show percentages instead of raw counts",
        ),
    },
    label="Confusion Matrix",
    category="Evaluation",
    description="Compute confusion matrix with per-class precision, recall, and F1 for classification tasks.",
    allowed_upstream={
        "predictions": [
            "decision_tree", "random_forest",
            "linear_regression", "logistic_regression", "gradient_boosting_train",
        ],
    },
    guide="""## Confusion Matrix

Visualise how a classifier's predictions compare against ground truth, broken down by class.

### How to read it
- **Rows** = true (actual) classes, **Columns** = predicted classes
- **Diagonal** cells are correct predictions (True Positives for each class)
- **Off-diagonal** cells are mistakes — the row tells you what the sample *was*, the column tells you what the model *guessed*

### Key metrics shown alongside the matrix
| Metric | Meaning |
|--------|---------|
| **Precision** | Of everything the model predicted as class X, how many actually were X? High precision = few false positives. |
| **Recall** | Of all actual class X samples, how many did the model find? High recall = few false negatives. |
| **F1** | Harmonic mean of precision and recall — a single number balancing both. |

### Normalize toggle
- **Off (raw counts):** see exactly how many samples fall into each cell — useful for spotting class imbalance.
- **On (percentages):** each row sums to 100 % — easier to compare recall across classes of different sizes.""",
)
def confusion_matrix(inputs: dict, params: dict) -> dict:
    """Compute confusion matrix with per-class precision/recall/F1."""
    import json

    import numpy as np
    import pandas as pd

    df = pd.read_parquet(inputs["predictions"])

    if "y_true" not in df.columns or "y_pred" not in df.columns:
        missing = [c for c in ("y_true", "y_pred") if c not in df.columns]
        report = {
            "report_type": "confusion_matrix",
            "summary": {"total_samples": len(df), "num_classes": 0},
            "confusion_matrix": [], "class_labels": [], "per_class": [],
            "accuracy": 0.0,
            "warnings": [{"type": "missing_columns",
                          "message": f"Required columns missing: {', '.join(missing)}. "
                          "Input table must contain 'y_true' and 'y_pred' columns."}],
        }
        out = _get_output_path("report", ext=".json")
        out.write_text(json.dumps(report))
        return {"report": str(out)}

    y_true = df["y_true"].values
    y_pred = df["y_pred"].values
    classes = sorted(set(y_true) | set(y_pred), key=str)
    class_labels = [str(c) for c in classes]
    n_classes = len(classes)
    total_samples = len(y_true)

    label_to_idx = {c: i for i, c in enumerate(classes)}
    cm = np.zeros((n_classes, n_classes), dtype=int)
    for t, p in zip(y_true, y_pred):
        cm[label_to_idx[t], label_to_idx[p]] += 1

    per_class = []
    for i, label in enumerate(class_labels):
        tp = int(cm[i, i])
        fp = int(cm[:, i].sum() - tp)
        fn = int(cm[i, :].sum() - tp)
        support = int(cm[i, :].sum())
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
        per_class.append({"label": label, "precision": round(prec, 4),
                          "recall": round(rec, 4), "f1": round(f1, 4), "support": support})

    accuracy = round(float(np.trace(cm)) / total_samples, 4) if total_samples > 0 else 0.0
    row_sums = cm.sum(axis=1, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        cm_normalized = np.where(row_sums > 0, cm / row_sums, 0.0)

    warn_list: list[dict] = []
    supports = [pc["support"] for pc in per_class]
    if supports and max(supports) > 3 * min(supports) and min(supports) > 0:
        warn_list.append({"type": "class_imbalance",
                          "message": f"Class sizes vary significantly (min {min(supports)}, max {max(supports)}). "
                          "Consider stratified sampling or class weights."})
    low_recall = [pc["label"] for pc in per_class if pc["recall"] < 0.5 and pc["support"] > 0]
    if low_recall:
        warn_list.append({"type": "low_recall",
                          "message": f"Low recall (<50%) for: {', '.join(low_recall)}. "
                          "The model is missing many samples of these classes."})

    report = {
        "report_type": "confusion_matrix",
        "summary": {"total_samples": total_samples, "num_classes": n_classes},
        "accuracy": accuracy, "class_labels": class_labels,
        "confusion_matrix": cm.tolist(),
        "confusion_matrix_normalized": [[round(v, 4) for v in row] for row in cm_normalized.tolist()],
        "normalize": params.get("normalize", False),
        "per_class": per_class, "warnings": warn_list,
    }
    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report))
    return {"report": str(out)}


# ── Model Comparison ──────────────────────────────────────────

_ALLOWED_MODEL_UPSTREAM = [
    "decision_tree", "random_forest",
    "linear_regression", "logistic_regression", "gradient_boosting_train",
]
_ALLOWED_TEST_UPSTREAM = [
    "random_holdout", "stratified_holdout",
    "column_dropper", "missing_value_imputer", "category_encoder",
    "scaler_transform", "log_transform", "feature_selector",
    "interaction_creator", "datetime_encoder",
]


@node(
    inputs={
        "model_a": PortType.MODEL, "model_b": PortType.MODEL,
        "model_c": PortType.MODEL, "model_d": PortType.MODEL,
        "test": PortType.TABLE,
    },
    outputs={"report": PortType.METRICS},
    params={"target_column": Text(default="", description="Target column (auto-detected from schema)")},
    label="Model Comparison",
    category="Evaluation",
    description="Compare 2-4 models side-by-side on the same test set.",
    guide="""\
## Model Comparison

Compare multiple trained models on the **same held-out test set** to make
a fair, apples-to-apples selection.

### What metrics are computed?
| Task | Metrics |
|------|---------|
| Classification | Accuracy, Precision, Recall, F1 Score |
| Regression | MSE, RMSE, MAE, R-squared |

Task type (classification vs regression) is auto-detected from the target column.

### How to read the results
- **Best values are highlighted** for each metric.
- For classification: higher is better for all metrics.
- For regression: lower is better for MSE/RMSE/MAE; higher is better for R-squared.

### Interpreting close results
When two models score within ~1-2% of each other, prefer the simpler model
(fewer features, faster training) — the difference likely won't survive new data.""",
    allowed_upstream={
        "model_a": _ALLOWED_MODEL_UPSTREAM, "model_b": _ALLOWED_MODEL_UPSTREAM,
        "model_c": _ALLOWED_MODEL_UPSTREAM, "model_d": _ALLOWED_MODEL_UPSTREAM,
        "test": _ALLOWED_TEST_UPSTREAM,
    },
)
def model_comparison(inputs: dict, params: dict) -> dict:
    """Compare 2-4 models side-by-side on the same test set."""
    import json
    import warnings

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.metrics import (
        accuracy_score, f1_score, mean_absolute_error,
        mean_squared_error, precision_score, r2_score, recall_score,
    )

    MODEL_PORTS = ["model_a", "model_b", "model_c", "model_d"]
    HIGHER_IS_BETTER = {"accuracy", "precision", "recall", "f1_score", "r2"}
    LOWER_IS_BETTER = {"mse", "rmse", "mae"}

    def detect_task(y: np.ndarray) -> str:
        unique = np.unique(y[~np.isnan(y)] if np.issubdtype(y.dtype, np.floating) else y)
        if len(unique) <= 20 and np.issubdtype(y.dtype, np.integer):
            return "classification"
        if len(unique) <= 20 and np.issubdtype(y.dtype, np.floating):
            if np.allclose(unique, np.round(unique)):
                return "classification"
        return "regression"

    def cls_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
        is_binary = len(np.unique(y_true)) <= 2
        avg = "binary" if is_binary else "weighted"
        return {
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "precision": float(precision_score(y_true, y_pred, average=avg, zero_division=0)),
            "recall": float(recall_score(y_true, y_pred, average=avg, zero_division=0)),
            "f1_score": float(f1_score(y_true, y_pred, average=avg, zero_division=0)),
        }

    def reg_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
        mse = float(mean_squared_error(y_true, y_pred))
        return {"mse": mse, "rmse": float(np.sqrt(mse)),
                "mae": float(mean_absolute_error(y_true, y_pred)),
                "r2": float(r2_score(y_true, y_pred))}

    def find_best(metric_name: str, scores: dict[str, float]) -> list[str]:
        if not scores:
            return []
        if metric_name in HIGHER_IS_BETTER:
            best_val = max(scores.values())
        elif metric_name in LOWER_IS_BETTER:
            best_val = min(scores.values())
        else:
            return []
        return [n for n, v in scores.items() if v == best_val]

    connected_models: list[tuple[str, str]] = []
    for port in MODEL_PORTS:
        if port in inputs:
            connected_models.append((port, inputs[port]))
    if len(connected_models) < 2:
        raise ValueError(f"Model Comparison requires at least 2 model inputs. Got {len(connected_models)}.")
    if "test" not in inputs:
        raise ValueError("Model Comparison requires a test TABLE input.")

    test_df = pd.read_parquet(inputs["test"])
    target_col = params.get("target_column", "")
    if not target_col or target_col == "auto":
        for name in ("target", "label", "y"):
            if name in test_df.columns:
                target_col = name
                break
    if not target_col or target_col not in test_df.columns:
        target_col = test_df.columns[-1]
        warnings.warn(f"Target column not specified; using last column: {target_col}", stacklevel=1)

    X_test = test_df.drop(columns=[target_col])
    y_test = test_df[target_col].to_numpy()
    task = detect_task(y_test)
    compute = cls_metrics if task == "classification" else reg_metrics

    models_info: list[dict] = []
    all_metric_names: list[str] = []
    for port_name, model_path in connected_models:
        model = joblib.load(model_path)
        y_pred = model.predict(X_test)
        metrics = compute(y_test, y_pred)
        if not all_metric_names:
            all_metric_names = list(metrics.keys())
        models_info.append({
            "port": port_name, "name": f"{port_name} ({type(model).__name__})",
            "model_type": type(model).__name__, "metrics": metrics,
        })

    best_per_metric = {
        mn: find_best(mn, {m["name"]: m["metrics"][mn] for m in models_info})
        for mn in all_metric_names
    }

    warn_list: list[dict] = []
    if len(test_df) < 50:
        warn_list.append({"type": "small_test_set",
                          "message": f"Test set has only {len(test_df)} rows — metrics may be unreliable."})
    for mn in all_metric_names:
        vals = [m["metrics"][mn] for m in models_info]
        if len(vals) >= 2:
            spread = max(vals) - min(vals)
            if mn in HIGHER_IS_BETTER and spread < 0.02:
                warn_list.append({"type": "close_results",
                                  "message": f"Models are within 2% on {mn} ({spread:.4f}) — consider model complexity."})
            elif mn in LOWER_IS_BETTER and spread < 0.01 * max(abs(v) for v in vals):
                warn_list.append({"type": "close_results",
                                  "message": f"Models are very close on {mn} — consider model complexity."})

    report = {
        "report_type": "model_comparison",
        "summary": {"models_compared": len(models_info), "task_type": task,
                     "test_rows": len(test_df), "features": len(X_test.columns)},
        "models": models_info,
        "comparison_table": {
            "metric_names": all_metric_names,
            "models": [m["name"] for m in models_info],
            "values": {mn: [m["metrics"][mn] for m in models_info] for mn in all_metric_names},
            "best": best_per_metric,
        },
        "warnings": warn_list,
    }
    output_path = _get_output_path("report")
    output_path.write_text(json.dumps(report, indent=2))
    return {"report": str(output_path)}
