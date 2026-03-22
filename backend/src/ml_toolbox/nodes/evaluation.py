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
