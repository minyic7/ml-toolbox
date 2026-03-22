"""Evaluation nodes for classification, regression, and model comparison tasks."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from ml_toolbox.protocol import PortType, Toggle, node
from ml_toolbox.protocol.params import Select

logger = logging.getLogger(__name__)


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
        "predictions": ["train_sklearn_model", "train_xgboost"],
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

### When high FP vs high FN matters
- **High False Positives (low precision):** costly when the *action* triggered by a positive prediction is expensive — e.g., flagging legitimate transactions as fraud (annoying customers).
- **High False Negatives (low recall):** costly when *missing* a positive is dangerous — e.g., failing to detect a malignant tumour.

### Normalize toggle
- **Off (raw counts):** see exactly how many samples fall into each cell — useful for spotting class imbalance.
- **On (percentages):** each row sums to 100 % — easier to compare recall across classes of different sizes.""",
)
def confusion_matrix(inputs: dict, params: dict) -> dict:
    """Compute confusion matrix with per-class precision/recall/F1."""
    df = pd.read_parquet(inputs["predictions"])

    # Validate required columns
    if "y_true" not in df.columns or "y_pred" not in df.columns:
        missing = [c for c in ("y_true", "y_pred") if c not in df.columns]
        report = {
            "report_type": "confusion_matrix",
            "summary": {"total_samples": len(df), "num_classes": 0},
            "confusion_matrix": [],
            "class_labels": [],
            "per_class": [],
            "accuracy": 0.0,
            "warnings": [
                {
                    "type": "missing_columns",
                    "message": f"Required columns missing: {', '.join(missing)}. "
                    "Input table must contain 'y_true' and 'y_pred' columns.",
                }
            ],
        }
        out = _get_output_path("report", ext=".json")
        out.write_text(json.dumps(report))
        return {"report": str(out)}

    y_true = df["y_true"].values
    y_pred = df["y_pred"].values

    # Determine class labels (sorted)
    classes = sorted(set(y_true) | set(y_pred), key=str)
    class_labels = [str(c) for c in classes]
    n_classes = len(classes)
    total_samples = len(y_true)

    # Build confusion matrix: rows = true, cols = predicted
    label_to_idx = {c: i for i, c in enumerate(classes)}
    cm = np.zeros((n_classes, n_classes), dtype=int)
    for t, p in zip(y_true, y_pred):
        cm[label_to_idx[t], label_to_idx[p]] += 1

    # Compute per-class metrics
    per_class = []
    for i, label in enumerate(class_labels):
        tp = int(cm[i, i])
        fp = int(cm[:, i].sum() - tp)
        fn = int(cm[i, :].sum() - tp)
        support = int(cm[i, :].sum())

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        per_class.append(
            {
                "label": label,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1": round(f1, 4),
                "support": support,
            }
        )

    # Overall accuracy
    accuracy = round(float(np.trace(cm)) / total_samples, 4) if total_samples > 0 else 0.0

    # Build normalized matrix (row-wise, each row sums to 1)
    row_sums = cm.sum(axis=1, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        cm_normalized = np.where(row_sums > 0, cm / row_sums, 0.0)

    normalize = params.get("normalize", False)

    # Warnings
    warnings: list[dict] = []
    supports = [pc["support"] for pc in per_class]
    if supports and max(supports) > 3 * min(supports) and min(supports) > 0:
        warnings.append(
            {
                "type": "class_imbalance",
                "message": f"Class sizes vary significantly (min {min(supports)}, max {max(supports)}). "
                "Consider stratified sampling or class weights.",
            }
        )

    low_recall = [pc["label"] for pc in per_class if pc["recall"] < 0.5 and pc["support"] > 0]
    if low_recall:
        warnings.append(
            {
                "type": "low_recall",
                "message": f"Low recall (<50%) for: {', '.join(low_recall)}. "
                "The model is missing many samples of these classes.",
            }
        )

    report = {
        "report_type": "confusion_matrix",
        "summary": {
            "total_samples": total_samples,
            "num_classes": n_classes,
        },
        "accuracy": accuracy,
        "class_labels": class_labels,
        "confusion_matrix": cm.tolist(),
        "confusion_matrix_normalized": [[round(v, 4) for v in row] for row in cm_normalized.tolist()],
        "normalize": normalize,
        "per_class": per_class,
        "warnings": warnings,
    }

    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report))
    return {"report": str(out)}


# ── Classification metrics ──────────────────────────────────────

def _classification_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Compute standard classification metrics."""
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
    )

    is_binary = len(np.unique(y_true)) <= 2
    average = "binary" if is_binary else "weighted"

    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, average=average, zero_division=0)),  # type: ignore[arg-type]
        "recall": float(recall_score(y_true, y_pred, average=average, zero_division=0)),  # type: ignore[arg-type]
        "f1_score": float(f1_score(y_true, y_pred, average=average, zero_division=0)),  # type: ignore[arg-type]
    }


# ── Regression metrics ──────────────────────────────────────────

def _regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """Compute standard regression metrics."""
    from sklearn.metrics import (
        mean_absolute_error,
        mean_squared_error,
        r2_score,
    )

    mse = float(mean_squared_error(y_true, y_pred))
    return {
        "mse": mse,
        "rmse": float(np.sqrt(mse)),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


# ── Task detection ──────────────────────────────────────────────

def _detect_task(y: np.ndarray) -> str:
    """Heuristic: classification if <=20 unique values and integer-like."""
    unique = np.unique(y[~np.isnan(y)] if np.issubdtype(y.dtype, np.floating) else y)
    if len(unique) <= 20 and np.issubdtype(y.dtype, np.integer):
        return "classification"
    if len(unique) <= 20 and np.issubdtype(y.dtype, np.floating):
        # Check if all values are integer-like
        if np.allclose(unique, np.round(unique)):
            return "classification"
    return "regression"


# ── Identify best values ────────────────────────────────────────

# Metrics where higher is better
_HIGHER_IS_BETTER = {"accuracy", "precision", "recall", "f1_score", "r2"}
# Metrics where lower is better
_LOWER_IS_BETTER = {"mse", "rmse", "mae"}


def _find_best(metric_name: str, model_scores: dict[str, float]) -> list[str]:
    """Return model name(s) that have the best value for this metric."""
    if not model_scores:
        return []

    if metric_name in _HIGHER_IS_BETTER:
        best_val = max(model_scores.values())
    elif metric_name in _LOWER_IS_BETTER:
        best_val = min(model_scores.values())
    else:
        return []

    return [name for name, val in model_scores.items() if val == best_val]


# ── Helper: extract target from metadata ────────────────────────

def _read_target_column(test_path: str) -> str | None:
    """Read target column name from .meta.json sidecar if it exists."""
    meta_path = Path(test_path).with_suffix(".meta.json")
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        return meta.get("target")
    return None


# ── Model name extraction ───────────────────────────────────────

def _model_display_name(model: object, port_label: str) -> str:
    """Extract a human-readable name from a sklearn/xgboost model."""
    cls_name = type(model).__name__
    return f"{port_label} ({cls_name})"


# ── Node definition ─────────────────────────────────────────────

_MODEL_PORTS = ["model_a", "model_b", "model_c", "model_d"]

_ALLOWED_MODEL_UPSTREAM = [
    "train_sklearn_model",
    "train_xgboost",
]

_ALLOWED_TEST_UPSTREAM = [
    "random_holdout",
    "column_dropper",
    "missing_value_imputer",
    "category_encoder",
    "scaler_transform",
    "log_transform",
    "interaction_creator",
    "datetime_encoder",
]


@node(
    inputs={
        "model_a": PortType.MODEL,
        "model_b": PortType.MODEL,
        "model_c": PortType.MODEL,
        "model_d": PortType.MODEL,
        "test": PortType.TABLE,
    },
    outputs={
        "report": PortType.METRICS,
    },
    params={
        "target_column": Select(
            ["auto"],
            default="auto",
            description="Target column — 'auto' reads from .meta.json sidecar",
        ),
    },
    label="Model Comparison",
    category="Evaluation",
    description="Compare 2-4 models side-by-side on the same test set.",
    guide="""\
## Model Comparison

Compare multiple trained models on the **same held-out test set** to make
a fair, apples-to-apples selection.

### Why compare on the same test set?
Each model must be evaluated on identical data so that differences in metrics
reflect real performance gaps — not differences in test samples.

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
(fewer features, faster training) — the difference likely won't survive new data.

### Ports
| Port | Required | Type |
|------|----------|------|
| model_a | Yes | MODEL |
| model_b | Yes | MODEL |
| model_c | No | MODEL |
| model_d | No | MODEL |
| test | Yes | TABLE |
""",
    allowed_upstream={
        "model_a": _ALLOWED_MODEL_UPSTREAM,
        "model_b": _ALLOWED_MODEL_UPSTREAM,
        "model_c": _ALLOWED_MODEL_UPSTREAM,
        "model_d": _ALLOWED_MODEL_UPSTREAM,
        "test": _ALLOWED_TEST_UPSTREAM,
    },
)
def model_comparison(inputs: dict, params: dict) -> dict:
    # ── Validate: need at least 2 models and 1 test set ─────────
    connected_models: list[tuple[str, str]] = []
    for port in _MODEL_PORTS:
        if port in inputs:
            connected_models.append((port, inputs[port]))

    if len(connected_models) < 2:
        raise ValueError(
            "Model Comparison requires at least 2 model inputs. "
            f"Got {len(connected_models)}."
        )

    if "test" not in inputs:
        raise ValueError("Model Comparison requires a test TABLE input.")

    # ── Load test data ──────────────────────────────────────────
    test_path = inputs["test"]
    test_df = pd.read_parquet(test_path)

    # Determine target column
    target_col = params.get("target_column", "auto")
    if target_col == "auto":
        target_col = _read_target_column(test_path)
    if not target_col or target_col not in test_df.columns:
        # Fallback: last column
        target_col = test_df.columns[-1]
        logger.warning("Target column not found in metadata; using last column: %s", target_col)

    X_test = test_df.drop(columns=[target_col])
    y_test: np.ndarray = test_df[target_col].to_numpy()

    # ── Detect task type ────────────────────────────────────────
    task = _detect_task(y_test)
    compute_metrics = _classification_metrics if task == "classification" else _regression_metrics

    # ── Load models and compute metrics ─────────────────────────
    models_info: list[dict] = []
    all_metric_names: list[str] = []

    for port_name, model_path in connected_models:
        model = joblib.load(model_path)
        display_name = _model_display_name(model, port_name)

        y_pred = model.predict(X_test)
        metrics = compute_metrics(y_test, y_pred)

        if not all_metric_names:
            all_metric_names = list(metrics.keys())

        models_info.append({
            "port": port_name,
            "name": display_name,
            "model_type": type(model).__name__,
            "metrics": metrics,
        })

    # ── Find best values per metric ─────────────────────────────
    best_per_metric: dict[str, list[str]] = {}
    for metric_name in all_metric_names:
        scores = {m["name"]: m["metrics"][metric_name] for m in models_info}
        best_per_metric[metric_name] = _find_best(metric_name, scores)

    # ── Build summary ───────────────────────────────────────────
    summary = {
        "models_compared": len(models_info),
        "task_type": task,
        "test_rows": len(test_df),
        "features": len(X_test.columns),
    }

    # ── Build comparison table ──────────────────────────────────
    # rows = metrics, columns = models
    comparison_table = {
        "metric_names": all_metric_names,
        "models": [m["name"] for m in models_info],
        "values": {
            metric: [m["metrics"][metric] for m in models_info]
            for metric in all_metric_names
        },
        "best": best_per_metric,
    }

    # ── Warnings ────────────────────────────────────────────────
    warnings: list[dict] = []

    if len(test_df) < 50:
        warnings.append({
            "type": "small_test_set",
            "message": f"Test set has only {len(test_df)} rows — metrics may be unreliable.",
        })

    # Check for close results
    for metric_name in all_metric_names:
        vals = [m["metrics"][metric_name] for m in models_info]
        if len(vals) >= 2:
            spread = max(vals) - min(vals)
            if metric_name in _HIGHER_IS_BETTER and spread < 0.02:
                warnings.append({
                    "type": "close_results",
                    "message": f"Models are within 2% on {metric_name} ({spread:.4f}) — "
                    "consider model complexity as a tiebreaker.",
                })
            elif metric_name in _LOWER_IS_BETTER and spread < 0.01 * max(abs(v) for v in vals):
                warnings.append({
                    "type": "close_results",
                    "message": f"Models are very close on {metric_name} — "
                    "consider model complexity as a tiebreaker.",
                })

    # ── Write output ────────────────────────────────────────────
    report = {
        "report_type": "model_comparison",
        "summary": summary,
        "models": models_info,
        "comparison_table": comparison_table,
        "warnings": warnings,
    }

    output_path = _get_output_path("report")
    output_path.write_text(json.dumps(report, indent=2))

    return {"report": str(output_path)}
