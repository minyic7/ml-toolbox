"""Evaluation nodes for classification and regression tasks."""

from __future__ import annotations

import json
from pathlib import Path
from ml_toolbox.protocol import PortType, Toggle, node


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
    category="Evaluate",
    description="Compute confusion matrix with per-class precision, recall, and F1 for classification tasks.",
    allowed_upstream={
        "predictions": ["random_holdout"],
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
    import json
    from pathlib import Path

    import numpy as np
    import pandas as pd

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
