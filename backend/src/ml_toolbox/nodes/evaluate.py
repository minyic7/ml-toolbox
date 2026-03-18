from pathlib import Path

from ml_toolbox.protocol import PortType, Text, node


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
    inputs={"model": PortType.MODEL, "test": PortType.TABLE},
    outputs={"metrics": PortType.METRICS},
    params={
        "target_column": Text(default=""),
    },
    label="Classification Metrics",
    category="Evaluate",
)
def classification(inputs: dict, params: dict) -> dict:
    """Evaluate a trained classifier on test data and return classification metrics."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.metrics import (
        accuracy_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column parameter is required")

    model = joblib.load(inputs["model"])
    df = pd.read_parquet(inputs["test"])

    y_true = df[target_column]
    X = df.drop(columns=[target_column])

    y_pred = model.predict(X)

    classes = np.unique(y_true)
    is_binary = len(classes) == 2

    metrics: dict = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, average="macro", zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, average="macro", zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
    }

    if is_binary and hasattr(model, "predict_proba"):
        y_proba = model.predict_proba(X)[:, 1]
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_proba))

    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))

    return {"metrics": str(metrics_path)}


@node(
    inputs={"model": PortType.MODEL, "test": PortType.TABLE},
    outputs={"metrics": PortType.METRICS},
    params={
        "target_column": Text(default=""),
    },
    label="Regression Metrics",
    category="Evaluate",
)
def regression(inputs: dict, params: dict) -> dict:
    """Evaluate a trained regression model on test data and return RMSE, MAE, R², and MAPE."""
    import json

    import joblib
    import pandas as pd
    from sklearn.metrics import (
        mean_absolute_error,
        mean_absolute_percentage_error,
        r2_score,
        root_mean_squared_error,
    )

    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column parameter is required")

    model = joblib.load(inputs["model"])
    df = pd.read_parquet(inputs["test"])

    y_true = df[target_column]
    X = df.drop(columns=[target_column])
    y_pred = model.predict(X)

    metrics = {
        "rmse": float(root_mean_squared_error(y_true, y_pred)),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
        "mape": float(mean_absolute_percentage_error(y_true, y_pred)),
    }

    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))

    return {"metrics": str(metrics_path)}
