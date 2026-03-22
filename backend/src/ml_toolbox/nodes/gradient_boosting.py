"""Gradient Boosting training node — auto-detects classification vs regression."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from ml_toolbox.protocol import PortType, Slider, node

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
        "learning_rate": Slider(
            min=0.001,
            max=1.0,
            step=0.001,
            default=0.1,
            description="Step size shrinkage — lower values need more estimators but generalise better",
        ),
        "n_estimators": Slider(
            min=10,
            max=1000,
            step=10,
            default=100,
            description="Maximum number of boosting rounds",
        ),
        "max_depth": Slider(
            min=1,
            max=20,
            step=1,
            default=6,
            description="Maximum tree depth — deeper trees capture more interactions but risk overfitting",
        ),
        "subsample": Slider(
            min=0.5,
            max=1.0,
            step=0.05,
            default=1.0,
            description="Fraction of training rows sampled per tree (< 1.0 = stochastic boosting)",
        ),
        "early_stopping_rounds": Slider(
            min=0,
            max=50,
            step=1,
            default=0,
            description="Stop if validation score doesn't improve for N rounds (0 = disabled, requires val connection)",
        ),
    },
    label="Gradient Boosting",
    category="Training",
    description="Train a gradient boosting model (XGBoost with sklearn fallback). Auto-detects classification vs regression from the target column.",
    allowed_upstream={
        "train": [
            "random_holdout",
            "column_dropper",
            "missing_value_imputer",
            "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout",
            "column_dropper",
            "missing_value_imputer",
            "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout",
            "column_dropper",
            "missing_value_imputer",
            "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Gradient Boosting

Gradient boosting builds an ensemble of **sequential decision trees**, where each tree corrects the errors of the previous ones. Unlike random forests (which train trees independently in parallel), boosting focuses on hard-to-predict examples.

### Gradient Boosting vs Random Forest

| Aspect | Gradient Boosting | Random Forest |
|--------|-------------------|---------------|
| **Training** | Sequential — each tree depends on prior errors | Parallel — trees are independent |
| **Accuracy** | Often higher with proper tuning | Good out-of-the-box |
| **Overfitting** | More prone — use early stopping + lower learning rate | Naturally resistant (bagging) |
| **Speed** | Slower to train (sequential) | Faster (parallelisable) |

### Key Parameter Trade-offs

| Parameter | Effect |
|-----------|--------|
| **learning_rate** | Lower values (0.01–0.1) learn slowly but generalise better. Requires more `n_estimators` to compensate |
| **n_estimators** | More trees = more capacity. With early stopping, set high and let validation decide when to stop |
| **max_depth** | Controls interaction depth. Shallow trees (3–6) often work well; deeper trees risk overfitting |
| **subsample** | < 1.0 adds stochasticity (stochastic gradient boosting), reducing variance at the cost of slightly more bias |
| **early_stopping_rounds** | Connect a validation set and set > 0 to automatically stop training when the model stops improving |

### Early Stopping

Early stopping monitors performance on the **validation set** and halts training when no improvement is seen for N consecutive rounds. This prevents overfitting and finds the optimal number of trees automatically.

**Requirements:** connect a `val` input AND set `early_stopping_rounds > 0`.

### Auto-detection

The node automatically detects whether to run **classification** or **regression** based on the target column:
- Integer target with ≤ 20 unique values → classification
- Otherwise → regression
""",
)
def gradient_boosting_train(inputs: dict, params: dict) -> dict:
    """Train a gradient boosting model with auto-detected task type."""
    import joblib

    # ── Read training data ────────────────────────────────────────
    train_df = pl.read_parquet(inputs["train"])

    # ── Read .meta.json sidecar to find target column ─────────────
    meta_path = Path(inputs["train"]).with_suffix(".meta.json")
    target_col: str | None = None
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            target_col = meta.get("target")
        except Exception:
            pass

    if target_col is None or target_col not in train_df.columns:
        raise ValueError(
            "Target column not found. Ensure upstream node produces a .meta.json "
            "sidecar with a 'target' field."
        )

    # ── Split features / target ───────────────────────────────────
    feature_cols = [c for c in train_df.columns if c != target_col]
    X_train: np.ndarray = np.asarray(train_df.select(feature_cols).to_pandas().values)
    y_train: np.ndarray = np.asarray(train_df[target_col].to_pandas().values)

    # ── Auto-detect task type ─────────────────────────────────────
    is_classification = _detect_task_type(y_train)
    task_type = "classification" if is_classification else "regression"
    logger.info("Auto-detected task type: %s", task_type)

    # ── Read params ───────────────────────────────────────────────
    learning_rate = float(params.get("learning_rate", 0.1))
    n_estimators = int(params.get("n_estimators", 100))
    max_depth = int(params.get("max_depth", 6))
    subsample = float(params.get("subsample", 1.0))
    early_stopping_rounds = int(params.get("early_stopping_rounds", 0))

    # ── Prepare validation data (if connected) ────────────────────
    val_path = inputs.get("val")
    X_val: np.ndarray | None = None
    y_val: np.ndarray | None = None
    if val_path and Path(val_path).exists():
        val_df = pl.read_parquet(val_path)
        if val_df.height > 0:
            X_val = np.asarray(val_df.select(feature_cols).to_pandas().values)
            y_val = np.asarray(val_df[target_col].to_pandas().values)

    # ── Determine early stopping ──────────────────────────────────
    use_early_stopping = (
        early_stopping_rounds > 0
        and X_val is not None
        and y_val is not None
    )

    # ── Build and train model ─────────────────────────────────────
    model: Any = _build_model(
        is_classification=is_classification,
        learning_rate=learning_rate,
        n_estimators=n_estimators,
        max_depth=max_depth,
        subsample=subsample,
        early_stopping_rounds=early_stopping_rounds if use_early_stopping else 0,
        X_val=X_val,
        y_val=y_val,
        use_early_stopping=use_early_stopping,
    )

    model.fit(
        X_train,
        y_train,
        **_fit_kwargs(model, X_val, y_val, use_early_stopping),
    )

    # ── Generate predictions on all splits ────────────────────────
    all_predictions: list[pl.DataFrame] = []
    for split_name in ("train", "val", "test"):
        split_path = inputs.get(split_name)
        if not split_path or not Path(split_path).exists():
            continue
        split_df = pl.read_parquet(split_path)
        if split_df.height == 0:
            continue
        X_split = split_df.select(feature_cols).to_pandas().values
        preds = model.predict(X_split)
        pred_df = pl.DataFrame({
            "split": [split_name] * len(preds),
            "prediction": preds.tolist(),
            "actual": split_df[target_col].to_list(),
        })
        all_predictions.append(pred_df)

    predictions_df = pl.concat(all_predictions) if all_predictions else pl.DataFrame()

    # ── Compute metrics ───────────────────────────────────────────
    metrics = _compute_metrics(
        model=model,
        X_train=X_train,
        y_train=y_train,
        X_val=X_val,
        y_val=y_val,
        feature_cols=feature_cols,
        is_classification=is_classification,
        task_type=task_type,
    )

    # ── Save outputs ──────────────────────────────────────────────
    pred_path = _get_output_path("predictions", ".parquet")
    predictions_df.write_parquet(pred_path)

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(model, model_path)

    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics, indent=2))

    return {
        "predictions": str(pred_path),
        "model": str(model_path),
        "metrics": str(metrics_path),
    }


# ── Helpers ───────────────────────────────────────────────────────


def _detect_task_type(y: np.ndarray) -> bool:
    """Return True if the target looks like a classification problem."""
    unique_values = np.unique(y[~np.isnan(y)] if np.issubdtype(y.dtype, np.floating) else y)
    n_unique = len(unique_values)
    # Integer-like target with few unique values → classification
    if np.issubdtype(y.dtype, np.integer) and n_unique <= 20:
        return True
    if np.issubdtype(y.dtype, np.floating):
        # Check if all values are integer-valued floats with few unique
        if np.all(np.equal(np.mod(unique_values, 1), 0)) and n_unique <= 20:
            return True
    return False


def _build_model(
    *,
    is_classification: bool,
    learning_rate: float,
    n_estimators: int,
    max_depth: int,
    subsample: float,
    early_stopping_rounds: int,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    use_early_stopping: bool,
) -> Any:
    """Instantiate an XGBoost model, falling back to sklearn if unavailable."""
    try:
        import xgboost as xgb

        xgb_params: dict = {
            "learning_rate": learning_rate,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "subsample": subsample,
            "random_state": 42,
            "verbosity": 0,
        }

        if use_early_stopping:
            xgb_params["early_stopping_rounds"] = early_stopping_rounds

        if is_classification:
            return xgb.XGBClassifier(**xgb_params)
        return xgb.XGBRegressor(**xgb_params)

    except ImportError:
        logger.warning("XGBoost not available, falling back to sklearn GradientBoosting")
        from sklearn.ensemble import (
            GradientBoostingClassifier,
            GradientBoostingRegressor,
        )

        sklearn_params: dict = {
            "learning_rate": learning_rate,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "subsample": subsample,
            "random_state": 42,
        }

        if is_classification:
            return GradientBoostingClassifier(**sklearn_params)
        return GradientBoostingRegressor(**sklearn_params)


def _fit_kwargs(
    model: Any,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    use_early_stopping: bool,
) -> dict:
    """Build keyword arguments for model.fit()."""
    kwargs: dict = {}

    if not use_early_stopping or X_val is None or y_val is None:
        return kwargs

    # XGBoost uses eval_set for early stopping
    try:
        import xgboost  # noqa: F401
        kwargs["eval_set"] = [(X_val, y_val)]
        kwargs["verbose"] = False
    except ImportError:
        # sklearn GradientBoosting doesn't support early stopping via fit()
        # (would need warm_start + manual loop — not worth the complexity)
        pass

    return kwargs


def _compute_metrics(
    *,
    model: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray | None,
    y_val: np.ndarray | None,
    feature_cols: list[str],
    is_classification: bool,
    task_type: str,
) -> dict:
    """Compute training/validation metrics and feature importances."""
    metrics: dict = {"task_type": task_type}

    # Train score
    metrics["train_score"] = float(model.score(X_train, y_train))
    # Validation score
    if X_val is not None and y_val is not None:
        metrics["val_score"] = float(model.score(X_val, y_val))
    # Classification-specific metrics
    if is_classification:
        from sklearn.metrics import accuracy_score, f1_score

        train_preds = model.predict(X_train)
        metrics["train_accuracy"] = float(accuracy_score(y_train, train_preds))

        n_classes = len(np.unique(y_train))
        average = "binary" if n_classes == 2 else "weighted"
        metrics["train_f1"] = float(f1_score(y_train, train_preds, average=average))

        if X_val is not None and y_val is not None:
            val_preds = model.predict(X_val)
            metrics["val_accuracy"] = float(accuracy_score(y_val, val_preds))
            metrics["val_f1"] = float(f1_score(y_val, val_preds, average=average))
    else:
        from sklearn.metrics import mean_absolute_error, mean_squared_error

        train_preds = model.predict(X_train)
        metrics["train_mse"] = float(mean_squared_error(y_train, train_preds))
        metrics["train_mae"] = float(mean_absolute_error(y_train, train_preds))

        if X_val is not None and y_val is not None:
            val_preds = model.predict(X_val)
            metrics["val_mse"] = float(mean_squared_error(y_val, val_preds))
            metrics["val_mae"] = float(mean_absolute_error(y_val, val_preds))

    # Feature importances
    importances = model.feature_importances_
    importance_pairs = sorted(
        zip(feature_cols, importances.tolist()),
        key=lambda x: x[1],
        reverse=True,
    )
    metrics["feature_importances"] = {name: round(imp, 6) for name, imp in importance_pairs}

    # Best iteration (XGBoost early stopping)
    best_iteration = getattr(model, "best_iteration", None)
    if best_iteration is not None:
        metrics["best_iteration"] = int(best_iteration)
        metrics["n_estimators_used"] = int(best_iteration) + 1

    return metrics
