"""XGBoost training node — auto-detects classification vs regression."""

from __future__ import annotations

from pathlib import Path

from ml_toolbox.protocol import PortType, Slider, Text, Toggle, node


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


_UPSTREAM = [
    "random_holdout", "stratified_holdout",
    "column_dropper", "missing_value_imputer", "category_encoder",
    "scaler_transform", "log_transform", "feature_selector",
    "interaction_creator", "datetime_encoder",
]


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
        "learning_rate": Slider(
            min=0.001,
            max=1.0,
            step=0.001,
            default=0.05,
            description="Step size shrinkage (eta) — lower values generalise better but need more rounds",
        ),
        "n_estimators": Slider(
            min=10,
            max=3000,
            step=10,
            default=500,
            description="Maximum number of boosting rounds (early stopping finds the optimal count)",
        ),
        "max_depth": Slider(
            min=1,
            max=15,
            step=1,
            default=6,
            description="Maximum tree depth — deeper trees capture more interactions but risk overfitting",
        ),
        "subsample": Slider(
            min=0.3,
            max=1.0,
            step=0.05,
            default=0.8,
            description="Fraction of training rows sampled per tree",
        ),
        "colsample_bytree": Slider(
            min=0.3,
            max=1.0,
            step=0.01,
            default=0.8,
            description="Fraction of features sampled per tree — reduces correlation between trees",
        ),
        "min_child_weight": Slider(
            min=1,
            max=20,
            step=1,
            default=1,
            description="Minimum sum of instance weight in a child — higher values = more conservative",
        ),
        "gamma": Slider(
            min=0,
            max=5.0,
            step=0.1,
            default=0,
            description="Minimum loss reduction required to make a split — acts as pre-pruning",
        ),
        "reg_alpha": Slider(
            min=0,
            max=10.0,
            step=0.1,
            default=0,
            description="L1 regularisation on leaf weights — encourages sparsity (feature selection effect)",
        ),
        "reg_lambda": Slider(
            min=0,
            max=10.0,
            step=0.1,
            default=1.0,
            description="L2 regularisation on leaf weights — smooths weights to prevent overfitting",
        ),
        "early_stopping": Toggle(
            default=True,
            description="Stop training when validation score stops improving (requires val connection)",
        ),
        "early_stopping_rounds": Slider(
            min=5,
            max=100,
            step=1,
            default=10,
            description="Patience — stop after N rounds without improvement",
        ),
    },
    label="XGBoost",
    category="Training",
    description="Train an XGBoost model with L1/L2 regularisation, column sampling, and early stopping. Auto-detects classification vs regression.",
    allowed_upstream={
        "train": _UPSTREAM,
        "val": _UPSTREAM,
        "test": _UPSTREAM,
    },
    guide="""## XGBoost

Extreme Gradient Boosting — an optimised, parallelised gradient boosting implementation.

### XGBoost vs sklearn Gradient Boosting

| Aspect | XGBoost | sklearn GradientBoosting |
|--------|---------|--------------------------|
| **Regularisation** | L1 + L2 on leaf weights | Shrinkage only |
| **Column sampling** | `colsample_bytree` per tree | Not available |
| **Missing values** | Built-in handling | Requires imputation |
| **Speed** | Multi-threaded | Single-threaded |
| **Early stopping** | Native with eval history | Basic |

### Key Parameters

| Parameter | What it does | Tuning guidance |
|-----------|-------------|-----------------|
| **learning_rate** | Controls how much each tree contributes | Start low (0.01–0.1), increase `n_estimators` to compensate |
| **max_depth** | Maximum tree depth | 3–8 for most problems. Deeper = more complex interactions |
| **subsample** | Row sampling per tree | 0.7–0.9 adds stochasticity, reduces overfitting |
| **colsample_bytree** | Feature sampling per tree | 0.6–0.9 decorrelates trees, especially with many features |
| **min_child_weight** | Minimum samples per leaf | Increase (5–20) for noisy data or to prevent overfitting |
| **gamma** | Minimum loss reduction for splits | 0.1–1.0 for pre-pruning, 0 = no restriction |
| **reg_alpha** | L1 (Lasso) penalty | > 0 pushes unimportant feature weights to zero |
| **reg_lambda** | L2 (Ridge) penalty | Default 1.0 usually fine; increase for strong regularisation |

### Early Stopping

When enabled, training monitors validation loss each round and stops when no improvement is seen for `early_stopping_rounds` consecutive rounds. The model reverts to the best iteration.

**Requirements:** toggle ON + connect a `val` input.

### Outputs

| Port | Type | Description |
|------|------|-------------|
| train/val/test_predictions | TABLE | y_true, y_pred (+ y_prob per class for classification) |
| model | MODEL | Trained XGBoost model (joblib-serialised) |
| report | METRICS | Training metadata, eval history (loss curve), feature importances |
""",
)
def xgboost_train(inputs: dict, params: dict) -> dict:
    """Train an XGBoost model with auto-detected task type."""
    import json
    import warnings
    from pathlib import Path
    from typing import Any

    import joblib
    import numpy as np
    import polars as pl
    import xgboost as xgb

    # ── Inline helpers ────────────────────────────────────────────

    def detect_task_type(y: np.ndarray) -> bool:
        """Return True if the target looks like a classification problem."""
        unique_values = np.unique(
            y[~np.isnan(y)] if np.issubdtype(y.dtype, np.floating) else y
        )
        n_unique = len(unique_values)
        if np.issubdtype(y.dtype, np.integer) and n_unique <= 20:
            return True
        if np.issubdtype(y.dtype, np.floating):
            if np.all(np.equal(np.mod(unique_values, 1), 0)) and n_unique <= 20:
                return True
        return False

    # ── Read training data ────────────────────────────────────────
    train_df = pl.read_parquet(inputs["train"])

    target_col = params.get("target_column", "")
    if not target_col or target_col not in train_df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            "Run auto-configure or set target_column manually."
        )

    # ── Split features / target ───────────────────────────────────
    feature_cols = [c for c in train_df.columns if c != target_col]
    X_train = np.asarray(train_df.select(feature_cols).to_pandas().values)
    y_train = np.asarray(train_df[target_col].to_pandas().values)

    # ── Auto-detect task type ─────────────────────────────────────
    is_classification = detect_task_type(y_train)
    task_type = "classification" if is_classification else "regression"
    warnings.warn(f"Auto-detected task type: {task_type}", stacklevel=1)

    # ── Read params ───────────────────────────────────────────────
    learning_rate = float(params.get("learning_rate", 0.05))
    n_estimators = int(params.get("n_estimators", 500))
    max_depth = int(params.get("max_depth", 6))
    subsample = float(params.get("subsample", 0.8))
    colsample_bytree = float(params.get("colsample_bytree", 0.8))
    min_child_weight = float(params.get("min_child_weight", 1))
    gamma = float(params.get("gamma", 0))
    reg_alpha = float(params.get("reg_alpha", 0))
    reg_lambda = float(params.get("reg_lambda", 1.0))
    early_stopping_enabled = bool(params.get("early_stopping", True))
    early_stopping_rounds = int(params.get("early_stopping_rounds", 10))

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
        early_stopping_enabled
        and X_val is not None
        and y_val is not None
    )

    # ── Determine eval metric ─────────────────────────────────────
    if is_classification:
        eval_metric = "logloss"
        eval_metric_direction = "minimize"
    else:
        eval_metric = "rmse"
        eval_metric_direction = "minimize"

    # ── Build model ───────────────────────────────────────────────
    xgb_params: dict[str, Any] = {
        "learning_rate": learning_rate,
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "subsample": subsample,
        "colsample_bytree": colsample_bytree,
        "min_child_weight": min_child_weight,
        "gamma": gamma,
        "reg_alpha": reg_alpha,
        "reg_lambda": reg_lambda,
        "random_state": 42,
        "verbosity": 0,
        "eval_metric": eval_metric,
    }
    if use_early_stopping:
        xgb_params["early_stopping_rounds"] = early_stopping_rounds

    if is_classification:
        model = xgb.XGBClassifier(**xgb_params)
    else:
        model = xgb.XGBRegressor(**xgb_params)

    # ── Train ─────────────────────────────────────────────────────
    fit_kwargs: dict[str, Any] = {}
    if X_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(X_train, y_train), (X_val, y_val)]
        fit_kwargs["verbose"] = False
    else:
        fit_kwargs["eval_set"] = [(X_train, y_train)]
        fit_kwargs["verbose"] = False

    model.fit(X_train, y_train, **fit_kwargs)

    # ── Helper: build prediction DataFrame ─────────────────────────
    def _build_predictions(split_df: pl.DataFrame) -> pl.DataFrame:
        X = split_df.select(feature_cols).to_pandas().values
        y_true = split_df[target_col].to_list()
        y_pred = model.predict(X)
        cols: dict = {"y_true": y_true, "y_pred": y_pred.tolist()}
        if is_classification and hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X)
            classes = model.classes_ if hasattr(model, "classes_") else np.unique(y_train)
            for i, cls in enumerate(classes):
                cols[f"y_prob_{cls}"] = y_prob[:, i].tolist()
        return pl.DataFrame(cols)

    # ── Write predictions per split ────────────────────────────────
    result: dict = {}
    sample_counts: dict = {"train": train_df.height}

    train_preds = _build_predictions(train_df)
    p = _get_output_path("train_predictions", ".parquet")
    train_preds.write_parquet(p)
    result["train_predictions"] = str(p)

    for split_name in ("val", "test"):
        split_path = inputs.get(split_name)
        if not split_path or not Path(split_path).exists():
            continue
        split_df = pl.read_parquet(split_path)
        if split_df.height == 0:
            continue
        preds = _build_predictions(split_df)
        p = _get_output_path(f"{split_name}_predictions", ".parquet")
        preds.write_parquet(p)
        result[f"{split_name}_predictions"] = str(p)
        sample_counts[split_name] = split_df.height

    # ── Feature importances (by gain) ─────────────────────────────
    importances = model.feature_importances_
    feature_importances = sorted(
        [{"feature": f, "importance": round(float(imp), 6)}
         for f, imp in zip(feature_cols, importances.tolist())],
        key=lambda x: x["importance"],
        reverse=True,
    )

    # ── Eval history (for training curve) ─────────────────────────
    eval_history: dict[str, list[float]] | None = None
    evals_result = getattr(model, "evals_result_", None) or {}
    if evals_result:
        history: dict[str, list[float]] = {}
        # XGBoost eval_set names: validation_0 = train, validation_1 = val
        key_map = {"validation_0": "train", "validation_1": "val"}
        for xgb_key, label in key_map.items():
            if xgb_key in evals_result and eval_metric in evals_result[xgb_key]:
                history[label] = [round(float(v), 6) for v in evals_result[xgb_key][eval_metric]]
        if history:
            eval_history = history

    # ── Training report ───────────────────────────────────────────
    # best_iteration is 0-indexed (XGBoost native), frontend renders +1
    best_iteration = getattr(model, "best_iteration", None)
    best_score = getattr(model, "best_score", None)

    report: dict[str, Any] = {
        "report_type": "training_report",
        "model_type": "xgboost",
        "task_type": task_type,
        "target_column": target_col,
        "feature_columns": feature_cols,
        "sample_counts": sample_counts,
        "params": {
            "learning_rate": learning_rate,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "subsample": subsample,
            "colsample_bytree": colsample_bytree,
            "min_child_weight": min_child_weight,
            "gamma": gamma,
            "reg_alpha": reg_alpha,
            "reg_lambda": reg_lambda,
            "early_stopping": early_stopping_enabled,
            "early_stopping_rounds": early_stopping_rounds,
        },
        "feature_importances": feature_importances,
        "importance_type": "gain",
        "xgb_version": xgb.__version__,
    }

    if eval_history:
        report["eval_history"] = eval_history
        report["eval_metric"] = eval_metric
        report["eval_metric_direction"] = eval_metric_direction

    if best_iteration is not None:
        # Store 0-indexed; n_estimators_used = best_iteration + 1
        report["best_iteration"] = int(best_iteration)
        report["n_estimators_used"] = int(best_iteration) + 1
    if best_score is not None:
        report["best_score"] = round(float(best_score), 6)

    if is_classification:
        classes = model.classes_ if hasattr(model, "classes_") else np.unique(y_train)
        report["classes"] = [int(c) if isinstance(c, (np.integer,)) else c for c in classes]
        report["target_distribution"] = {
            str(cls): int(np.sum(y_train == cls))
            for cls in classes
        }

    report_path = _get_output_path("report", ".json")
    report_path.write_text(json.dumps(report, indent=2))
    result["report"] = str(report_path)

    # ── Save model ────────────────────────────────────────────────
    model_path = _get_output_path("model", ".joblib")
    joblib.dump(model, model_path)
    result["model"] = str(model_path)

    return result
