"""Training nodes — Decision Tree and Random Forest."""

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


# ── Decision Tree ────────────────────────────────────────────────────


@node(
    inputs={"train": PortType.TABLE, "val": PortType.TABLE, "test": PortType.TABLE},
    outputs={
        "predictions": PortType.TABLE,
        "model": PortType.MODEL,
        "metrics": PortType.METRICS,
    },
    params={
        "target_column": Text(default="", description="Target column (auto-detected from schema)"),
        "max_depth": Slider(
            min=1, max=50, step=1, default=10,
            description="Maximum tree depth — primary regularization knob",
        ),
        "min_samples_split": Slider(
            min=2, max=20, step=1, default=2,
            description="Minimum samples required to split an internal node",
        ),
        "criterion": Select(
            options=["gini", "entropy", "squared_error", "absolute_error"],
            default="gini",
            description=(
                "Split quality metric. gini/entropy for classification, "
                "squared_error/absolute_error for regression (auto-corrected if mismatched)"
            ),
        ),
    },
    label="Decision Tree",
    category="Training",
    description=(
        "Train a Decision Tree model. Auto-detects classification vs regression "
        "from the target column data."
    ),
    allowed_upstream={
        "train": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "scaler_transform",
            "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "scaler_transform",
            "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "scaler_transform",
            "category_encoder",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Decision Tree

Train a **Decision Tree** for classification or regression. The task type is
auto-detected from the target column data.

### Pros
- **Highly interpretable** — the tree structure can be visualised and explained
- **No feature scaling needed** — splits are invariant to monotonic transforms
- **Handles both numeric and categorical targets**

### Cons
- **Prone to overfitting** — especially with deep trees
- **High variance** — small data changes can produce very different trees

### Parameters
| Parameter | Purpose |
|-----------|---------|
| `max_depth` | Maximum tree depth — the **primary regularization knob**. Lower values reduce overfitting. |
| `min_samples_split` | Minimum samples to allow a split. Raising this also reduces overfitting. |
| `criterion` | Split metric. Auto-corrected to match the detected task type. |

### Auto-detection
The node detects the task type from the target column data:
- **integer with ≤ 20 unique values** → classification
- **float** → regression
""",
)
def decision_tree(inputs: dict, params: dict) -> dict:
    """Train a Decision Tree — auto-detect classification vs regression."""
    import json
    from pathlib import Path

    import polars as pl
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        mean_absolute_error,
        mean_squared_error,
        precision_score,
        r2_score,
        recall_score,
    )
    from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor

    # ── Read training data ────────────────────────────────────────
    train_df = pl.read_parquet(inputs["train"])
    target_col = params.get("target_column", "")

    if not target_col or target_col not in train_df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            "Target column not specified. Run auto-configure or set target_column manually."
        )

    # ── Detect task type ──────────────────────────────────────────
    import warnings

    dtype = train_df[target_col].dtype
    if dtype in (pl.Float32, pl.Float64):
        task_type = "regression"
    elif train_df[target_col].n_unique() <= 20:
        task_type = "classification"
    else:
        task_type = "regression"

    # ── Resolve criterion ─────────────────────────────────────────
    criterion = params.get("criterion", "gini")
    if task_type == "classification" and criterion not in ("gini", "entropy"):
        warnings.warn(
            f"Criterion '{criterion}' is not valid for classification — "
            f"falling back to 'gini'",
            stacklevel=1,
        )
        criterion = "gini"
    elif task_type == "regression" and criterion not in ("squared_error", "absolute_error"):
        warnings.warn(
            f"Criterion '{criterion}' is not valid for regression — "
            f"falling back to 'squared_error'",
            stacklevel=1,
        )
        criterion = "squared_error"

    max_depth = int(params.get("max_depth", 10))
    min_samples_split = int(params.get("min_samples_split", 2))

    # ── Prepare features / target ─────────────────────────────────
    feature_cols = [c for c in train_df.columns if c != target_col]
    X_train = train_df.select(feature_cols).to_pandas()
    y_train = train_df[target_col].to_pandas()

    # ── Build and fit model ───────────────────────────────────────
    if task_type == "classification":
        model = DecisionTreeClassifier(
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            criterion=criterion,
            random_state=42,
        )
    else:
        model = DecisionTreeRegressor(
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            criterion=criterion,
            random_state=42,
        )

    model.fit(X_train, y_train)

    # ── Generate predictions and metrics for each split ───────────
    all_predictions: list[pl.DataFrame] = []
    metrics: dict = {"task_type": task_type}

    for split_name in ("train", "val", "test"):
        input_path = inputs.get(split_name)
        if not input_path:
            continue

        split_path = Path(input_path)
        if not split_path.exists():
            continue

        df = train_df if split_name == "train" else pl.read_parquet(split_path)
        if df.height == 0:
            continue

        X = df.select(feature_cols).to_pandas()
        y_true = df[target_col].to_pandas()
        y_pred = model.predict(X)

        # Build predictions frame
        pred_df = df.with_columns(
            pl.Series("prediction", y_pred),
            pl.Series("split", [split_name] * df.height),
        )
        all_predictions.append(pred_df)

        # Compute metrics
        if task_type == "classification":
            avg = "weighted" if y_true.nunique() > 2 else "binary"
            metrics[split_name] = {
                "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
                "precision": round(float(precision_score(y_true, y_pred, average=avg, zero_division=0.0)), 4),  # type: ignore[arg-type]
                "recall": round(float(recall_score(y_true, y_pred, average=avg, zero_division=0.0)), 4),  # type: ignore[arg-type]
                "f1": round(float(f1_score(y_true, y_pred, average=avg, zero_division=0.0)), 4),  # type: ignore[arg-type]
            }
        else:
            metrics[split_name] = {
                "mse": round(float(mean_squared_error(y_true, y_pred)), 4),
                "mae": round(float(mean_absolute_error(y_true, y_pred)), 4),
                "r2": round(float(r2_score(y_true, y_pred)), 4),
            }

    # ── Write predictions ─────────────────────────────────────────
    predictions_df = pl.concat(all_predictions) if all_predictions else pl.DataFrame()
    predictions_path = _get_output_path("predictions", ".parquet")
    predictions_df.write_parquet(predictions_path)

    # ── Write metrics ─────────────────────────────────────────────
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics, indent=2))

    # ── Return model as raw object (auto-serialized by runner) ────
    return {
        "predictions": str(predictions_path),
        "model": model,
        "metrics": str(metrics_path),
    }


# ── Random Forest ────────────────────────────────────────────────────


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
        "n_estimators": Slider(
            min=10,
            max=500,
            step=10,
            default=100,
            description="Number of trees in the forest. More trees = better accuracy but slower training.",
        ),
        "max_depth": Slider(
            min=1,
            max=50,
            step=1,
            default=10,
            description="Maximum depth of each tree. Higher = more complex model, risk of overfitting.",
        ),
        "min_samples_split": Slider(
            min=2,
            max=20,
            step=1,
            default=2,
            description="Minimum samples required to split an internal node.",
        ),
        "n_jobs": Text(
            default="-1",
            description="Number of parallel jobs (-1 = use all cores)",
            placeholder="-1",
        ),
    },
    label="Random Forest",
    category="Training",
    description="Train a Random Forest model (classifier or regressor). Auto-detects task type from target column data.",
    allowed_upstream={
        "train": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "scaler_transform",
            "log_transform", "interaction_creator", "datetime_encoder",
        ],
    },
    guide="""## Random Forest

A **Random Forest** is an ensemble of decision trees trained on random subsets of the data (bagging). Each tree votes on the prediction, and the forest aggregates the votes — majority vote for classification, average for regression.

### Why Random Forest?
- **Robust out-of-the-box** — works well without much tuning
- **Handles mixed feature types** — numeric and categorical (after encoding)
- **Built-in feature importance** — see which features drive predictions
- **Resistant to overfitting** — thanks to bagging and random feature selection

### Key Parameters

| Parameter | Effect | Guidance |
|-----------|--------|----------|
| **n_estimators** | Number of trees | More trees → better accuracy but slower. 100–300 is usually enough; diminishing returns beyond that. |
| **max_depth** | Max tree depth | Controls model complexity. Lower = simpler model, less overfitting. Start with 10, increase if underfitting. |
| **min_samples_split** | Min samples to split a node | Higher values prevent the tree from learning overly specific patterns. |
| **n_jobs** | Parallel workers | `-1` uses all CPU cores. Set to `1` for debugging. |

### Auto-detection
The node detects the task type from the target column data:
- **integer with ≤ 20 unique values** → `RandomForestClassifier`
- **float / many unique values** → `RandomForestRegressor`

### Feature Importance
The output `metrics.json` includes `feature_importances` — a ranked list of features by importance (Gini importance). The **Feature Importance** evaluation node can read and visualize this.

### Inputs / Outputs
| Port | Type | Required | Description |
|------|------|----------|-------------|
| train | TABLE | Yes | Training data with features + target |
| val | TABLE | No | Validation split — predictions + metrics computed if connected |
| test | TABLE | No | Test split — predictions + metrics computed if connected |
| predictions | TABLE | Out | Predictions for all connected splits (stacked) |
| model | MODEL | Out | Trained sklearn model (joblib-serialized) |
| metrics | METRICS | Out | Accuracy/RMSE per split + feature importances |
""",
)
def random_forest(inputs: dict, params: dict) -> dict:
    """Train a Random Forest model — auto-detects classification vs regression."""
    import json
    from pathlib import Path

    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        mean_absolute_error,
        mean_squared_error,
        r2_score,
    )

    n_estimators = int(params.get("n_estimators", 100))
    max_depth = int(params.get("max_depth", 10))
    min_samples_split = int(params.get("min_samples_split", 2))
    n_jobs_str = str(params.get("n_jobs", "-1")).strip()
    n_jobs = int(n_jobs_str) if n_jobs_str else -1

    # ── Read train data ──────────────────────────────────────────
    train_df = pd.read_parquet(inputs["train"])

    # ── Read target column from params ───────────────────────────
    target_col = params.get("target_column", "")

    if not target_col or target_col not in train_df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            "Target column not specified. Run auto-configure or set target_column manually."
        )

    # ── Auto-detect task type from data ──────────────────────────
    y_dtype = train_df[target_col].dtype
    n_unique = train_df[target_col].nunique()
    is_classification = (
        pd.api.types.is_integer_dtype(y_dtype) and n_unique <= 20
    ) or (
        pd.api.types.is_float_dtype(y_dtype)
        and (train_df[target_col].dropna() % 1 == 0).all()
        and n_unique <= 20
    )

    # ── Prepare features and target ──────────────────────────────
    feature_cols = [c for c in train_df.columns if c != target_col]
    X_train = train_df[feature_cols]
    y_train = train_df[target_col]

    # ── Train model ──────────────────────────────────────────────
    if is_classification:
        model = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            n_jobs=n_jobs,
            random_state=42,
        )
    else:
        model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            n_jobs=n_jobs,
            random_state=42,
        )

    model.fit(X_train, y_train)

    # ── Compute metrics per split ────────────────────────────────
    def _evaluate(df: pd.DataFrame, split_name: str) -> tuple[dict, pd.DataFrame]:
        X = df[feature_cols]
        y_true = df[target_col]
        y_pred = model.predict(X)

        pred_df = df.copy()
        pred_df["prediction"] = y_pred
        pred_df["split"] = split_name

        if is_classification:
            return {
                "accuracy": float(accuracy_score(y_true, y_pred)),
                "f1_weighted": float(f1_score(y_true, y_pred, average="weighted", zero_division="warn")),
            }, pred_df
        else:
            return {
                "rmse": float(mean_squared_error(y_true, y_pred) ** 0.5),
                "mae": float(mean_absolute_error(y_true, y_pred)),
                "r2": float(r2_score(y_true, y_pred)),
            }, pred_df

    all_predictions: list[pd.DataFrame] = []
    split_metrics: dict[str, dict] = {}

    # Train metrics
    train_metrics, train_preds = _evaluate(train_df, "train")
    split_metrics["train_metrics"] = train_metrics
    all_predictions.append(train_preds)

    # Val metrics (optional)
    if inputs.get("val"):
        val_path = Path(inputs["val"])
        if val_path.exists():
            val_df = pd.read_parquet(val_path)
            val_metrics, val_preds = _evaluate(val_df, "val")
            split_metrics["val_metrics"] = val_metrics
            all_predictions.append(val_preds)

    # Test metrics (optional)
    if inputs.get("test"):
        test_path = Path(inputs["test"])
        if test_path.exists():
            test_df = pd.read_parquet(test_path)
            test_metrics, test_preds = _evaluate(test_df, "test")
            split_metrics["test_metrics"] = test_metrics
            all_predictions.append(test_preds)

    # ── Feature importances ──────────────────────────────────────
    importances = model.feature_importances_
    feature_importances = sorted(
        [{"feature": f, "importance": float(imp)} for f, imp in zip(feature_cols, importances)],
        key=lambda x: x["importance"],
        reverse=True,
    )

    # ── Build summary ────────────────────────────────────────────
    # Use val metrics for summary if available, else train
    summary_source = split_metrics.get("val_metrics", split_metrics["train_metrics"])

    metrics_output = {
        "report_type": "random_forest",
        "task": "classification" if is_classification else "regression",
        "summary": summary_source,
        **split_metrics,
        "feature_importances": feature_importances,
        "params": {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "min_samples_split": min_samples_split,
            "n_jobs": n_jobs,
        },
    }

    # ── Write outputs ────────────────────────────────────────────
    # Predictions
    predictions_df = pd.concat(all_predictions, ignore_index=True)
    pred_path = _get_output_path("predictions", ".parquet")
    predictions_df.to_parquet(pred_path, index=False)

    # Metrics
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics_output, indent=2))

    return {
        "predictions": str(pred_path),
        "model": model,
        "metrics": str(metrics_path),
    }
