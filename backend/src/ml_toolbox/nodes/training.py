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
        "train_predictions": PortType.TABLE,
        "val_predictions": PortType.TABLE,
        "test_predictions": PortType.TABLE,
        "model": PortType.MODEL,
        "report": PortType.METRICS,
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
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
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

### Outputs
| Port | Type | Description |
|------|------|-------------|
| train_predictions | TABLE | y_true, y_pred (+ y_prob per class for classification) on train split |
| val_predictions | TABLE | Same format on validation split (if connected) |
| test_predictions | TABLE | Same format on test split (if connected) |
| model | MODEL | Trained sklearn model (joblib-serialized) |
| report | METRICS | Training metadata: task type, features, sample counts, feature importances |
""",
)
def decision_tree(inputs: dict, params: dict) -> dict:
    """Train a Decision Tree — auto-detect classification vs regression."""
    import json
    import warnings
    from pathlib import Path

    import polars as pl
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
    dtype = train_df[target_col].dtype
    if dtype in (pl.Float32, pl.Float64):
        task_type = "regression"
    elif train_df[target_col].n_unique() <= 20:
        task_type = "classification"
    else:
        task_type = "regression"

    is_classification = task_type == "classification"

    # ── Resolve criterion ─────────────────────────────────────────
    criterion = params.get("criterion", "gini")
    if is_classification and criterion not in ("gini", "entropy"):
        warnings.warn(
            f"Criterion '{criterion}' is not valid for classification — "
            f"falling back to 'gini'",
            stacklevel=1,
        )
        criterion = "gini"
    elif not is_classification and criterion not in ("squared_error", "absolute_error"):
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
    if is_classification:
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

    # ── Helper: build prediction DataFrame ─────────────────────────
    def _build_predictions(df: pl.DataFrame) -> pl.DataFrame:
        X = df.select(feature_cols).to_pandas()
        y_true = df[target_col]
        y_pred = model.predict(X)
        cols: dict = {"y_true": y_true, "y_pred": y_pred}
        if is_classification and hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X)
            for i, cls in enumerate(model.classes_):
                cols[f"y_prob_{cls}"] = y_prob[:, i]
        return pl.DataFrame(cols)

    # ── Write predictions per split ────────────────────────────────
    result: dict = {}
    sample_counts: dict = {"train": train_df.height}

    train_preds = _build_predictions(train_df)
    p = _get_output_path("train_predictions", ".parquet")
    train_preds.write_parquet(p)
    result["train_predictions"] = str(p)

    for split_name in ("val", "test"):
        if inputs.get(split_name) and Path(inputs[split_name]).exists():
            split_df = pl.read_parquet(inputs[split_name])
            if split_df.height > 0:
                preds = _build_predictions(split_df)
                p = _get_output_path(f"{split_name}_predictions", ".parquet")
                preds.write_parquet(p)
                result[f"{split_name}_predictions"] = str(p)
                sample_counts[split_name] = split_df.height

    # ── Training report ───────────────────────────────────────────
    report: dict = {
        "report_type": "training_report",
        "model_type": "decision_tree",
        "task_type": task_type,
        "target_column": target_col,
        "feature_columns": feature_cols,
        "sample_counts": sample_counts,
        "params": {
            "max_depth": max_depth,
            "min_samples_split": min_samples_split,
            "criterion": criterion,
        },
    }

    if is_classification:
        classes = model.classes_.tolist()
        report["classes"] = classes
        target_series = train_df[target_col]
        report["target_distribution"] = {
            str(cls): int((target_series == cls).sum())
            for cls in classes
        }

    importances = model.feature_importances_
    report["feature_importances"] = sorted(
        [{"feature": f, "importance": round(float(imp), 6)}
         for f, imp in zip(feature_cols, importances)],
        key=lambda x: x["importance"],
        reverse=True,
    )

    report_path = _get_output_path("report", ".json")
    report_path.write_text(json.dumps(report, indent=2))
    result["report"] = str(report_path)

    result["model"] = model
    return result


# ── Random Forest ────────────────────────────────────────────────────


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
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
        "val": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
        ],
        "test": [
            "random_holdout", "stratified_holdout",
            "column_dropper", "missing_value_imputer", "category_encoder",
            "scaler_transform", "log_transform", "feature_selector",
            "interaction_creator", "datetime_encoder",
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

### Outputs
| Port | Type | Description |
|------|------|-------------|
| train_predictions | TABLE | y_true, y_pred (+ y_prob per class for classification) on train split |
| val_predictions | TABLE | Same format on validation split (if connected) |
| test_predictions | TABLE | Same format on test split (if connected) |
| model | MODEL | Trained sklearn model (joblib-serialized) |
| report | METRICS | Training metadata: task type, features, sample counts, feature importances |
""",
)
def random_forest(inputs: dict, params: dict) -> dict:
    """Train a Random Forest model — auto-detects classification vs regression."""
    import json
    from pathlib import Path

    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

    n_estimators = int(params.get("n_estimators", 100))
    max_depth = int(params.get("max_depth", 10))
    min_samples_split = int(params.get("min_samples_split", 2))
    n_jobs_str = str(params.get("n_jobs", "-1")).strip()
    n_jobs = int(n_jobs_str) if n_jobs_str else -1

    # ── Read train data ──────────────────────────────────────────
    train_df = pd.read_parquet(inputs["train"])

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
    task_type = "classification" if is_classification else "regression"

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

    # ── Helper: build prediction DataFrame ─────────────────────────
    def _build_predictions(df: pd.DataFrame) -> pd.DataFrame:
        X = df[feature_cols]
        y_pred = model.predict(X)
        pred_data: dict = {"y_true": df[target_col].values, "y_pred": y_pred}
        if is_classification and hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X)
            for i, cls in enumerate(model.classes_):
                pred_data[f"y_prob_{cls}"] = y_prob[:, i]
        return pd.DataFrame(pred_data)

    # ── Write predictions per split ────────────────────────────────
    result: dict = {}
    sample_counts: dict = {"train": len(train_df)}

    train_preds = _build_predictions(train_df)
    p = _get_output_path("train_predictions", ".parquet")
    train_preds.to_parquet(p, index=False)
    result["train_predictions"] = str(p)

    for split_name in ("val", "test"):
        if inputs.get(split_name):
            split_path = Path(inputs[split_name])
            if split_path.exists():
                split_df = pd.read_parquet(split_path)
                if len(split_df) > 0:
                    preds = _build_predictions(split_df)
                    p = _get_output_path(f"{split_name}_predictions", ".parquet")
                    preds.to_parquet(p, index=False)
                    result[f"{split_name}_predictions"] = str(p)
                    sample_counts[split_name] = len(split_df)

    # ── Feature importances ──────────────────────────────────────
    importances = model.feature_importances_
    feature_importances = sorted(
        [{"feature": f, "importance": round(float(imp), 6)}
         for f, imp in zip(feature_cols, importances)],
        key=lambda x: x["importance"],
        reverse=True,
    )

    # ── Training report ──────────────────────────────────────────
    report: dict = {
        "report_type": "training_report",
        "model_type": "random_forest",
        "task_type": task_type,
        "target_column": target_col,
        "feature_columns": feature_cols,
        "sample_counts": sample_counts,
        "params": {
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "min_samples_split": min_samples_split,
            "n_jobs": n_jobs,
        },
        "feature_importances": feature_importances,
    }

    if is_classification:
        classes = model.classes_.tolist()
        report["classes"] = classes
        report["target_distribution"] = {
            str(cls): int((train_df[target_col] == cls).sum())
            for cls in classes
        }

    report_path = _get_output_path("report", ".json")
    report_path.write_text(json.dumps(report, indent=2))
    result["report"] = str(report_path)

    result["model"] = model
    return result
