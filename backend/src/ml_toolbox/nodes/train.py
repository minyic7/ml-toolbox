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


_ESTIMATOR_MAP = {
    "LinearRegression": ("sklearn.linear_model", "LinearRegression"),
    "LogisticRegression": ("sklearn.linear_model", "LogisticRegression"),
    "RandomForestClassifier": ("sklearn.ensemble", "RandomForestClassifier"),
    "RandomForestRegressor": ("sklearn.ensemble", "RandomForestRegressor"),
    "GradientBoostingClassifier": ("sklearn.ensemble", "GradientBoostingClassifier"),
    "GradientBoostingRegressor": ("sklearn.ensemble", "GradientBoostingRegressor"),
    "SVC": ("sklearn.svm", "SVC"),
    "SVR": ("sklearn.svm", "SVR"),
    "KNeighborsClassifier": ("sklearn.neighbors", "KNeighborsClassifier"),
    "DecisionTreeClassifier": ("sklearn.tree", "DecisionTreeClassifier"),
}

_REGRESSION_OBJECTIVES = {"reg:squarederror"}


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "estimator": Select(
            [
                "LinearRegression",
                "LogisticRegression",
                "RandomForestClassifier",
                "RandomForestRegressor",
                "GradientBoostingClassifier",
                "GradientBoostingRegressor",
                "SVC",
                "SVR",
                "KNeighborsClassifier",
                "DecisionTreeClassifier",
            ],
            default="RandomForestClassifier",
            description="Scikit-learn algorithm to train",
        ),
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "hyperparams": Text(default="{}", description="JSON dict of estimator hyperparameters", placeholder='{"n_estimators": 100}'),
    },
    label="Train sklearn Model",
    category="Train",
)
def sklearn_train(inputs: dict, params: dict) -> dict:
    """Instantiate a sklearn estimator, fit on training data, and return model + metrics."""
    import importlib
    import json

    import joblib
    import numpy as np
    import pandas as pd

    _ESTIMATOR_MAP = {
        "LinearRegression": ("sklearn.linear_model", "LinearRegression"),
        "LogisticRegression": ("sklearn.linear_model", "LogisticRegression"),
        "RandomForestClassifier": ("sklearn.ensemble", "RandomForestClassifier"),
        "RandomForestRegressor": ("sklearn.ensemble", "RandomForestRegressor"),
        "GradientBoostingClassifier": ("sklearn.ensemble", "GradientBoostingClassifier"),
        "GradientBoostingRegressor": ("sklearn.ensemble", "GradientBoostingRegressor"),
        "SVC": ("sklearn.svm", "SVC"),
        "SVR": ("sklearn.svm", "SVR"),
        "KNeighborsClassifier": ("sklearn.neighbors", "KNeighborsClassifier"),
        "DecisionTreeClassifier": ("sklearn.tree", "DecisionTreeClassifier"),
    }

    df = pd.read_parquet(inputs["train"])

    estimator_name = params.get("estimator", "RandomForestClassifier")
    target_column = params.get("target_column", "")
    hyperparams_str = params.get("hyperparams", "{}")

    if estimator_name not in _ESTIMATOR_MAP:
        raise ValueError(
            f"Unknown estimator '{estimator_name}'. "
            f"Supported: {sorted(_ESTIMATOR_MAP.keys())}"
        )

    if not target_column:
        raise ValueError("target_column parameter is required")

    # Parse hyperparameters
    hyperparams = json.loads(hyperparams_str)

    # Split features and target
    y = df[target_column]
    X = df.drop(columns=[target_column])

    # Import and instantiate estimator
    module_path, class_name = _ESTIMATOR_MAP[estimator_name]
    module = importlib.import_module(module_path)
    estimator_cls = getattr(module, class_name)
    model = estimator_cls(**hyperparams)

    # Fit
    model.fit(X, y)

    # Build metrics
    train_score = float(model.score(X, y))
    metrics: dict = {"train_score": train_score}

    # Add accuracy for classifiers
    if hasattr(model, "predict"):
        preds = model.predict(X)
        if hasattr(model, "classes_"):
            accuracy = float(np.mean(preds == y))
            metrics["accuracy"] = accuracy

    # Add feature importances if available
    if hasattr(model, "feature_importances_"):
        metrics["feature_importances"] = model.feature_importances_.tolist()
    elif hasattr(model, "coef_"):
        coef = model.coef_
        if coef.ndim > 1:
            coef = coef[0]
        metrics["feature_importances"] = coef.tolist()

    # Save model
    model_path = _get_output_path("model", ".joblib")
    joblib.dump(model, model_path)

    # Save metrics
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))

    return {"model": str(model_path), "metrics": str(metrics_path)}


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "objective": Select(
            [
                "reg:squarederror",
                "binary:logistic",
                "multi:softmax",
                "multi:softprob",
            ],
            default="binary:logistic",
            description="XGBoost learning objective",
        ),
        "target_column": Text(default="target", description="Column name to predict", placeholder="target"),
        "n_estimators": Slider(min=10, max=1000, step=10, default=100, description="Number of boosting rounds"),
        "max_depth": Slider(min=1, max=20, step=1, default=6, description="Maximum tree depth per round"),
        "learning_rate": Slider(min=0.001, max=1, step=0.01, default=0.1, description="Step size shrinkage to prevent overfitting"),
    },
    label="Train XGBoost",
    category="Train",
    description="Train an XGBoost classifier or regressor and output the fitted model with evaluation metrics.",
)
def xgb_train(inputs: dict, params: dict) -> dict:
    """Train an XGBoost classifier or regressor."""
    import json

    import pandas as pd
    from sklearn.model_selection import train_test_split

    _REGRESSION_OBJECTIVES = {"reg:squarederror"}

    df = pd.read_parquet(inputs["train"])

    target_col = params.get("target_column", "target")
    objective = params.get("objective", "binary:logistic")
    n_estimators = int(params.get("n_estimators", 100))
    max_depth = int(params.get("max_depth", 6))
    learning_rate = float(params.get("learning_rate", 0.1))

    y = df[target_col]
    X = df.drop(columns=[target_col])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    if objective in _REGRESSION_OBJECTIVES:
        from xgboost import XGBRegressor

        model = XGBRegressor(
            objective=objective,
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            random_state=42,
        )
        model.fit(X_train, y_train)

        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        preds = model.predict(X_test)
        metrics = {
            "objective": objective,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "r2": float(r2_score(y_test, preds)),
            "mse": float(mean_squared_error(y_test, preds)),
            "mae": float(mean_absolute_error(y_test, preds)),
        }
    else:
        from xgboost import XGBClassifier

        model = XGBClassifier(
            objective=objective,
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            random_state=42,
            eval_metric="logloss",
        )
        model.fit(X_train, y_train)

        from sklearn.metrics import accuracy_score, f1_score

        preds = model.predict(X_test)
        metrics = {
            "objective": objective,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "accuracy": float(accuracy_score(y_test, preds)),
            "f1": float(f1_score(y_test, preds, average="weighted")),
        }

    # Save model
    import joblib

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(model, model_path)

    # Save metrics as JSON
    metrics_path = _get_output_path("xgb_metrics", ext=".json")
    metrics_path.write_text(json.dumps(metrics))

    return {"model": str(model_path), "metrics": str(metrics_path)}
