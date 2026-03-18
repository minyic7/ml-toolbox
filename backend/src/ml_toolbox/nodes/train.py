from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Text, node


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
        ),
        "target_column": Text(default=""),
        "hyperparams": Text(default="{}"),
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
