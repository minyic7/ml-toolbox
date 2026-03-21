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


def _train_and_save(estimator, X, y, _get_output_path):  # type: ignore[no-untyped-def]
    """Shared helper: fit estimator, compute metrics, persist model + metrics."""
    import json

    import joblib
    import numpy as np

    estimator.fit(X, y)
    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    if hasattr(estimator, "feature_importances_"):
        metrics["feature_importances"] = estimator.feature_importances_.tolist()
    elif hasattr(estimator, "coef_"):
        coef = estimator.coef_
        metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()
    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


def _validate_and_split(inputs: dict, params: dict):  # type: ignore[no-untyped-def]
    """Shared helper: read parquet, validate target_column, split X/y."""
    import pandas as pd

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    return X, y


# ---------------------------------------------------------------------------
# Individual classifier nodes
# ---------------------------------------------------------------------------


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "n_estimators": Slider(min=10, max=500, step=10, default=100, description="Number of trees"),
        "max_depth": Slider(min=1, max=50, step=1, default=10, description="Maximum tree depth"),
        "min_samples_split": Slider(min=2, max=20, step=1, default=2, description="Minimum samples to split a node"),
    },
    label="Random Forest Classifier",
    category="Train",
)
def random_forest_classifier(inputs: dict, params: dict) -> dict:
    """Train a Random Forest classifier."""
    from sklearn.ensemble import RandomForestClassifier as RFC

    X, y = _validate_and_split(inputs, params)
    estimator = RFC(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 10)),
        min_samples_split=int(params.get("min_samples_split", 2)),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "n_estimators": Slider(min=10, max=500, step=10, default=100, description="Number of boosting stages"),
        "max_depth": Slider(min=1, max=20, step=1, default=3, description="Maximum tree depth"),
        "learning_rate": Slider(min=0.001, max=1, step=0.01, default=0.1, description="Step size shrinkage"),
    },
    label="Gradient Boosting Classifier",
    category="Train",
)
def gradient_boosting_classifier(inputs: dict, params: dict) -> dict:
    """Train a Gradient Boosting classifier."""
    from sklearn.ensemble import GradientBoostingClassifier as GBC

    X, y = _validate_and_split(inputs, params)
    estimator = GBC(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 3)),
        learning_rate=float(params.get("learning_rate", 0.1)),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "C": Slider(min=0.001, max=100, step=0.1, default=1.0, description="Inverse regularization strength"),
        "max_iter": Slider(min=100, max=5000, step=100, default=1000, description="Maximum iterations"),
    },
    label="Logistic Regression",
    category="Train",
)
def logistic_regression(inputs: dict, params: dict) -> dict:
    """Train a Logistic Regression classifier."""
    from sklearn.linear_model import LogisticRegression as LR

    X, y = _validate_and_split(inputs, params)
    estimator = LR(
        C=float(params.get("C", 1.0)),
        max_iter=int(params.get("max_iter", 1000)),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "kernel": Select(["linear", "rbf", "poly", "sigmoid"], default="rbf", description="Kernel function"),
        "C": Slider(min=0.001, max=100, step=0.1, default=1.0, description="Regularization parameter"),
    },
    label="SVC",
    category="Train",
)
def svc_classifier(inputs: dict, params: dict) -> dict:
    """Train a Support Vector Classifier."""
    from sklearn.svm import SVC

    X, y = _validate_and_split(inputs, params)
    estimator = SVC(
        kernel=str(params.get("kernel", "rbf")),
        C=float(params.get("C", 1.0)),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "max_depth": Slider(min=1, max=50, step=1, default=10, description="Maximum tree depth"),
        "min_samples_split": Slider(min=2, max=20, step=1, default=2, description="Minimum samples to split a node"),
        "criterion": Select(["gini", "entropy"], default="gini", description="Split quality function"),
    },
    label="Decision Tree Classifier",
    category="Train",
)
def decision_tree_classifier(inputs: dict, params: dict) -> dict:
    """Train a Decision Tree classifier."""
    from sklearn.tree import DecisionTreeClassifier as DTC

    X, y = _validate_and_split(inputs, params)
    estimator = DTC(
        max_depth=int(params.get("max_depth", 10)),
        min_samples_split=int(params.get("min_samples_split", 2)),
        criterion=str(params.get("criterion", "gini")),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "n_neighbors": Slider(min=1, max=50, step=1, default=5, description="Number of neighbors"),
        "weights": Select(["uniform", "distance"], default="uniform", description="Weight function for prediction"),
    },
    label="KNN Classifier",
    category="Train",
)
def knn_classifier(inputs: dict, params: dict) -> dict:
    """Train a K-Nearest Neighbors classifier."""
    from sklearn.neighbors import KNeighborsClassifier as KNC

    X, y = _validate_and_split(inputs, params)
    estimator = KNC(
        n_neighbors=int(params.get("n_neighbors", 5)),
        weights=str(params.get("weights", "uniform")),
    )
    return _train_and_save(estimator, X, y, _get_output_path)


# ---------------------------------------------------------------------------
# Individual regressor nodes
# ---------------------------------------------------------------------------


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
    },
    label="Linear Regression",
    category="Train",
)
def linear_regression(inputs: dict, params: dict) -> dict:
    """Train a LinearRegression model (no tunable hyperparameters)."""
    from sklearn.linear_model import LinearRegression

    X, y = _validate_and_split(inputs, params)
    estimator = LinearRegression()
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "n_estimators": Slider(min=10, max=500, step=10, default=100, description="Number of trees in the forest"),
        "max_depth": Slider(min=1, max=50, step=1, default=10, description="Maximum depth of each tree"),
        "min_samples_split": Slider(min=2, max=20, step=1, default=2, description="Minimum samples required to split an internal node"),
    },
    label="Random Forest Regressor",
    category="Train",
)
def random_forest_regressor(inputs: dict, params: dict) -> dict:
    """Train a RandomForestRegressor."""
    from sklearn.ensemble import RandomForestRegressor as RFR

    X, y = _validate_and_split(inputs, params)
    estimator = RFR(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 10)),
        min_samples_split=int(params.get("min_samples_split", 2)),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "n_estimators": Slider(min=10, max=500, step=10, default=100, description="Number of boosting stages"),
        "max_depth": Slider(min=1, max=20, step=1, default=3, description="Maximum depth of each tree"),
        "learning_rate": Slider(min=0.001, max=1, step=0.01, default=0.1, description="Step size shrinkage to prevent overfitting"),
    },
    label="Gradient Boosting Regressor",
    category="Train",
)
def gradient_boosting_regressor(inputs: dict, params: dict) -> dict:
    """Train a GradientBoostingRegressor."""
    from sklearn.ensemble import GradientBoostingRegressor as GBR

    X, y = _validate_and_split(inputs, params)
    estimator = GBR(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 3)),
        learning_rate=float(params.get("learning_rate", 0.1)),
        random_state=42,
    )
    return _train_and_save(estimator, X, y, _get_output_path)


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "kernel": Select(
            ["linear", "rbf", "poly", "sigmoid"],
            default="rbf",
            description="Kernel type for the SVR algorithm",
        ),
        "C": Slider(min=0.001, max=100, step=0.001, default=1.0, description="Regularization parameter"),
        "epsilon": Slider(min=0.01, max=1, step=0.01, default=0.1, description="Epsilon in the epsilon-SVR model"),
    },
    label="SVR",
    category="Train",
)
def svr_train(inputs: dict, params: dict) -> dict:
    """Train a Support Vector Regressor (SVR)."""
    from sklearn.svm import SVR

    X, y = _validate_and_split(inputs, params)
    estimator = SVR(
        kernel=str(params.get("kernel", "rbf")),
        C=float(params.get("C", 1.0)),
        epsilon=float(params.get("epsilon", 0.1)),
    )
    return _train_and_save(estimator, X, y, _get_output_path)


# ---------------------------------------------------------------------------
# Original sklearn_train (kept for backward compat — separate cleanup ticket)
# ---------------------------------------------------------------------------


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
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")

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
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
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

    target_col = params.get("target_column", "")
    if not target_col:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
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
