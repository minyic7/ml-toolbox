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
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
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
    X = X.select_dtypes(include="number")
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
    category="Classification",
)
def random_forest_classifier(inputs: dict, params: dict) -> dict:
    """Train a Random Forest classifier."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier as RFC

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = RFC(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 10)),
        min_samples_split=int(params.get("min_samples_split", 2)),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


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
    category="Classification",
)
def gradient_boosting_classifier(inputs: dict, params: dict) -> dict:
    """Train a Gradient Boosting classifier."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import GradientBoostingClassifier as GBC

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = GBC(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 3)),
        learning_rate=float(params.get("learning_rate", 0.1)),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "C": Slider(min=0.001, max=100, step=0.1, default=1.0, description="Inverse regularization strength"),
        "max_iter": Slider(min=100, max=5000, step=100, default=1000, description="Maximum iterations"),
    },
    label="Logistic Regression",
    category="Classification",
)
def logistic_regression(inputs: dict, params: dict) -> dict:
    """Train a Logistic Regression classifier."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LogisticRegression as LR

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = LR(
        C=float(params.get("C", 1.0)),
        max_iter=int(params.get("max_iter", 1000)),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "kernel": Select(["linear", "rbf", "poly", "sigmoid"], default="rbf", description="Kernel function"),
        "C": Slider(min=0.001, max=100, step=0.1, default=1.0, description="Regularization parameter"),
    },
    label="SVC",
    category="Classification",
)
def svc_classifier(inputs: dict, params: dict) -> dict:
    """Train a Support Vector Classifier."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.svm import SVC

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = SVC(
        kernel=str(params.get("kernel", "rbf")),
        C=float(params.get("C", 1.0)),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


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
    category="Classification",
)
def decision_tree_classifier(inputs: dict, params: dict) -> dict:
    """Train a Decision Tree classifier."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.tree import DecisionTreeClassifier as DTC

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = DTC(
        max_depth=int(params.get("max_depth", 10)),
        min_samples_split=int(params.get("min_samples_split", 2)),
        criterion=str(params.get("criterion", "gini")),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


@node(
    inputs={"train": PortType.TABLE},
    outputs={"model": PortType.MODEL, "metrics": PortType.METRICS},
    params={
        "target_column": Text(default="", description="Column name to predict", placeholder="target"),
        "n_neighbors": Slider(min=1, max=50, step=1, default=5, description="Number of neighbors"),
        "weights": Select(["uniform", "distance"], default="uniform", description="Weight function for prediction"),
    },
    label="KNN Classifier",
    category="Classification",
)
def knn_classifier(inputs: dict, params: dict) -> dict:
    """Train a K-Nearest Neighbors classifier."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.neighbors import KNeighborsClassifier as KNC

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = KNC(
        n_neighbors=int(params.get("n_neighbors", 5)),
        weights=str(params.get("weights", "uniform")),
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


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
    category="Regression",
)
def linear_regression(inputs: dict, params: dict) -> dict:
    """Train a LinearRegression model (no tunable hyperparameters)."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LinearRegression

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = LinearRegression()
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


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
    category="Regression",
)
def random_forest_regressor(inputs: dict, params: dict) -> dict:
    """Train a RandomForestRegressor."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import RandomForestRegressor as RFR

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = RFR(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 10)),
        min_samples_split=int(params.get("min_samples_split", 2)),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


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
    category="Regression",
)
def gradient_boosting_regressor(inputs: dict, params: dict) -> dict:
    """Train a GradientBoostingRegressor."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import GradientBoostingRegressor as GBR

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = GBR(
        n_estimators=int(params.get("n_estimators", 100)),
        max_depth=int(params.get("max_depth", 3)),
        learning_rate=float(params.get("learning_rate", 0.1)),
        random_state=42,
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
    metrics_path = _get_output_path("metrics", ".json")
    metrics_path.write_text(json.dumps(metrics))
    return {"model": str(model_path), "metrics": str(metrics_path)}


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
    category="Regression",
)
def svr_train(inputs: dict, params: dict) -> dict:
    """Train a Support Vector Regressor (SVR)."""
    import json

    import joblib
    import numpy as np
    import pandas as pd
    from sklearn.svm import SVR

    df = pd.read_parquet(inputs["train"])
    target_column = params.get("target_column", "")
    if not target_column:
        raise ValueError("target_column is required — set it in the Params tab (e.g. 'Survived', 'target')")
    y = df[target_column]
    X = df.drop(columns=[target_column])
    X = X.select_dtypes(include="number")

    estimator = SVR(
        kernel=str(params.get("kernel", "rbf")),
        C=float(params.get("C", 1.0)),
        epsilon=float(params.get("epsilon", 0.1)),
    )
    estimator.fit(X, y)

    train_score = float(estimator.score(X, y))
    metrics: dict[str, object] = {"train_score": train_score}
    if hasattr(estimator, "classes_"):
        metrics["accuracy"] = float(np.mean(estimator.predict(X) == y))
    fi = getattr(estimator, "feature_importances_", None)
    if fi is not None:
        metrics["feature_importances"] = fi.tolist()
    else:
        coef = getattr(estimator, "coef_", None)
        if coef is not None:
            metrics["feature_importances"] = (np.abs(coef[0]) if coef.ndim > 1 else np.abs(coef)).tolist()

    model_path = _get_output_path("model", ".joblib")
    joblib.dump(estimator, model_path)
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
