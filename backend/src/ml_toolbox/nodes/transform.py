from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Slider, Text, Toggle, node


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
    inputs={"df": PortType.TABLE},
    outputs={"df": PortType.TABLE},
    params={
        "drop_nulls": Toggle(default=True),
        "drop_duplicates": Toggle(default=True),
        "fill_strategy": Select(
            ["none", "mean", "median", "zero", "ffill"], default="none"
        ),
    },
    label="Clean Data",
    category="Transform",
)
def clean(inputs: dict, params: dict) -> dict:
    """Drop nulls, duplicates, or fill missing values in a DataFrame."""
    import pandas as pd

    df = pd.read_parquet(inputs["df"])

    drop_nulls = params.get("drop_nulls", True)
    drop_duplicates = params.get("drop_duplicates", True)
    fill_strategy = params.get("fill_strategy", "none")

    # Fill takes precedence over drop_nulls
    if fill_strategy != "none":
        if fill_strategy == "mean":
            df = df.fillna(df.select_dtypes("number").mean())
        elif fill_strategy == "median":
            df = df.fillna(df.select_dtypes("number").median())
        elif fill_strategy == "zero":
            df = df.fillna(0)
        elif fill_strategy == "ffill":
            df = df.ffill()
    elif drop_nulls:
        df = df.dropna()

    if drop_duplicates:
        df = df.drop_duplicates()

    out = _get_output_path("df")
    df.to_parquet(out, index=False)
    return {"df": str(out)}


@node(
    inputs={"df": PortType.TABLE},
    outputs={"df": PortType.TABLE},
    params={
        "scale_columns": Text(default=""),
        "encode_columns": Text(default=""),
        "bin_columns": Text(default=""),
    },
    label="Feature Engineering",
    category="Transform",
    description="Apply scaling, one-hot encoding, and quartile binning to selected columns.",
)
def feature_eng(inputs: dict, params: dict) -> dict:
    """Apply scaling, one-hot encoding, and quartile binning to selected columns."""
    import pandas as pd

    df = pd.read_parquet(inputs["df"])

    # Standard scaling
    scale_cols = [c.strip() for c in params.get("scale_columns", "").split(",") if c.strip()]
    if scale_cols:
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        df[scale_cols] = scaler.fit_transform(df[scale_cols])

    # One-hot encoding
    encode_cols = [c.strip() for c in params.get("encode_columns", "").split(",") if c.strip()]
    if encode_cols:
        df = pd.get_dummies(df, columns=encode_cols)

    # Quartile binning
    bin_cols = [c.strip() for c in params.get("bin_columns", "").split(",") if c.strip()]
    for col in bin_cols:
        df[f"{col}_bin"] = pd.qcut(df[col], q=4, labels=["Q1", "Q2", "Q3", "Q4"])

    out = _get_output_path("df")
    df.to_parquet(out, index=False)
    return {"df": str(out)}


@node(
    inputs={"df": PortType.TABLE},
    outputs={"train": PortType.TABLE, "test": PortType.TABLE},
    params={
        "test_size": Slider(min=0.05, max=0.5, step=0.05, default=0.2),
        "random_seed": Slider(min=0, max=100, step=1, default=42),
        "stratify_column": Text(default=""),
    },
    label="Train/Test Split",
    category="Transform",
    description="Split a DataFrame into train and test sets using sklearn.",
)
def split(inputs: dict, params: dict) -> dict:
    """Split a DataFrame into train and test sets using sklearn."""
    import polars as pl
    from sklearn.model_selection import train_test_split

    df = pl.read_parquet(inputs["df"])

    test_size = float(params.get("test_size", 0.2))
    random_seed = int(params.get("random_seed", 42))
    stratify_col = params.get("stratify_column", "")

    stratify = df[stratify_col].to_list() if stratify_col else None

    train_idx, test_idx = train_test_split(
        range(len(df)),
        test_size=test_size,
        random_state=random_seed,
        stratify=stratify,
    )

    train_df = df[train_idx]
    test_df = df[test_idx]

    train_path = _get_output_path("train")
    test_path = _get_output_path("test")
    train_df.write_parquet(train_path)
    test_df.write_parquet(test_path)

    return {"train": str(train_path), "test": str(test_path)}


@node(
    inputs={"df": PortType.TABLE},
    outputs={"stats": PortType.VALUE},
    params={
        "column": Text(default=""),
        "statistic": Select(
            ["mean", "median", "std", "min", "max", "count", "sum"], default="mean"
        ),
    },
    label="Compute Stats",
    category="Transform",
    description="Compute a single summary statistic from a table column. Outputs a JSON value.",
)
def compute_stats(inputs: dict, params: dict) -> dict:
    """Compute a single summary statistic from a table column."""
    import json

    import polars as pl

    df = pl.read_parquet(inputs["df"])

    column = params.get("column", "")
    statistic = params.get("statistic", "mean")

    # If column is empty or not found, use the first numeric column
    numeric_cols = [
        c for c in df.columns if df[c].dtype in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)
    ]
    if not column or column not in df.columns:
        column = numeric_cols[0] if numeric_cols else df.columns[0]

    series = df[column]

    stat_funcs = {
        "mean": lambda s: s.mean(),
        "median": lambda s: s.median(),
        "std": lambda s: s.std(),
        "min": lambda s: s.min(),
        "max": lambda s: s.max(),
        "count": lambda s: s.len(),
        "sum": lambda s: s.sum(),
    }

    value = stat_funcs[statistic](series)

    output_path = _get_output_path("stats", ".json")
    result = {"value": value, "column": column, "statistic": statistic}
    output_path.write_text(json.dumps(result))

    return {"stats": str(output_path)}
