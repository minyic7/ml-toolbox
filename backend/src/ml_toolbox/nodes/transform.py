from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Toggle, node


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
