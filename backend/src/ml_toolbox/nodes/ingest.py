from pathlib import Path

from ml_toolbox.protocol import PortType, Text, Toggle, node


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
    outputs={"df": PortType.TABLE},
    params={
        "path": Text(default="", description="Absolute path to the CSV file on disk", placeholder="/path/to/data.csv"),
        "separator": Text(default=",", description="Column delimiter character", placeholder=","),
        "header": Toggle(default=True, description="First row contains column names"),
    },
    label="CSV Reader",
    category="Ingest",
    description="Load a CSV file into a TABLE output.",
)
def csv_reader(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Load a CSV file into a TABLE output."""
    import pandas as pd

    path = params.get("path", "")
    separator = params.get("separator", ",")
    header = params.get("header", True)

    df = pd.read_csv(
        path,
        sep=separator,
        header=0 if header else None,
    )

    out = _get_output_path("df")
    df.to_parquet(out, index=False)
    return {"df": str(out)}


@node(
    outputs={"df": PortType.TABLE},
    params={
        "path": Text(default="", description="Absolute path to the Parquet file on disk", placeholder="/path/to/data.parquet"),
        "columns": Text(default="", description="Comma-separated list of columns to load (empty = all)", placeholder="col1, col2, col3"),
    },
    label="Parquet Reader",
    category="Ingest",
    description="Load a Parquet file into a TABLE output.",
)
def parquet_reader(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Load a Parquet file into a TABLE output."""
    import polars as pl

    path = params["path"]
    columns_param = params.get("columns", "")
    columns = [c.strip() for c in columns_param.split(",") if c.strip()] or None

    df = pl.read_parquet(path, columns=columns)

    out = _get_output_path("df")
    df.write_parquet(out)
    return {"df": str(out)}
