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
    allowed_upstream={},
)
def csv_reader(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Load a CSV file into a TABLE output."""
    import pandas as pd

    path = params.get("path", "")
    if not path:
        raise ValueError("path parameter is required — upload a file or enter a file path")
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
    allowed_upstream={},
)
def parquet_reader(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Load a Parquet file into a TABLE output."""
    import polars as pl

    path = params["path"]
    if not path:
        raise ValueError("path parameter is required — upload a file or enter a file path")
    columns_param = params.get("columns", "")
    columns = [c.strip() for c in columns_param.split(",") if c.strip()] or None

    df = pl.read_parquet(path, columns=columns)

    out = _get_output_path("df")
    df.write_parquet(out)
    return {"df": str(out)}


@node(
    outputs={"df": PortType.TABLE},
    params={
        "path": Text(default="", description="Absolute path to the Excel file on disk", placeholder="/path/to/data.xlsx"),
        "sheet_name": Text(default="", description="Sheet name to read (empty = first sheet)", placeholder="Sheet1"),
        "header_row": Text(default="0", description="Row number containing column headers (0-based)", placeholder="0"),
        "skip_rows": Text(default="0", description="Number of rows to skip from the top before reading", placeholder="0"),
    },
    label="Excel Reader",
    category="Ingest",
    description="Load an Excel file (.xlsx) into a TABLE output.",
    allowed_upstream={},
)
def excel_reader(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Load an Excel file (.xlsx) into a TABLE output."""
    import pandas as pd

    path = params.get("path", "")
    if not path:
        raise ValueError("path parameter is required — upload a file or enter a file path")

    sheet_name: str | int = params.get("sheet_name", "") or 0
    header_row = int(params.get("header_row", "0") or "0")
    skip_rows = int(params.get("skip_rows", "0") or "0")

    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=header_row,
        skiprows=skip_rows,
        engine="openpyxl",
    )

    out = _get_output_path("df")
    df.to_parquet(out, index=False)
    return {"df": str(out)}
