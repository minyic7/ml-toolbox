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


@node(
    inputs={"df": PortType.TABLE},
    outputs={"file": PortType.TABLE},
    params={
        "format": Select(["csv", "parquet"], default="csv"),
        "filename": Text(default="output"),
    },
    label="Export Table",
    category="Export",
    description="Export a TABLE to CSV or Parquet for download.",
)
def export_table(inputs: dict, params: dict) -> dict:
    """Export a TABLE to CSV or Parquet for download."""
    import polars as pl

    df = pl.read_parquet(inputs["df"])
    fmt = params.get("format", "csv")
    filename = params.get("filename", "output")

    ext = ".csv" if fmt == "csv" else ".parquet"
    out = _get_output_path(filename, ext=ext)

    if fmt == "csv":
        df.write_csv(out)
    else:
        df.write_parquet(out)

    return {"file": str(out)}
