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
        "format": Select(["csv", "parquet"], default="csv", description="Output file format"),
        "filename": Text(default="output", description="Name for the exported file (without extension)", placeholder="output"),
    },
    label="Export Table",
    category="Export",
    description="Export a TABLE to CSV or Parquet for download.",
)
def export_table(inputs: dict, params: dict) -> dict:
    """Export a TABLE to CSV or Parquet for download."""
    from pathlib import Path

    import polars as pl

    df = pl.read_parquet(inputs["df"])
    fmt = params.get("format", "csv")
    filename = params.get("filename", "output")

    # Sanitize filename: take only the stem and strip path separators
    filename = Path(filename).stem.replace("/", "").replace("..", "")
    if not filename:
        filename = "output"

    ext = ".csv" if fmt == "csv" else ".parquet"
    out = _get_output_path(filename, ext=ext)

    if fmt == "csv":
        df.write_csv(out)
    else:
        df.write_parquet(out)

    # Pass through the original TABLE for downstream chaining
    return {"file": inputs["df"]}


@node(
    inputs={"model": PortType.MODEL},
    outputs={"model": PortType.MODEL},
    params={
        "filename": Text(default="model", description="Name for the exported model file (without extension)", placeholder="model"),
    },
    label="Export Model",
    category="Export",
    description="Export a MODEL to a downloadable .joblib file.",
)
def export_model(inputs: dict, params: dict) -> dict:
    """Export a MODEL to a downloadable .joblib file."""
    from pathlib import Path

    import joblib

    filename = params.get("filename", "model")

    # Sanitize filename: take only the stem and strip path separators
    filename = Path(filename).stem.replace("/", "").replace("..", "")
    if not filename:
        filename = "model"

    model = joblib.load(inputs["model"])
    out = _get_output_path(filename, ext=".joblib")
    joblib.dump(model, out)

    # Pass through the original MODEL for downstream chaining
    return {"model": inputs["model"]}
