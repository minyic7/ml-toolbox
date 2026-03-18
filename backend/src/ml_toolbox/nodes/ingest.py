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
        "path": Text(default=""),
        "separator": Text(default=","),
        "header": Toggle(default=True),
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
