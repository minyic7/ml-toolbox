import json
from pathlib import Path

import polars as pl

from ml_toolbox.protocol import PortType, Select, Slider, node


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


# ── generate_data ────────────────────────────────────────────────────
@node(
    outputs={"df": PortType.TABLE},
    params={"rows": Slider(min=10, max=1000, step=10, default=100)},
    label="Generate Data",
    description="Generate a random DataFrame and write it to parquet.",
)
def run(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Generate a random DataFrame and write it to parquet."""
    import random
    import polars as pl

    n = int(params.get("rows", 100))
    df = pl.DataFrame(
        {
            "id": list(range(n)),
            "value_a": [random.gauss(0, 1) for _ in range(n)],
            "value_b": [random.gauss(5, 2) for _ in range(n)],
            "category": [random.choice(["A", "B", "C"]) for _ in range(n)],
        }
    )
    out = _get_output_path("df")
    df.write_parquet(out)
    return {"df": str(out)}


# ── clean_data ───────────────────────────────────────────────────────
@node(
    inputs={"df": PortType.TABLE},
    outputs={"df": PortType.TABLE},
    params={
        "strategy": Select(["mean", "median", "drop"], default="mean"),
    },
)
def clean_data(inputs: dict, params: dict) -> dict:
    """Read parquet, handle nulls according to strategy, write parquet."""
    import polars as pl

    df = pl.read_parquet(inputs["df"])
    strategy = params.get("strategy", "mean")

    if strategy == "drop":
        df = df.drop_nulls()
    else:
        numeric_cols = [c for c in df.columns if df[c].dtype.is_numeric()]
        for col in numeric_cols:
            fill_value = getattr(df[col], strategy)()
            df = df.with_columns(pl.col(col).fill_null(fill_value))

    out = _get_output_path("df")
    df.write_parquet(out)
    return {"df": str(out)}


# ── summarize_data ───────────────────────────────────────────────────
@node(
    inputs={"df": PortType.TABLE},
    outputs={"summary": PortType.METRICS},
)
def summarize_data(inputs: dict, params: dict) -> dict:  # noqa: ARG001
    """Read parquet and compute basic statistics."""
    import json
    import polars as pl

    df = pl.read_parquet(inputs["df"])

    summary = {
        "row_count": df.height,
        "column_count": df.width,
        "null_counts": {col: df[col].null_count() for col in df.columns},
    }

    out = _get_output_path("summary", ext=".json")
    out.write_text(json.dumps(summary, indent=2))
    return {"summary": str(out)}
