import json
from pathlib import Path

from ml_toolbox.protocol import PortType, Text, node


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
    inputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    outputs={
        "train": PortType.TABLE,
        "val": PortType.TABLE,
        "test": PortType.TABLE,
    },
    params={
        "columns_to_drop": Text(
            default="",
            description="Comma-separated list of columns to remove from all splits",
            placeholder="id, name, timestamp",
        ),
    },
    label="Column Dropper",
    description="Drop selected columns from train/val/test splits. Target column is protected.",
    allowed_upstream={
        "train": ["random_holdout", "stratified_holdout"],
        "val": ["random_holdout", "stratified_holdout"],
        "test": ["random_holdout", "stratified_holdout"],
    },
    guide="""## Column Dropper

Remove unwanted columns from your dataset across all splits (train, validation, test).

### What it does
- Drops the specified columns from every connected split
- Updates `.meta.json` so downstream nodes see the correct schema
- **Protects the target column** — if you accidentally select the target, it is kept and a warning is printed

### When to use
- **Remove ID / index columns** that would leak row identity to the model
- **Drop high-cardinality categoricals** (e.g. names, free-text) that add noise
- **Remove redundant features** you identified during EDA (e.g. highly correlated pairs)
- **Exclude date/time columns** that need dedicated feature engineering first

### Inputs / Outputs
| Port | Required | Description |
|------|----------|-------------|
| train | Yes | Training split — always required |
| val | No | Validation split — processed identically if connected |
| test | No | Test split — processed identically if connected |

### Parameters
| Parameter | Description |
|-----------|-------------|
| `columns_to_drop` | Comma-separated column names (e.g. `id, name, timestamp`) |

### Target protection
The target column (read from `.meta.json`) is **never dropped**, even if listed in
`columns_to_drop`. A warning is printed instead. This prevents accidentally removing
the variable you are trying to predict.
""",
)
def column_dropper(inputs: dict, params: dict) -> dict:
    """Drop selected columns from train/val/test splits."""
    import json
    import warnings
    from pathlib import Path

    import polars as pl

    # ── Parse columns_to_drop ────────────────────────────────────
    raw = params.get("columns_to_drop", "")
    columns_to_drop = [c.strip() for c in raw.split(",") if c.strip()]

    if not columns_to_drop:
        raise ValueError("columns_to_drop is empty — select at least one column to drop.")

    # ── Read train (mandatory) ───────────────────────────────────
    train_path = Path(inputs["train"])
    train_df = pl.read_parquet(train_path)

    # ── Read .meta.json for target column ────────────────────────
    meta_path = train_path.with_suffix(".meta.json")
    meta = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
    target_col = meta.get("target", "")

    # ── Validate columns exist in schema ─────────────────────────
    schema_cols = set(train_df.columns)
    missing = [c for c in columns_to_drop if c not in schema_cols]
    if missing:
        raise ValueError(
            f"Columns not found in schema: {missing}. "
            f"Available columns: {sorted(schema_cols)}"
        )

    # ── Protect target column ────────────────────────────────────
    actual_drop = []
    for col in columns_to_drop:
        if col == target_col:
            warnings.warn(
                f"Target column '{target_col}' cannot be dropped — skipping it.",
                stacklevel=1,
            )
        else:
            actual_drop.append(col)

    if not actual_drop:
        raise ValueError("No columns to drop after excluding the protected target column.")

    # ── Helper: drop columns + write output + update meta ────────
    def _process_split(df: pl.DataFrame, split_name: str) -> str:
        out_df = df.drop(actual_drop)
        out_path = _get_output_path(split_name)
        out_df.write_parquet(out_path)

        # Write updated .meta.json (remove dropped columns)
        if meta:
            updated_meta = dict(meta)
            if "columns" in updated_meta:
                updated_meta["columns"] = {
                    k: v for k, v in updated_meta["columns"].items()
                    if k not in actual_drop
                }
            meta_out = Path(str(out_path)).with_suffix(".meta.json")
            meta_out.write_text(json.dumps(updated_meta, indent=2))

        return str(out_path)

    result: dict[str, str] = {}

    # ── Process train (mandatory) ────────────────────────────────
    result["train"] = _process_split(train_df, "train")

    # ── Process val (optional) ───────────────────────────────────
    if "val" in inputs:
        val_df = pl.read_parquet(inputs["val"])
        result["val"] = _process_split(val_df, "val")

    # ── Process test (optional) ──────────────────────────────────
    if "test" in inputs:
        test_df = pl.read_parquet(inputs["test"])
        result["test"] = _process_split(test_df, "test")

    return result
