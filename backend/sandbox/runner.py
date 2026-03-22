"""Sandbox runner — executes node code inside an isolated container.

Edge conditions are evaluated here (inside the sandbox), never on the
backend host, to prevent arbitrary code execution on the server.
"""

import json
import sys
import traceback
from pathlib import Path

manifest_path = Path(sys.argv[1])
manifest = json.loads(manifest_path.read_text())

code = manifest["code"]
inputs = manifest["inputs"]  # already /data/... paths inside the container
params = manifest["params"]
node_id = manifest["node_id"]
conditions = manifest.get("conditions", [])
output_types = manifest.get("output_types", {})
run_dir = manifest_path.parent


def _get_output_path(name: str = "output", ext: str = ".parquet") -> Path:
    return run_dir / f"{node_id}_{name}{ext}"


# ── Evaluate edge conditions (if any) ─────────────────────────
# Each condition references upstream results via a "result" variable.
# If ANY condition evaluates to False, the node is skipped.

def _check_conditions() -> bool:
    """Return True if all incoming edge conditions pass."""
    for cond_entry in conditions:
        source_id = cond_entry["source_id"]
        condition = cond_entry["condition"]

        # Load upstream result
        result_path = run_dir / f"{source_id}_manifest_result.json"
        result = {}
        if result_path.exists():
            try:
                result = json.loads(result_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass

        safe_builtins = {
            "len": len, "int": int, "float": float,
            "str": str, "bool": bool, "abs": abs,
            "min": min, "max": max, "sum": sum,
            "round": round, "isinstance": isinstance,
            "list": list, "dict": dict, "tuple": tuple,
            "True": True, "False": False, "None": None,
        }
        namespace = {"__builtins__": safe_builtins, "result": result}
        try:
            if not eval(condition, namespace):  # noqa: S307
                return False
        except Exception:
            # Condition evaluation failed — treat as not met
            return False
    return True


try:
    # Check conditions first
    if conditions and not _check_conditions():
        out_path = manifest_path.parent / (manifest_path.stem + "_result.json")
        out_path.write_text(json.dumps({"skipped": True}))
        sys.exit(0)

    entry_fn = manifest.get("entry_fn", "run")
    namespace = {"_get_output_path": _get_output_path}
    exec(code, namespace)  # noqa: S102
    result = namespace[entry_fn](inputs, params)

    # ── Auto-serialization of output values ──────────────────────
    # If node code returns a raw Python object instead of a file path,
    # serialize it based on the declared output port type.
    if isinstance(result, dict):
        for key, value in result.items():
            port_type = output_types.get(key, "")
            if isinstance(value, str):
                # Already a file path — leave as-is
                continue
            if port_type == "MODEL":
                import joblib

                out = _get_output_path(key, ".joblib")
                joblib.dump(value, out)
                result[key] = str(out)
            elif port_type == "TABLE":
                try:
                    import polars as pl

                    if isinstance(value, pl.DataFrame):
                        out = _get_output_path(key, ".parquet")
                        value.write_parquet(out)
                        result[key] = str(out)
                        continue
                except ImportError:
                    pass
                try:
                    import pandas as pd

                    if isinstance(value, pd.DataFrame):
                        out = _get_output_path(key, ".parquet")
                        value.to_parquet(out)
                        result[key] = str(out)
                except ImportError:
                    pass

    # ── Propagate sidecar files from inputs to outputs ──────────
    # If an input file has .meta.json or .eda-context.json sidecars,
    # copy them alongside each TABLE output so downstream nodes inherit
    # column metadata and EDA context.
    import shutil

    for _input_name, input_path_str in inputs.items():
        input_path = Path(str(input_path_str))
        for sidecar_suffix in [".meta.json", ".eda-context.json"]:
            sidecar_path = input_path.with_suffix(sidecar_suffix)
            if not sidecar_path.exists():
                # Also check for {node_id}_portname{suffix} pattern
                candidates = list(input_path.parent.glob(
                    f"{input_path.stem}{sidecar_suffix}"
                ))
                if candidates:
                    sidecar_path = candidates[0]
                else:
                    continue
            # Copy to each TABLE output
            if isinstance(result, dict):
                for out_key, out_value in result.items():
                    out_type = output_types.get(out_key, "")
                    if out_type == "TABLE" and isinstance(out_value, str):
                        out_p = Path(out_value)
                        dest = out_p.with_suffix(sidecar_suffix)
                        if not dest.exists():
                            shutil.copy2(str(sidecar_path), str(dest))

    out_path = manifest_path.parent / (manifest_path.stem + "_result.json")
    out_path.write_text(json.dumps(result))
except Exception:
    err_path = manifest_path.parent / (manifest_path.stem + "_error.json")
    err_path.write_text(json.dumps({"error": traceback.format_exc()}))
    sys.exit(1)
