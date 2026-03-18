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
run_dir = manifest_path.parent


def _get_output_path(name: str = "output", ext: str = ".parquet") -> str:
    return str(run_dir / f"{node_id}_{name}{ext}")


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

    namespace = {"_get_output_path": _get_output_path}
    exec(code, namespace)  # noqa: S102
    result = namespace["run"](inputs, params)

    out_path = manifest_path.parent / (manifest_path.stem + "_result.json")
    out_path.write_text(json.dumps(result))
except Exception:
    err_path = manifest_path.parent / (manifest_path.stem + "_error.json")
    err_path.write_text(json.dumps({"error": traceback.format_exc()}))
    sys.exit(1)
