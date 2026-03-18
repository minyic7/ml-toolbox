"""Sandbox runner — executes node code inside an isolated container."""

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
run_dir = manifest_path.parent


def _get_output_path(name: str = "output", ext: str = ".parquet") -> str:
    return str(run_dir / f"{node_id}_{name}{ext}")


try:
    namespace = {"_get_output_path": _get_output_path}
    exec(code, namespace)
    result = namespace["run"](inputs, params)

    out_path = manifest_path.parent / (manifest_path.stem + "_result.json")
    out_path.write_text(json.dumps(result))
except Exception:
    err_path = manifest_path.parent / (manifest_path.stem + "_error.json")
    err_path.write_text(json.dumps({"error": traceback.format_exc()}))
    sys.exit(1)
