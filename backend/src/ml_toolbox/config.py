import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("ML_TOOLBOX_DATA_DIR", Path.home() / ".ml-toolbox"))

HOST = os.environ.get("ML_TOOLBOX_HOST", "0.0.0.0")
PORT = int(os.environ.get("ML_TOOLBOX_PORT", "8000"))
