---
name: outputs
description: List all node outputs from the latest run with file sizes and types
---

# Outputs

List all output files from the latest pipeline run.

## Steps
1. Find the latest run directory: `ls -td {{runs_dir}}/*/ | head -1`
2. List all output files (exclude .hash and internal files)
3. For each file show: #seq (from pipeline node), node label, filename, type (parquet/json/joblib), size
4. For .json files (EDA reports), show the report_type
5. For .parquet files, show row/column count: `python3 -c "import pandas as pd; df=pd.read_parquet('file'); print(f'{len(df)} rows, {len(df.columns)} cols')"`
