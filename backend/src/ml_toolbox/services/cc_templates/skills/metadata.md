---
name: metadata
description: Show column metadata from .meta.json files in the latest run
---

# Metadata

Read and display .meta.json files from the latest run.

## Steps
1. Find the latest run directory: `ls -td {{runs_dir}}/*/ | head -1`
2. Find all .meta.json files: `ls {{runs_dir}}/latest_run/*.meta.json`
3. For each .meta.json, read and display a summary table:
   | Column | Dtype | Semantic Type | Role | Confidence |
4. Highlight any columns with low confidence or ambiguous classification
5. Show the target column prominently if one is identified
