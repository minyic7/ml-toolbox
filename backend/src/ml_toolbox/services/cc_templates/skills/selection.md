---
name: selection
description: Show which nodes the user currently has selected on the canvas
---

# Selection

Check which nodes the user has selected on the canvas.

## Steps
1. Read the selection file: `cat {{project_dir}}/.selection.json`
2. If empty or no file, tell the user nothing is selected
3. For each selected node ID, look up its type, label, and seq from the pipeline:
   `curl -s {{api_base}}/api/pipelines/{{pipeline_id}}` and match node IDs
4. Show a summary using #seq: e.g. "Selected: #3 (Outlier Detection), #5 (Correlation Matrix)"
