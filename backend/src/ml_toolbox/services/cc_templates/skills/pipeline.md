---
name: pipeline
description: Show current pipeline overview — nodes, edges, runs, metadata
---

# Pipeline Overview

Fetch and display a comprehensive overview of the current pipeline.

## Steps
1. Fetch pipeline: `curl -s {{api_base}}/api/pipelines/{{pipeline_id}} | python3 -m json.tool`
2. Count nodes and edges, list each node with its #seq number, type, and params
3. Check latest run: `curl -s {{api_base}}/api/pipelines/{{pipeline_id}}/status`
4. List available .meta.json files: `ls {{runs_dir}}/*/*.meta.json 2>/dev/null`
5. For each .meta.json found, show a brief summary (column count, target column)
6. Present a clean summary table
