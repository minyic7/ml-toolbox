---
name: runs
description: Show pipeline run history with status and duration
---

# Runs

Show the history of pipeline runs.

## Steps
1. Fetch runs: `curl -s {{api_base}}/api/runs?pipeline_id={{pipeline_id}}`
2. Display a table: run_id (short) | status | started_at | duration | nodes_run
3. Highlight the latest run
