---
name: configure-node
description: Auto-configure a node's parameters based on upstream .meta.json
args: "{node_id}"
---

# Configure Node

Auto-configure a pipeline node's parameters based on the upstream data's `.meta.json`.

## Steps

1. **Get the pipeline definition.** Fetch `GET {{api_base}}/api/pipelines/{{pipeline_id}}` to get the full pipeline with nodes and edges.

2. **Identify the target node.** Find the node matching `{node_id}` in the pipeline's nodes array. Note its `type` (e.g., `ml_toolbox.nodes.eda.outlier_detection`).

3. **Find the upstream node.** Trace edges backward to find which node feeds into this one. Identify the upstream node's output parquet file.

4. **Read upstream `.meta.json`.** Look for `{upstream_node_id}.meta.json` in the latest run directory under `{{runs_dir}}`. If no `.meta.json` exists, suggest running `/infer-schema` first.

5. **Determine optimal params based on node type:**

   ### Outlier Detection (`ml_toolbox.nodes.eda.outlier_detection`)
   - No column selection param exists — it processes all numeric columns automatically
   - Set `method` based on data distribution: use `iqr` for skewed data, `zscore` for roughly normal data, `both` if mixed
   - Adjust `iqr_multiplier`: use `1.5` (default) for standard analysis, `3.0` if you want only extreme outliers
   - Adjust `zscore_threshold`: use `3.0` (default), raise to `4.0` if data is heavy-tailed

   ### Correlation Matrix (`ml_toolbox.nodes.eda.correlation_matrix`)
   - Set `target_column` to the column with `role: "target"` in `.meta.json`
   - Set `method`: use `spearman` if many features are skewed, `pearson` for roughly normal data, `both` for comprehensive analysis

   ### Distribution Profile (`ml_toolbox.nodes.eda.distribution_profile`)
   - Set `target_column` to the column with `role: "target"` in `.meta.json`

   ### Missing Analysis (`ml_toolbox.nodes.eda.missing_analysis`)
   - No configurable params — just confirm it's connected to the right input

   ### Random Hold-out (`ml_toolbox.nodes.preprocessing.random_holdout`)
   - Default splits are usually fine (train/val/test)
   - If the dataset is small (<1000 rows), suggest a larger train fraction

6. **Apply the configuration.** Call:
   ```
   PATCH {{api_base}}/api/pipelines/{{pipeline_id}}/nodes/{node_id}
   Content-Type: application/json

   {
     "params": {
       "target_column": "the_target",
       "method": "spearman"
     }
   }
   ```

7. **Report what was configured.** Show the user:
   - Which params were set and why
   - What data characteristics drove each decision
   - Any warnings (e.g., "no target column found in metadata")
