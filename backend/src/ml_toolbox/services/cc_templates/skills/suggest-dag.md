---
name: suggest-dag
description: Recommend a complete pipeline DAG based on dataset characteristics
---

# Suggest DAG

Analyze the ingested dataset and recommend a complete pipeline DAG — which nodes to add and in what order.

## Steps

1. **Find the ingest node's .meta.json.** Look in `{{runs_dir}}` for the latest run. Find the `.meta.json` for the ingest node (csv_reader or parquet_reader). If none exists, run the pipeline first to generate outputs.

2. **Also read the parquet file** to get basic stats: row count, column count, memory size.

3. **Analyze the dataset characteristics:**
   - **Size**: small (<1K rows), medium (1K-100K), large (>100K)
   - **Column types**: how many continuous, categorical, binary, datetime, identifiers
   - **Missing data**: any columns with significant nulls?
   - **Target**: is there a target column? Classification or regression?
   - **Cardinality**: any high-cardinality categoricals?

4. **Recommend a DAG based on the analysis.** Follow this decision framework:

   ### Always recommend (in order):
   1. **Ingest** (already exists) — the data source
   2. **Missing Analysis** — understand missing patterns before cleaning
   3. **Distribution Profile** — understand feature distributions and target balance

   ### Conditionally recommend:
   4. **Outlier Detection** — if there are continuous features (skip if all categorical)
   5. **Correlation Matrix** — if there are 3+ numeric features (fewer is not useful)

   ### Edge connections:
   - Ingest → Missing Analysis
   - Ingest → Distribution Profile
   - Ingest → Outlier Detection
   - Ingest → Correlation Matrix
   - (EDA nodes typically all branch from ingest in parallel)

   ### If splitting is needed:
   - **Random Hold-out** — always before any model training; connect after ingest
   - The train split output feeds into EDA nodes (analyze train only)

5. **Output a step-by-step plan:**

   ```
   ## Recommended Pipeline

   Based on your dataset (1000 rows, 12 columns, 8 numeric, 3 categorical, 1 target):

   ### Step 1: Missing Analysis
   - Why: 3 columns have >5% missing values — need to understand patterns
   - Connect: Ingest → Missing Analysis

   ### Step 2: Distribution Profile
   - Why: Need to check target balance and feature distributions
   - Set: target_column = "class"
   - Connect: Ingest → Distribution Profile

   ### Step 3: Outlier Detection
   - Why: 8 numeric features that may have outliers
   - Set: method = "iqr"
   - Connect: Ingest → Outlier Detection

   ### Step 4: Correlation Matrix
   - Why: 8 numeric features — check for multicollinearity
   - Set: target_column = "class", method = "spearman"
   - Connect: Ingest → Correlation Matrix

   ### Optional: Random Hold-out
   - Why: If you plan to train models, split first and run EDA on train only
   - Connect: Ingest → Random Hold-out → (EDA nodes on train split)
   ```

6. **Warn about pitfalls:**
   - If target is imbalanced, note it and suggest stratified splitting
   - If dataset is small, warn about overfitting risk
   - If many features relative to rows, warn about curse of dimensionality
   - If there are identifier columns, warn not to include them as features

## Notes
- The recommendations should be practical and actionable, not exhaustive.
- Explain *why* each node is recommended, not just *what* to add.
- Use `PATCH {{api_base}}/api/pipelines/{{pipeline_id}}/nodes/{node_id}` to set params for each recommended node.
