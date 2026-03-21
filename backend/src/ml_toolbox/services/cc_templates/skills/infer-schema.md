---
name: infer-schema
description: Analyze a node's parquet output and generate .meta.json with column semantic types and roles
---

# Infer Schema

Analyze a parquet file from the pipeline and generate a `.meta.json` file describing each column's semantic type, role, and characteristics.

## Steps

1. **Find the latest parquet output.** Look in `{{runs_dir}}` for the most recent run directory. List the `.parquet` files available. If the user specifies a node ID, use that; otherwise pick the first ingest node's output.

2. **Read and sample the data.** Use pandas or polars to read the parquet file. If it has more than 10,000 rows, sample 10,000 rows for analysis (but use the full dataset for counts).

3. **Analyze each column.** For every column, compute:
   - `dtype`: the pandas/polars dtype as a string
   - `unique_count`: number of distinct values
   - `unique_ratio`: unique_count / total_rows
   - `null_pct`: fraction of null/NaN values (0.0 to 1.0)
   - `value_counts`: top 10 most frequent values with counts (for categoricals)
   - `sample_values`: 5 representative non-null values

4. **Classify each column using heuristics + your understanding:**

   | Condition | semantic_type |
   |-----------|--------------|
   | dtype is datetime or name contains date/time/timestamp | `datetime` |
   | dtype is object/string and unique_ratio > 0.9 | `identifier` or `text` |
   | dtype is object/string and unique_ratio <= 0.9 | `categorical` |
   | dtype is bool or unique_count == 2 | `binary` |
   | dtype is numeric and unique_count <= 15 | `categorical` (encoded) |
   | dtype is numeric and unique_count > 15 | `continuous` |
   | Column name suggests target (target, label, class, y) | `target` |

   Assign `role`:
   - `target` if semantic_type is target or column name strongly suggests it
   - `identifier` if semantic_type is identifier (e.g., ID columns)
   - `metadata` if column is datetime or non-predictive
   - `feature` for everything else

5. **Write `.meta.json`.** Save alongside the parquet file:
   ```
   {node_id}.meta.json
   ```

   Format:
   ```json
   {
     "columns": {
       "col_name": {
         "dtype": "float64",
         "semantic_type": "continuous",
         "role": "feature",
         "nullable": true,
         "unique_count": 342,
         "unique_ratio": 0.342,
         "null_pct": 0.02,
         "sample_values": [1.5, 2.3, 0.8, 4.1, 3.7],
         "reasoning": "Numeric column with high cardinality (342 unique / 1000 rows) — continuous feature"
       }
     },
     "row_count": 1000,
     "generated_by": "infer-schema",
     "node_id": "the_node_id"
   }
   ```

6. **Show a summary table.** After writing, display results:

   ```
   | Column | dtype | Semantic Type | Role | Nulls | Unique | Reasoning |
   |--------|-------|--------------|------|-------|--------|-----------|
   | age    | int64 | continuous   | feature | 0% | 72   | High cardinality numeric |
   | sex    | object | categorical | feature | 0% | 2    | Low cardinality string |
   | target | int64 | target       | target  | 0% | 2    | Binary, name suggests target |
   ```

## Notes
- Always provide detailed `reasoning` for each classification — this helps the user verify and correct mistakes.
- If uncertain about a column's role, say so in the reasoning and default to `feature`.
- The `.meta.json` is used by `/configure-node` to auto-configure downstream nodes.
