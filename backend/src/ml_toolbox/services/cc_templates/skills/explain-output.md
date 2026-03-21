---
name: explain-output
description: Interpret an EDA node's output report and give actionable recommendations
args: "{node_id}"
---

# Explain Output

Read and interpret an EDA node's output report, highlighting key findings and giving actionable recommendations.

## Steps

1. **Find the output file.** Look in `{{runs_dir}}` for the latest run directory. Find `{node_id}.json` — this is the report output from the EDA node.

2. **Identify the report type.** The JSON has a `report_type` field:
   - `correlation_matrix`
   - `distribution_profile`
   - `missing_analysis`
   - `outlier_detection`

3. **Interpret based on report type:**

   ### Correlation Matrix (`report_type: "correlation_matrix"`)
   - **High correlations (|r| > 0.8):** Flag multicollinearity risk. Name the pairs and their r values. Suggest dropping one of the pair for linear models.
   - **Target correlations:** Rank features by correlation strength with the target. Highlight top predictive features (|r| > 0.3) and weak features (|r| < 0.05).
   - **Pearson vs Spearman discrepancy:** If both computed and differ significantly for a pair, there's likely a non-linear relationship.
   - **Action items:** "Consider dropping X (r=0.95 with Y)" or "Feature Z has strongest signal for target (r=0.72)".

   ### Distribution Profile (`report_type: "distribution_profile"`)
   - **Skewed features:** Identify columns with |skewness| > 1. Suggest log transform for right-skewed, sqrt for moderately skewed.
   - **High kurtosis:** Values > 3 indicate heavy tails — outlier-prone.
   - **Target balance:** If classification, report class ratios. Flag imbalance > 3:1.
   - **High cardinality categoricals:** Flag columns with >20 unique values — suggest target encoding.
   - **Action items:** "Column X is heavily right-skewed (skew=2.3) — apply log transform" or "Target is imbalanced (80/20) — use class weights or SMOTE".

   ### Missing Analysis (`report_type: "missing_analysis"`)
   - **High missing (>30%):** Recommend dropping the column or using a missing indicator.
   - **Medium missing (5-30%):** Recommend investigating if MNAR before choosing imputation.
   - **Low missing (<5%):** Safe to impute with mean/median/mode.
   - **Patterns:** If multiple columns are missing together, suggest they may share a cause.
   - **Action items:** "Drop column X (45% missing)" or "Impute column Y with median (2% missing)".

   ### Outlier Detection (`report_type: "outlier_detection"`)
   - **High outlier rate (>5%):** The threshold may be too aggressive, or data is naturally heavy-tailed.
   - **Extreme outliers:** Values far from the fence — likely data errors.
   - **Column comparison:** Which columns are most affected.
   - **Action items:** "Cap column X at upper fence (150.3)" or "Column Y has extreme outlier at 99999 — likely data error, investigate".

4. **Also check the `warnings` array** in the report — these are pre-computed alerts from the node. Include them in your interpretation.

5. **Format your response:**

   ```
   ## Report: [Report Type] for node `{node_id}`

   ### Key Findings
   1. [Most important finding]
   2. [Second finding]
   3. [Third finding]

   ### Recommendations
   - [ ] [Actionable step 1]
   - [ ] [Actionable step 2]
   - [ ] [Actionable step 3]

   ### Warnings
   - [Warning from report]
   ```

6. **Cross-reference with other reports if available.** If you can find other EDA reports in the same run, note connections (e.g., "Column X shows both high missing (from Missing Analysis) and extreme outliers (from Outlier Detection) — the outliers may be misencoded missing values").

## Notes
- Be specific: use actual column names, values, and statistics from the report.
- Prioritize actionable advice over exhaustive description.
- Frame recommendations in terms of what the user should do next in the pipeline.
- If a report shows no issues (no warnings, balanced target, etc.), say so — it's useful to confirm the data is clean.
