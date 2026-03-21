"""EDA (Exploratory Data Analysis) nodes."""

import json
from pathlib import Path

from ml_toolbox.protocol import PortType, Select, Text, node


def _get_output_path(name: str = "output", ext: str = ".json") -> Path:
    """Return the output path for a node artifact.

    At runtime this is overridden by the sandbox runner to point at the
    container's scratch volume.  During development / tests it falls back
    to a temp-style local path.
    """
    p = Path("/tmp/ml_toolbox_outputs")
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{name}{ext}"


@node(
    inputs={"df": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={
        "method": Select(["pearson", "spearman", "both"], default="pearson",
                        description="Correlation method"),
        "target_column": Text(default="", description="Target column to rank feature correlations",
                             placeholder="target"),
    },
    label="Correlation Matrix",
    category="Eda",
    description="Compute pairwise correlation matrix for numeric columns.",
    guide="""## Correlation Matrix\n\nFind linear (Pearson) or monotonic (Spearman) relationships between numeric features.\n\n### What it does\n- Full pairwise correlation matrix for all numeric columns\n- Flags highly correlated pairs (|r| > 0.8) — collinearity risk\n- Ranks features by correlation to target (if specified)\n\n### When to act\n- **|r| > 0.8 between features**: consider dropping one — collinear features add noise and inflate variance in linear models\n- **|r| > 0.5 with target**: strong predictive signal — keep these features\n- **|r| < 0.05 with target**: weak signal — candidate for removal to reduce dimensionality\n\n### Pearson vs Spearman\n- **Pearson**: measures linear relationship. Sensitive to outliers.\n- **Spearman**: measures monotonic relationship. Robust to outliers and non-linear monotonic trends.\n- **Both**: compute both and compare — big differences hint at non-linear relationships.""",
)
def correlation_matrix(inputs: dict, params: dict) -> dict:
    """Compute pairwise correlation matrix for numeric columns."""
    import pandas as pd

    df = pd.read_parquet(inputs["df"])
    numeric_df = df.select_dtypes(include="number")
    method = params.get("method", "pearson")
    target_column = params.get("target_column", "")

    cols = list(numeric_df.columns)
    n_cols = len(cols)

    if n_cols < 2:
        report = {
            "report_type": "correlation_matrix",
            "method": method,
            "summary": {
                "numeric_columns": n_cols,
                "total_pairs": 0,
                "high_correlation_pairs": 0,
            },
            "matrix": {"columns": cols, "values": [[1.0]] if n_cols == 1 else []},
            "top_pairs": [],
            "warnings": [
                {
                    "type": "insufficient_columns",
                    "message": f"Need at least 2 numeric columns, got {n_cols}",
                }
            ],
        }
        if target_column:
            report["target_correlations"] = []
        out = _get_output_path("report")
        out.write_text(json.dumps(report))
        return {"report": str(out)}

    def _compute(corr_df: pd.DataFrame, method_name: str) -> dict:
        values = corr_df.values.tolist()

        # Extract upper-triangle pairs
        pairs = []
        for i in range(n_cols):
            for j in range(i + 1, n_cols):
                r = round(values[i][j], 6)
                pairs.append({"a": cols[i], "b": cols[j], "r": r, "abs_r": round(abs(r), 6)})
        pairs.sort(key=lambda p: p["abs_r"], reverse=True)

        # Warnings for high correlation
        warnings = []
        for p in pairs:
            if p["abs_r"] > 0.8:
                warnings.append({
                    "type": "high_correlation",
                    "columns": [p["a"], p["b"]],
                    "r": p["r"],
                    "message": f"{p['a']} ↔ {p['b']}: r={p['r']} — high collinearity",
                })

        result = {
            "report_type": "correlation_matrix",
            "method": method_name,
            "summary": {
                "numeric_columns": n_cols,
                "total_pairs": len(pairs),
                "high_correlation_pairs": sum(1 for p in pairs if p["abs_r"] > 0.8),
            },
            "matrix": {"columns": cols, "values": [[round(v, 6) for v in row] for row in values]},
            "top_pairs": pairs,
            "warnings": warnings,
        }

        # Target correlations
        if target_column and target_column in cols:
            target_corrs = []
            target_idx = cols.index(target_column)
            for i, col in enumerate(cols):
                if col == target_column:
                    continue
                r = round(values[i][target_idx], 6)
                target_corrs.append({"feature": col, "r": r})
            target_corrs.sort(key=lambda x: abs(x["r"]), reverse=True)
            result["target_correlations"] = target_corrs

        return result

    if method == "both":
        corr_pearson = numeric_df.corr(method="pearson")
        corr_spearman = numeric_df.corr(method="spearman")
        report_pearson = _compute(corr_pearson, "pearson")
        report_spearman = _compute(corr_spearman, "spearman")

        report = {
            "report_type": "correlation_matrix",
            "method": "both",
            "summary": report_pearson["summary"],
            "matrix_pearson": report_pearson["matrix"],
            "matrix_spearman": report_spearman["matrix"],
            "top_pairs": report_pearson["top_pairs"],
            "warnings": report_pearson["warnings"],
        }
        if "target_correlations" in report_pearson:
            report["target_correlations"] = report_pearson["target_correlations"]
    else:
        corr_df = numeric_df.corr(method=method)
        report = _compute(corr_df, method)

    out = _get_output_path("report")
    out.write_text(json.dumps(report))
    return {"report": str(out)}


@node(
    inputs={"df": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={
        "target_column": Text(
            default="",
            description="Target column for special analysis (class balance / distribution)",
            placeholder="target",
        ),
    },
    label="Distribution Profile",
    category="Eda",
    description="Profile all columns: dtype, stats, distribution shape, value counts.",
    guide=(
        "## Distribution Profile\n\n"
        "Analyze the statistical distribution of every column in your dataset.\n\n"
        "### What it does\n"
        "- **Numeric columns**: count, mean, median, std, min, max, skewness, kurtosis, "
        "percentiles (Q25/Q50/Q75)\n"
        "- **Categorical columns**: cardinality (unique count), top N values with counts "
        "and percentages\n"
        "- **Target column** (if specified): class balance for classification, or "
        "distribution stats for regression\n\n"
        "### Why it matters\n"
        "- **Skewed distributions** may need log/sqrt transforms for linear models\n"
        "- **High cardinality** categoricals may need encoding strategies (target encoding "
        "vs one-hot)\n"
        "- **Class imbalance** in the target affects model training — may need SMOTE or "
        "class weights\n\n"
        "### Remember\n"
        "All statistics come from **train only**. Never peek at val/test distributions."
    ),
)
def distribution_profile(inputs: dict, params: dict) -> dict:
    """Profile all columns: dtype, stats, distribution shape, value counts."""
    from typing import Any, cast

    import numpy as np
    import pandas as pd

    def _scalar(v: Any) -> float:
        """Extract a scalar float from a pandas aggregate result."""
        return float(v)

    df = pd.read_parquet(inputs["df"])

    target_column = params.get("target_column", "")
    total_rows = len(df)
    total_columns = len(df.columns)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    categorical_cols = df.select_dtypes(exclude="number").columns.tolist()

    columns_report: list[dict] = []
    warnings: list[dict] = []

    for col in df.columns:
        role = "target" if col == target_column else "feature"

        if col in numeric_cols:
            series = df[col].dropna()
            count = int(cast(int, series.count()))
            stats: dict = {
                "count": count,
                "mean": round(_scalar(series.mean()), 4),
                "median": round(_scalar(series.median()), 4),
                "std": round(_scalar(series.std()), 4),
                "min": round(_scalar(series.min()), 4),
                "max": round(_scalar(series.max()), 4),
                "skewness": round(_scalar(series.skew()), 4),
                "kurtosis": round(_scalar(series.kurtosis()), 4),
                "q25": round(_scalar(series.quantile(0.25)), 4),
                "q50": round(_scalar(series.quantile(0.50)), 4),
                "q75": round(_scalar(series.quantile(0.75)), 4),
            }

            counts_arr, bin_edges_arr = np.histogram(series, bins=10)
            histogram = {
                "bin_edges": [round(float(e), 4) for e in bin_edges_arr],
                "counts": [int(c) for c in counts_arr],
            }

            entry: dict = {
                "name": col,
                "dtype": str(df[col].dtype),
                "role": role,
                "stats": stats,
                "histogram": histogram,
            }
            columns_report.append(entry)

            # Skewness warning
            skew_val = stats["skewness"]
            abs_skew = abs(skew_val)
            if abs_skew >= 0.5:
                direction = "right" if skew_val > 0 else "left"
                severity = "strong" if abs_skew >= 1.0 else "moderate"
                warnings.append({
                    "column": col,
                    "type": "skewed",
                    "message": (
                        f"Skewness {skew_val} — {severity} {direction} skew, "
                        f"consider log transform for linear models"
                    ),
                })
        else:
            # Categorical column
            value_counts = df[col].value_counts()
            count = int(cast(int, df[col].count()))
            cardinality = int(cast(int, df[col].nunique()))
            top_values = []
            for val, cnt in value_counts.head(10).items():
                top_values.append({
                    "value": val,
                    "count": int(cnt),
                    "pct": round(int(cnt) / total_rows, 4) if total_rows > 0 else 0,
                })

            entry = {
                "name": col,
                "dtype": str(df[col].dtype),
                "role": role,
                "stats": {
                    "count": count,
                    "cardinality": cardinality,
                    "top_values": top_values,
                },
            }
            columns_report.append(entry)

            # High cardinality warning
            if cardinality > 20:
                warnings.append({
                    "column": col,
                    "type": "high_cardinality",
                    "message": (
                        f"{cardinality} unique values — consider target encoding "
                        f"instead of one-hot"
                    ),
                })

    report: dict = {
        "report_type": "distribution_profile",
        "summary": {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "numeric_count": len(numeric_cols),
            "categorical_count": len(categorical_cols),
        },
        "columns": columns_report,
        "warnings": warnings,
    }

    # Target section
    if target_column and target_column in df.columns:
        target_dtype = str(df[target_column].dtype)
        if target_column in categorical_cols or int(cast(int, df[target_column].nunique())) <= 20:
            # Classification-style: class balance
            vc = df[target_column].value_counts()
            class_balance = []
            for val, cnt in vc.items():
                class_balance.append({
                    "value": val,
                    "count": int(cnt),
                    "pct": round(int(cnt) / total_rows, 4) if total_rows > 0 else 0,
                })
            report["target"] = {
                "name": target_column,
                "dtype": target_dtype,
                "class_balance": class_balance,
            }

            # Class imbalance warning
            if len(class_balance) >= 2:
                pcts = [cb["pct"] for cb in class_balance]
                ratio = max(pcts) / min(pcts) if min(pcts) > 0 else float("inf")
                if ratio > 3:
                    warnings.append({
                        "column": target_column,
                        "type": "class_imbalance",
                        "message": (
                            f"Class ratio {ratio:.1f}:1 — consider SMOTE or "
                            f"class weights"
                        ),
                    })
        else:
            # Regression-style: distribution stats
            series = df[target_column].dropna()
            report["target"] = {
                "name": target_column,
                "dtype": target_dtype,
                "distribution": {
                    "mean": round(_scalar(series.mean()), 4),
                    "median": round(_scalar(series.median()), 4),
                    "std": round(_scalar(series.std()), 4),
                    "min": round(_scalar(series.min()), 4),
                    "max": round(_scalar(series.max()), 4),
                },
            }

    out = _get_output_path("report", ext=".json")
    out.write_text(json.dumps(report, indent=2))
    return {"report": str(out)}


@node(
    inputs={"df": PortType.TABLE},
    outputs={"report": PortType.METRICS},
    params={},
    label="Missing Analysis",
    category="Eda",
    description="Analyze missing value patterns across all columns.",
    guide="""## Missing Analysis\n\nUnderstand where and how much data is missing before deciding on a strategy.\n\n### What it does\n- Per-column missing count and percentage\n- Overall dataset completeness\n- Missing severity classification (none / low <5% / medium 5-30% / high >30%)\n- Complete rows ratio\n\n### How to interpret\n- **Low missing (<5%)**: safe to impute with mean/median/mode\n- **Medium missing (5-30%)**: investigate if MAR or MNAR before imputing\n- **High missing (>30%)**: consider dropping the column or using a missing indicator flag\n- **MNAR (Missing Not At Random)**: the missingness itself carries information — add a binary flag column\n\n### Remember\nMissing analysis on train only. Apply the same imputation strategy (fitted on train) to val/test.""",
)
def missing_analysis(inputs: dict, params: dict) -> dict:
    """Analyze missing value patterns across all columns."""
    import pandas as pd

    df = pd.read_parquet(inputs["df"])

    total_rows = len(df)
    total_columns = len(df.columns)
    total_cells = total_rows * total_columns

    missing_counts = df.isnull().sum()
    missing_pcts = df.isnull().mean()
    complete_rows = int(df.dropna().shape[0])

    total_missing_cells = int(missing_counts.sum())
    overall_missing_pct = round(total_missing_cells / total_cells, 4) if total_cells > 0 else 0.0

    # Build per-column entries (only columns with missing > 0), sorted by missing_pct desc
    columns = []
    for col in df.columns:
        mc = int(missing_counts[col].item())
        if mc == 0:
            continue
        mp = round(float(missing_pcts[col].item()), 4)
        if mp > 0.30:
            severity = "high"
        elif mp >= 0.05:
            severity = "medium"
        else:
            severity = "low"
        columns.append({
            "name": col,
            "missing_count": mc,
            "missing_pct": mp,
            "severity": severity,
            "present_count": total_rows - mc,
        })

    columns.sort(key=lambda c: c["missing_pct"], reverse=True)

    no_missing_count = total_columns - len(columns)

    # Generate warnings for medium/high missing columns
    warnings = []
    for col_info in columns:
        pct_display = round(col_info["missing_pct"] * 100, 1)
        if col_info["severity"] == "high":
            warnings.append({
                "column": col_info["name"],
                "type": "critical_missing",
                "message": f"{pct_display}% missing — consider dropping or adding a missing indicator",
            })
        elif col_info["severity"] == "medium":
            warnings.append({
                "column": col_info["name"],
                "type": "high_missing",
                "message": f"{pct_display}% missing — investigate MAR vs MNAR before imputing",
            })

    report = {
        "report_type": "missing_analysis",
        "summary": {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_missing_cells": total_missing_cells,
            "total_cells": total_cells,
            "overall_missing_pct": overall_missing_pct,
            "complete_rows": complete_rows,
            "complete_rows_pct": round(complete_rows / total_rows, 4) if total_rows > 0 else 0.0,
            "no_missing_count": no_missing_count,
        },
        "columns": columns,
        "warnings": warnings,
    }

    out_path = _get_output_path("report")
    out_path.write_text(json.dumps(report, indent=2))
    return {"report": str(out_path)}
