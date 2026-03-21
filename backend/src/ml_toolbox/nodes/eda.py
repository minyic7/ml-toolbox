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
                    "message": f"{p['a']} \u2194 {p['b']}: r={p['r']} \u2014 high collinearity",
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
