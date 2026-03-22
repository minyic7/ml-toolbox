import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface ModelComparisonReportProps {
  data: Record<string, unknown>;
  analysis?: CcAnalysis | null;
}

const SECTION_HEADER: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 600,
  fontFamily: "'Inter', sans-serif",
  textTransform: "uppercase",
  color: "var(--text-secondary)",
  borderBottom: "1px solid var(--border-default)",
  paddingBottom: 6,
  marginBottom: 8,
  marginTop: 16,
};

const cellStyle: React.CSSProperties = {
  padding: "6px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

const headerCellStyle: React.CSSProperties = {
  padding: "6px 8px",
  textAlign: "left",
  fontFamily: "'Inter', sans-serif",
  fontSize: 10,
  fontWeight: 600,
  color: "var(--text-secondary)",
  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
  borderBottom: "1px solid var(--border-default)",
};

/** Metrics where higher = better */
const HIGHER_IS_BETTER = new Set(["accuracy", "precision", "recall", "f1_score", "r2"]);
/** Metrics where lower = better */
const LOWER_IS_BETTER = new Set(["mse", "rmse", "mae"]);

function formatMetricName(name: string): string {
  const MAP: Record<string, string> = {
    accuracy: "Accuracy",
    precision: "Precision",
    recall: "Recall",
    f1_score: "F1 Score",
    mse: "MSE",
    rmse: "RMSE",
    mae: "MAE",
    r2: "R\u00b2",
  };
  return MAP[name] ?? name;
}

function formatValue(val: number, metric: string): string {
  if (HIGHER_IS_BETTER.has(metric)) {
    // Percentage-like metrics: show 4 decimal places
    return val.toFixed(4);
  }
  // Error metrics: show 4 decimals
  return val.toFixed(4);
}

export function ModelComparisonReport({ data, analysis }: ModelComparisonReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const table = data.comparison_table as Record<string, unknown> | undefined;
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];
  const aiWarnings = analysis?.warnings ?? [];

  const taskType = (summary?.task_type as string) ?? "unknown";
  const modelsCompared = (summary?.models_compared as number) ?? 0;
  const testRows = (summary?.test_rows as number) ?? 0;
  const features = (summary?.features as number) ?? 0;

  const metricNames = (table?.metric_names ?? []) as string[];
  const modelNames = (table?.models ?? []) as string[];
  const values = (table?.values ?? {}) as Record<string, number[]>;
  const best = (table?.best ?? {}) as Record<string, string[]>;

  const summaryItems = [
    { label: "Models", value: modelsCompared },
    { label: "Task", value: taskType === "classification" ? "Classification" : "Regression" },
    { label: "Test Rows", value: testRows },
    { label: "Features", value: features },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {metricNames.length > 0 && modelNames.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Side-by-Side Comparison</div>
          <ComparisonTable
            metricNames={metricNames}
            modelNames={modelNames}
            values={values}
            best={best}
          />
        </>
      )}

      {aiWarnings.length > 0 ? (
        <WarningList
          warnings={aiWarnings.map((w) => ({
            type: w.type,
            column: w.column ?? undefined,
            message: w.message,
          }))}
          source="ai"
        />
      ) : (
        <WarningList warnings={warnings} />
      )}
    </div>
  );
}

/* ---------- Comparison table ---------- */

function ComparisonTable({
  metricNames,
  modelNames,
  values,
  best,
}: {
  metricNames: string[];
  modelNames: string[];
  values: Record<string, number[]>;
  best: Record<string, string[]>;
}) {
  return (
    <div
      style={{
        overflowX: "auto",
        border: "1px solid var(--border-default)",
        borderRadius: 6,
        marginBottom: 12,
      }}
    >
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 10,
        }}
      >
        <thead>
          <tr>
            <th style={{ ...headerCellStyle, position: "sticky", left: 0, zIndex: 1 }}>
              Metric
            </th>
            {modelNames.map((name) => (
              <th key={name} style={headerCellStyle}>
                {name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {metricNames.map((metric, rowIdx) => {
            const metricValues = values[metric] ?? [];
            const bestModels = new Set(best[metric] ?? []);
            const direction = HIGHER_IS_BETTER.has(metric) ? "higher" : LOWER_IS_BETTER.has(metric) ? "lower" : null;

            return (
              <tr
                key={metric}
                style={{
                  backgroundColor:
                    rowIdx % 2 === 1
                      ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                      : "transparent",
                }}
              >
                <td
                  style={{
                    ...cellStyle,
                    fontFamily: "'Inter', sans-serif",
                    fontWeight: 600,
                    color: "var(--text-secondary)",
                    position: "sticky",
                    left: 0,
                    backgroundColor:
                      rowIdx % 2 === 1
                        ? "var(--output-row-hover, #fafafa)"
                        : "var(--node-bg, #ffffff)",
                    zIndex: 1,
                  }}
                >
                  {formatMetricName(metric)}
                  {direction && (
                    <span
                      style={{
                        fontSize: 8,
                        color: "var(--text-muted)",
                        marginLeft: 4,
                      }}
                    >
                      {direction === "higher" ? "\u2191" : "\u2193"}
                    </span>
                  )}
                </td>
                {metricValues.map((val, colIdx) => {
                  const isBest = bestModels.has(modelNames[colIdx]);
                  return (
                    <td
                      key={colIdx}
                      style={{
                        ...cellStyle,
                        fontWeight: isBest ? 700 : 400,
                        backgroundColor: isBest
                          ? "rgba(34, 197, 94, 0.1)"
                          : undefined,
                      }}
                    >
                      {formatValue(val, metric)}
                      {isBest && (
                        <span
                          style={{
                            marginLeft: 4,
                            fontSize: 8,
                            padding: "1px 5px",
                            borderRadius: 9999,
                            backgroundColor: "#dcfce7",
                            color: "#166534",
                            fontWeight: 700,
                            textTransform: "uppercase",
                          }}
                        >
                          best
                        </span>
                      )}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
