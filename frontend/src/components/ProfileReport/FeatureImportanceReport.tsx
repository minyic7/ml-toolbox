import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface FeatureImportanceReportProps {
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

const METHOD_LABELS: Record<string, string> = {
  tree_importance: "Gini / Variance Reduction",
  coefficient_magnitude: "Coefficient Magnitude",
  unsupported: "Unsupported",
};

export function FeatureImportanceReport({ data, analysis }: FeatureImportanceReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const features = (data.features ?? []) as {
    name: string;
    importance: number;
    raw_importance: number;
  }[];
  const method = (data.method ?? "") as string;
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];
  const aiWarnings = analysis?.warnings ?? [];

  const summaryItems = [
    { label: "Features", value: (summary?.feature_count as number) ?? 0 },
    { label: "Model", value: (summary?.model_type as string) ?? "—" },
    { label: "Method", value: METHOD_LABELS[method] ?? method },
    {
      label: "Top Feature",
      value: (summary?.top_feature as string) ?? "—",
    },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {features.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Feature Importances</div>
          <FeatureBarChart features={features} />
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

/* ---------- Horizontal bar chart ---------- */

function FeatureBarChart({
  features,
}: {
  features: { name: string; importance: number; raw_importance: number }[];
}) {
  const maxImportance = features.length > 0 ? features[0].importance : 1;

  return (
    <div
      style={{
        border: "1px solid var(--border-default)",
        borderRadius: 6,
        overflow: "hidden",
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
            {["Feature", "Importance", ""].map((h, i) => (
              <th
                key={i}
                style={{
                  padding: "6px 8px",
                  textAlign: "left",
                  fontFamily: "'Inter', sans-serif",
                  fontSize: 10,
                  fontWeight: 600,
                  color: "var(--text-secondary)",
                  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                  borderBottom: "1px solid var(--border-default)",
                  width: i === 0 ? "30%" : i === 1 ? "15%" : "55%",
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {features.map((f, i) => {
            const barWidth = maxImportance > 0 ? (f.importance / maxImportance) * 100 : 0;
            return (
              <tr
                key={f.name}
                style={{
                  backgroundColor:
                    i % 2 === 1
                      ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                      : "transparent",
                }}
              >
                <td
                  style={{
                    padding: "4px 8px",
                    color: "var(--text-primary)",
                    borderBottom: "1px solid var(--border-default)",
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    maxWidth: 160,
                  }}
                  title={f.name}
                >
                  {f.name}
                </td>
                <td
                  style={{
                    padding: "4px 8px",
                    color: "var(--text-primary)",
                    borderBottom: "1px solid var(--border-default)",
                    whiteSpace: "nowrap",
                  }}
                >
                  {(f.importance * 100).toFixed(1)}%
                </td>
                <td
                  style={{
                    padding: "4px 8px",
                    borderBottom: "1px solid var(--border-default)",
                  }}
                >
                  <div
                    style={{
                      height: 12,
                      backgroundColor: "var(--border-default)",
                      borderRadius: 3,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        width: `${barWidth}%`,
                        height: "100%",
                        backgroundColor: barColor(f.importance),
                        borderRadius: 3,
                        transition: "width 0.2s ease",
                      }}
                    />
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function barColor(importance: number): string {
  if (importance >= 0.2) return "rgba(245, 158, 11, 0.8)";   // amber — high
  if (importance >= 0.05) return "rgba(245, 158, 11, 0.5)";  // amber — medium
  return "rgba(245, 158, 11, 0.25)";                         // amber — low
}
