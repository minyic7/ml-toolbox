import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";

interface TrainingMetricsReportProps {
  data: Record<string, unknown>;
  analysis?: CcAnalysis | null;
}

type SplitMetrics = {
  accuracy?: number;
  f1?: number;
  precision?: number;
  recall?: number;
  auc?: number;
};

const SPLIT_ORDER = ["train", "val", "test"] as const;

const METRIC_LABELS: Record<string, string> = {
  accuracy: "Accuracy",
  f1: "F1",
  precision: "Precision",
  recall: "Recall",
  auc: "AUC",
};

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

export function TrainingMetricsReport({ data }: TrainingMetricsReportProps) {
  const splits = SPLIT_ORDER.filter(
    (s) => data[s] && typeof data[s] === "object",
  );

  if (splits.length === 0) {
    return (
      <div className="output-empty" style={{ padding: 12 }}>
        No training metrics available
      </div>
    );
  }

  // Use train metrics for summary cards (primary split)
  const primaryMetrics = data[splits[0]] as SplitMetrics;
  const summaryItems = [
    { label: "Accuracy", value: primaryMetrics.accuracy ?? 0 },
    { label: "F1 Score", value: primaryMetrics.f1 ?? 0 },
    ...(primaryMetrics.auc != null
      ? [{ label: "AUC", value: primaryMetrics.auc }]
      : []),
  ];

  const metricKeys = Object.keys(METRIC_LABELS).filter((k) =>
    splits.some((s) => (data[s] as SplitMetrics)[k as keyof SplitMetrics] != null),
  );

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      <div style={SECTION_HEADER}>Metrics by Split</div>
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
              <th style={thStyle}>Metric</th>
              {splits.map((s) => (
                <th key={s} style={thStyle}>
                  {s.charAt(0).toUpperCase() + s.slice(1)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metricKeys.map((metricKey, i) => (
              <tr
                key={metricKey}
                style={{
                  backgroundColor:
                    i % 2 === 1
                      ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                      : "transparent",
                }}
              >
                <td style={{ ...cellStyle, fontWeight: 600 }}>
                  {METRIC_LABELS[metricKey]}
                </td>
                {splits.map((s) => {
                  const val = (data[s] as SplitMetrics)[
                    metricKey as keyof SplitMetrics
                  ];
                  return (
                    <td key={s} style={cellStyle}>
                      {val != null ? <MetricWithBar value={val} /> : "—"}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ---------- Styles ---------- */

const thStyle: React.CSSProperties = {
  padding: "6px 8px",
  textAlign: "left",
  fontFamily: "'Inter', sans-serif",
  fontSize: 10,
  fontWeight: 600,
  color: "var(--text-secondary)",
  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
  borderBottom: "1px solid var(--border-default)",
};

const cellStyle: React.CSSProperties = {
  padding: "4px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

/* ---------- Metric value with inline bar ---------- */

function MetricWithBar({ value }: { value: number }) {
  const pct = value * 100;
  const color =
    value >= 0.8
      ? "rgba(34, 197, 94, 0.7)"
      : value >= 0.5
        ? "rgba(234, 179, 8, 0.7)"
        : "rgba(239, 68, 68, 0.7)";

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 90 }}>
      <span style={{ width: 38, textAlign: "right" }}>{pct.toFixed(1)}%</span>
      <div
        style={{
          flex: 1,
          height: 8,
          backgroundColor: "var(--border-default)",
          borderRadius: 3,
          overflow: "hidden",
          minWidth: 40,
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: "100%",
            backgroundColor: color,
            borderRadius: 3,
          }}
        />
      </div>
    </div>
  );
}
