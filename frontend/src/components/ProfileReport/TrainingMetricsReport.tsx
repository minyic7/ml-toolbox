import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface TrainingMetricsReportProps {
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

/** Format a metric value for display. */
function fmtMetric(v: unknown): string {
  if (typeof v !== "number") return String(v ?? "—");
  if (Number.isInteger(v)) return v.toLocaleString();
  return v.toFixed(4);
}

/** Color-code a metric comparison — green if better, red if worse. */
function diffColor(
  trainVal: number | undefined,
  splitVal: number | undefined,
  higherIsBetter: boolean,
): string | undefined {
  if (trainVal === undefined || splitVal === undefined) return undefined;
  const diff = splitVal - trainVal;
  if (Math.abs(diff) < 0.005) return undefined; // within noise
  const isBetter = higherIsBetter ? diff > 0 : diff < 0;
  return isBetter ? "#16a34a" : "#dc2626";
}

const HIGHER_IS_BETTER: Record<string, boolean> = {
  accuracy: true,
  f1_macro: true,
  precision_macro: true,
  recall_macro: true,
  auc: true,
  r2: true,
  mae: false,
  rmse: false,
};

export function TrainingMetricsReport({ data }: TrainingMetricsReportProps) {
  const taskType = data.task_type as string;
  const splits = data.splits as Record<string, Record<string, number>> | undefined;
  const splitOrder = (data.split_order ?? []) as string[];
  const metricInfo = (data.metric_info ?? {}) as Record<string, string>;
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];

  if (!splits || splitOrder.length === 0) {
    return <div className="output-empty">No metrics computed</div>;
  }

  // Collect all metric keys (excluding "support")
  const firstSplit = splits[splitOrder[0]];
  const metricKeys = Object.keys(firstSplit).filter((k) => k !== "support");

  // Summary cards: total samples, task type, split count
  const totalSamples = splitOrder.reduce(
    (sum, s) => sum + (splits[s]?.support ?? 0),
    0,
  );
  const summaryItems = [
    {
      label: "Task Type",
      value: taskType === "classification" ? "Classification" : "Regression",
    },
    { label: "Splits", value: splitOrder.length },
    { label: "Total Samples", value: totalSamples },
  ];

  const trainMetrics = splits["train"];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {/* Metrics comparison table */}
      <div style={SECTION_HEADER}>Metrics by Split</div>
      <div style={{ overflowX: "auto" }}>
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontSize: 12,
            fontFamily: "'JetBrains Mono', monospace",
          }}
        >
          <thead>
            <tr>
              <th
                style={{
                  textAlign: "left",
                  padding: "6px 10px",
                  fontSize: 10,
                  fontWeight: 600,
                  color: "var(--text-muted)",
                  textTransform: "uppercase",
                  fontFamily: "'Inter', sans-serif",
                  borderBottom: "1px solid var(--border-default)",
                }}
              >
                Metric
              </th>
              {splitOrder.map((split) => (
                <th
                  key={split}
                  style={{
                    textAlign: "right",
                    padding: "6px 10px",
                    fontSize: 10,
                    fontWeight: 600,
                    color: "var(--text-muted)",
                    textTransform: "uppercase",
                    fontFamily: "'Inter', sans-serif",
                    borderBottom: "1px solid var(--border-default)",
                  }}
                >
                  {split}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metricKeys.map((metric) => (
              <tr
                key={metric}
                style={{
                  borderBottom: "1px solid var(--border-default)",
                }}
              >
                <td
                  style={{
                    padding: "8px 10px",
                    fontWeight: 500,
                    color: "var(--text-primary)",
                    fontFamily: "'Inter', sans-serif",
                    fontSize: 11,
                  }}
                  title={metricInfo[metric] ?? ""}
                >
                  {metric}
                </td>
                {splitOrder.map((split) => {
                  const val = splits[split]?.[metric];
                  const hib = HIGHER_IS_BETTER[metric] ?? true;
                  const color =
                    split !== "train" && trainMetrics
                      ? diffColor(trainMetrics[metric], val, hib)
                      : undefined;
                  return (
                    <td
                      key={split}
                      style={{
                        textAlign: "right",
                        padding: "8px 10px",
                        color: color ?? "var(--text-primary)",
                        fontWeight: color ? 600 : 400,
                      }}
                    >
                      {fmtMetric(val)}
                    </td>
                  );
                })}
              </tr>
            ))}
            {/* Support row */}
            <tr>
              <td
                style={{
                  padding: "8px 10px",
                  fontFamily: "'Inter', sans-serif",
                  fontSize: 11,
                  color: "var(--text-muted)",
                }}
              >
                support
              </td>
              {splitOrder.map((split) => (
                <td
                  key={split}
                  style={{
                    textAlign: "right",
                    padding: "8px 10px",
                    color: "var(--text-muted)",
                  }}
                >
                  {(splits[split]?.support ?? 0).toLocaleString()}
                </td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>

      {/* Per-split metric cards for quick visual comparison */}
      {splitOrder.length > 1 && (
        <>
          <div style={{ ...SECTION_HEADER, marginTop: 20 }}>
            Split Comparison
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: `repeat(${Math.min(splitOrder.length, 3)}, 1fr)`,
              gap: 10,
            }}
          >
            {splitOrder.map((split) => {
              const m = splits[split];
              if (!m) return null;
              return (
                <div
                  key={split}
                  style={{
                    border: "1px solid var(--border-default)",
                    borderRadius: 8,
                    padding: "10px 12px",
                    boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
                  }}
                >
                  <div
                    style={{
                      fontSize: 10,
                      fontWeight: 700,
                      textTransform: "uppercase",
                      color: "var(--text-muted)",
                      letterSpacing: "0.05em",
                      marginBottom: 8,
                      fontFamily: "'Inter', sans-serif",
                    }}
                  >
                    {split}
                  </div>
                  {metricKeys.map((metric) => (
                    <div
                      key={metric}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        padding: "3px 0",
                        fontSize: 11,
                      }}
                    >
                      <span
                        style={{
                          color: "var(--text-secondary)",
                          fontFamily: "'Inter', sans-serif",
                        }}
                      >
                        {metric}
                      </span>
                      <span
                        style={{
                          fontFamily: "'JetBrains Mono', monospace",
                          fontWeight: 600,
                          color: "var(--text-primary)",
                        }}
                      >
                        {fmtMetric(m[metric])}
                      </span>
                    </div>
                  ))}
                  <div
                    style={{
                      fontSize: 10,
                      color: "var(--text-muted)",
                      marginTop: 6,
                      fontFamily: "'JetBrains Mono', monospace",
                    }}
                  >
                    n={m.support?.toLocaleString()}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Per-label breakdown (classification only) */}
      {taskType === "classification" && <PerLabelBreakdown splits={splits} splitOrder={splitOrder} />}

      <WarningList warnings={warnings} />
    </div>
  );
}

// ── Per-Label Breakdown ──────────────────────────────────────────

function PerLabelBreakdown({
  splits,
  splitOrder,
}: {
  splits: Record<string, Record<string, unknown>>;
  splitOrder: string[];
}) {
  // Use the first split that has per_label data
  const perLabelBySplit = splitOrder
    .map((s) => ({
      split: s,
      perLabel: splits[s]?.per_label as Record<string, Record<string, number>> | undefined,
    }))
    .filter((x) => x.perLabel && Object.keys(x.perLabel).length > 0);

  if (perLabelBySplit.length === 0) return null;

  return (
    <>
      {perLabelBySplit.map(({ split, perLabel }) => {
        if (!perLabel) return null;
        const labels = Object.keys(perLabel);
        const metrics = ["precision", "recall", "f1", "support"] as const;

        return (
          <div key={split}>
            <div style={{ ...SECTION_HEADER, marginTop: 20 }}>
              Per-Label Metrics ({split})
            </div>
            <div style={{ overflowX: "auto" }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                  fontFamily: "'JetBrains Mono', monospace",
                }}
              >
                <thead>
                  <tr>
                    <th style={perLabelThStyle}>Label</th>
                    {metrics.map((m) => (
                      <th key={m} style={{ ...perLabelThStyle, textAlign: "right" }}>
                        {m}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {labels.map((label) => {
                    const lm = perLabel[label];
                    return (
                      <tr key={label} style={{ borderBottom: "1px solid var(--border-default)" }}>
                        <td style={{ padding: "6px 10px", fontWeight: 500, fontFamily: "'Inter', sans-serif", fontSize: 11 }}>
                          {label}
                        </td>
                        {metrics.map((m) => (
                          <td key={m} style={{ textAlign: "right", padding: "6px 10px" }}>
                            {m === "support" ? (lm[m] ?? 0).toLocaleString() : fmtMetric(lm[m])}
                          </td>
                        ))}
                      </tr>
                    );
                  })}
                  {/* Macro avg row */}
                  <tr style={{ borderTop: "2px solid var(--border-default)" }}>
                    <td style={{ padding: "6px 10px", fontWeight: 700, fontFamily: "'Inter', sans-serif", fontSize: 11, color: "var(--text-secondary)" }}>
                      macro avg
                    </td>
                    {metrics.map((m) => {
                      if (m === "support") {
                        const total = labels.reduce((s, l) => s + (perLabel[l]?.support ?? 0), 0);
                        return <td key={m} style={{ textAlign: "right", padding: "6px 10px", fontWeight: 600 }}>{total.toLocaleString()}</td>;
                      }
                      const avg = labels.reduce((s, l) => s + (perLabel[l]?.[m] ?? 0), 0) / labels.length;
                      return <td key={m} style={{ textAlign: "right", padding: "6px 10px", fontWeight: 600 }}>{fmtMetric(avg)}</td>;
                    })}
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </>
  );
}

const perLabelThStyle: React.CSSProperties = {
  textAlign: "left",
  padding: "6px 10px",
  fontSize: 10,
  fontWeight: 600,
  color: "var(--text-muted)",
  textTransform: "uppercase",
  fontFamily: "'Inter', sans-serif",
  borderBottom: "1px solid var(--border-default)",
};
