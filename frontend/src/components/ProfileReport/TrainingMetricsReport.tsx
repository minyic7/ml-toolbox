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

/** Format a metric value for display (3 decimal places). */
function fmtMetric(v: unknown): string {
  if (typeof v !== "number") return String(v ?? "—");
  if (Number.isInteger(v)) return v.toLocaleString();
  return v.toFixed(3);
}

/** Color-code a metric comparison — green if better, orange-brown if worse. */
function diffColor(
  trainVal: number | undefined,
  splitVal: number | undefined,
  higherIsBetter: boolean,
): string | undefined {
  if (trainVal === undefined || splitVal === undefined) return undefined;
  const diff = splitVal - trainVal;
  if (Math.abs(diff) < 0.005) return undefined;
  const isBetter = higherIsBetter ? diff > 0 : diff < 0;
  return isBetter ? "#16a34a" : "#92400e";
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

/** Human-readable metric names */
const METRIC_LABELS: Record<string, string> = {
  accuracy: "accuracy",
  auc: "auc",
  f1_macro: "f1 macro",
  precision_macro: "precision macro",
  recall_macro: "recall macro",
  mae: "mae",
  rmse: "rmse",
  r2: "r²",
};

export function TrainingMetricsReport({ data }: TrainingMetricsReportProps) {
  const splits = data.splits as Record<string, Record<string, unknown>> | undefined;
  const splitOrder = (data.split_order ?? []) as string[];
  const metricInfo = (data.metric_info ?? {}) as Record<string, string>;
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];
  const classInfo = data.class_info as {
    n_classes?: number;
    majority_label?: string;
    majority_pct?: number;
    is_binary?: boolean;
    is_imbalanced?: boolean;
  } | undefined;
  const totalSamples = (data.total_samples as number) ?? splitOrder.reduce(
    (sum, s) => sum + ((splits?.[s]?.support as number) ?? 0), 0,
  );
  const splitPcts = data.split_pcts as Record<string, number> | undefined;

  if (!splits || splitOrder.length === 0) {
    return <div className="output-empty">No metrics computed</div>;
  }

  // Metric keys: exclude support and nested objects
  const firstSplit = splits[splitOrder[0]];
  const metricKeys = Object.keys(firstSplit).filter(
    (k) => k !== "support" && typeof firstSplit[k] !== "object",
  );

  // Summary card 1: total samples + split ratio
  const splitRatioStr = splitPcts
    ? splitOrder.map((s) => splitPcts[s]).join("/")
    : undefined;
  const samplesValue = splitRatioStr
    ? `${totalSamples.toLocaleString()} · ${splitRatioStr} split`
    : totalSamples.toLocaleString();

  const summaryItems: { label: string; value: string | number }[] = [
    { label: "total samples", value: samplesValue },
  ];

  // Summary card 2: class distribution (classification only)
  if (classInfo?.majority_pct != null && classInfo.majority_label != null) {
    const pctStr = `${Math.round(classInfo.majority_pct * 100)}%`;
    const parts = [`${pctStr} label ${classInfo.majority_label}`];
    if (classInfo.is_imbalanced) parts.push("class imbalance");
    if (classInfo.is_binary) parts.push("binary");
    else if (classInfo.n_classes) parts.push(`${classInfo.n_classes}-class`);
    summaryItems.push({ label: parts.slice(1).join(" · "), value: parts[0] });
  }

  const trainMetrics = splits["train"];

  // Determine which split to show per-label for (prefer test > val > train)
  const perLabelSplit = (["test", "val", "train"] as const).find(
    (s) => splits[s]?.per_label && Object.keys(splits[s].per_label as object).length > 0,
  );
  const perLabelData = perLabelSplit
    ? (splits[perLabelSplit].per_label as Record<string, Record<string, number>>)
    : undefined;

  // Find majority/minority labels for tagging
  const labelTags: Record<string, string> = {};
  if (perLabelData) {
    const labels = Object.keys(perLabelData);
    if (labels.length === 2) {
      const sorted = [...labels].sort(
        (a, b) => (perLabelData[b]?.support ?? 0) - (perLabelData[a]?.support ?? 0),
      );
      labelTags[sorted[0]] = "majority";
      labelTags[sorted[1]] = "minority";
    }
  }

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {/* Overall metrics by split */}
      <div style={SECTION_HEADER}>Overall Metrics by Split</div>
      <div style={{ overflowX: "auto" }}>
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle} />
              {splitOrder.map((split) => (
                <th key={split} style={{ ...thStyle, textAlign: "right" }}>
                  {split}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metricKeys.map((metric) => (
              <tr key={metric} style={{ borderBottom: "1px solid var(--border-default)" }}>
                <td style={metricLabelStyle} title={metricInfo[metric] ?? ""}>
                  {METRIC_LABELS[metric] ?? metric}
                </td>
                {splitOrder.map((split) => {
                  const val = splits[split]?.[metric] as number | undefined;
                  const hib = HIGHER_IS_BETTER[metric] ?? true;
                  const color = split !== "train" && trainMetrics
                    ? diffColor(trainMetrics[metric] as number, val, hib)
                    : undefined;
                  return (
                    <td key={split} style={{
                      textAlign: "right",
                      padding: "8px 10px",
                      color: color ?? "var(--text-primary)",
                      fontWeight: color ? 600 : 400,
                    }}>
                      {fmtMetric(val)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Per-label breakdown — test split only */}
      {perLabelData && perLabelSplit && (
        <PerLabelTable
          perLabel={perLabelData}
          splitName={perLabelSplit}
          labelTags={labelTags}
        />
      )}

      <WarningList warnings={warnings} />
    </div>
  );
}

// ── Per-Label Table ──────────────────────────────────────────────

function PerLabelTable({
  perLabel,
  splitName,
  labelTags,
}: {
  perLabel: Record<string, Record<string, number>>;
  splitName: string;
  labelTags: Record<string, string>;
}) {
  const labels = Object.keys(perLabel);
  const cols = ["precision", "recall", "f1", "n"] as const;

  return (
    <>
      <div style={{ ...SECTION_HEADER, marginTop: 20 }}>
        Per-Label Breakdown · {splitName} split
      </div>
      <div style={{ overflowX: "auto" }}>
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>label</th>
              {cols.map((c) => (
                <th key={c} style={{ ...thStyle, textAlign: "right" }}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {labels.map((label) => {
              const lm = perLabel[label];
              const tag = labelTags[label];
              return (
                <tr key={label} style={{ borderBottom: "1px solid var(--border-default)" }}>
                  <td style={{ padding: "6px 10px", fontWeight: 500, fontFamily: "'Inter', sans-serif", fontSize: 11 }}>
                    {label}
                    {tag && (
                      <span style={{
                        marginLeft: 6,
                        fontSize: 9,
                        color: tag === "minority" ? "#92400e" : "#065f46",
                        fontWeight: 400,
                      }}>
                        {tag}
                      </span>
                    )}
                  </td>
                  {cols.map((c) => {
                    const key = c === "n" ? "support" : c;
                    const val = lm[key];
                    const isLow = c !== "n" && typeof val === "number" && val < 0.5;
                    return (
                      <td key={c} style={{
                        textAlign: "right",
                        padding: "6px 10px",
                        color: isLow ? "#92400e" : undefined,
                        fontWeight: isLow ? 600 : 400,
                      }}>
                        {c === "n" ? (val ?? 0).toLocaleString() : fmtMetric(val)}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </>
  );
}

// ── Shared styles ────────────────────────────────────────────────

const tableStyle: React.CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
  fontSize: 12,
  fontFamily: "'JetBrains Mono', monospace",
};

const thStyle: React.CSSProperties = {
  textAlign: "left",
  padding: "6px 10px",
  fontSize: 10,
  fontWeight: 600,
  color: "var(--text-muted)",
  textTransform: "uppercase",
  fontFamily: "'Inter', sans-serif",
  borderBottom: "1px solid var(--border-default)",
};

const metricLabelStyle: React.CSSProperties = {
  padding: "8px 10px",
  fontWeight: 500,
  color: "var(--text-primary)",
  fontFamily: "'Inter', sans-serif",
  fontSize: 11,
};
