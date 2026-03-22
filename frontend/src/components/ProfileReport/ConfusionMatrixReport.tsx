import { useState } from "react";
import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface ConfusionMatrixReportProps {
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

export function ConfusionMatrixReport({ data, analysis }: ConfusionMatrixReportProps) {
  const summary = data.summary as { total_samples: number; num_classes: number } | undefined;
  const accuracy = (data.accuracy as number) ?? 0;
  const classLabels = (data.class_labels ?? []) as string[];
  const cmRaw = (data.confusion_matrix ?? []) as number[][];
  const cmNormalized = (data.confusion_matrix_normalized ?? []) as number[][];
  const perClass = (data.per_class ?? []) as {
    label: string;
    precision: number;
    recall: number;
    f1: number;
    support: number;
  }[];
  const defaultNormalize = (data.normalize ?? false) as boolean;
  const warnings = (data.warnings ?? []) as { type: string; column?: string; message: string }[];
  const aiWarnings = analysis?.warnings ?? [];

  const [normalized, setNormalized] = useState(defaultNormalize);
  const matrix = normalized ? cmNormalized : cmRaw;

  const summaryItems = [
    { label: "Accuracy", value: accuracy },
    { label: "Samples", value: summary?.total_samples ?? 0 },
    { label: "Classes", value: summary?.num_classes ?? 0 },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {/* Confusion Matrix Heatmap */}
      {classLabels.length > 0 && matrix.length > 0 && (
        <>
          <div style={{ ...SECTION_HEADER, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span>Confusion Matrix</span>
            <NormalizeToggle normalized={normalized} onChange={setNormalized} />
          </div>
          <ColorLegend />
          <HeatmapTable
            labels={classLabels}
            matrix={matrix}
            normalized={normalized}
          />
        </>
      )}

      {/* Per-class metrics */}
      {perClass.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Per-Class Metrics</div>
          <PerClassTable perClass={perClass} />
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

/* ---------- Normalize toggle ---------- */

function NormalizeToggle({
  normalized,
  onChange,
}: {
  normalized: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <div style={{ display: "flex", gap: 2 }}>
      {(["Counts", "Pct"] as const).map((label) => {
        const isActive = label === "Pct" ? normalized : !normalized;
        return (
          <button
            key={label}
            onClick={() => onChange(label === "Pct")}
            style={{
              fontSize: 9,
              fontWeight: isActive ? 700 : 500,
              fontFamily: "'Inter', sans-serif",
              padding: "2px 8px",
              borderRadius: 4,
              border: isActive
                ? "1px solid var(--accent-primary)"
                : "1px solid var(--border-default)",
              background: isActive ? "var(--ghost-hover-bg)" : "transparent",
              color: isActive ? "var(--accent-primary)" : "var(--text-muted)",
              cursor: "pointer",
              textTransform: "uppercase",
              letterSpacing: "0.05em",
            }}
          >
            {label}
          </button>
        );
      })}
    </div>
  );
}

/* ---------- Color legend ---------- */

function ColorLegend() {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 6,
        marginBottom: 8,
        fontSize: 9,
        fontFamily: "'Inter', sans-serif",
        color: "var(--text-secondary)",
      }}
    >
      <span>0</span>
      <div
        style={{
          width: 120,
          height: 10,
          borderRadius: 3,
          background:
            "linear-gradient(to right, rgba(34,197,94,0.05), rgba(34,197,94,0.7))",
          border: "1px solid var(--border-default)",
        }}
      />
      <span>max</span>
      <span style={{ marginLeft: 6, color: "var(--text-muted)" }}>
        diagonal = correct predictions
      </span>
    </div>
  );
}

/* ---------- Heatmap cell color ---------- */

function heatmapCellColor(value: number, maxVal: number, isDiagonal: boolean): string {
  if (maxVal === 0) return "transparent";
  const intensity = value / maxVal;
  if (isDiagonal) {
    // Green for correct predictions
    const opacity = 0.08 + intensity * 0.62;
    return `rgba(34, 197, 94, ${opacity})`;
  }
  // Red/orange for misclassifications
  if (value === 0) return "transparent";
  const opacity = 0.08 + intensity * 0.52;
  return `rgba(239, 68, 68, ${opacity})`;
}

/* ---------- Heatmap table ---------- */

function HeatmapTable({
  labels,
  matrix,
  normalized,
}: {
  labels: string[];
  matrix: number[][];
  normalized: boolean;
}) {
  if (labels.length === 0 || matrix.length === 0) return null;

  // Find max value for color scaling
  const allValues = matrix.flat();
  const maxVal = Math.max(...allValues, 0);

  return (
    <div
      style={{
        overflowX: "auto",
        border: "1px solid var(--border-default)",
        borderRadius: 6,
        marginBottom: 12,
      }}
    >
      {/* Axis label */}
      <div
        style={{
          fontSize: 9,
          fontFamily: "'Inter', sans-serif",
          color: "var(--text-muted)",
          padding: "4px 8px",
          textAlign: "center",
          fontWeight: 600,
          letterSpacing: "0.05em",
          textTransform: "uppercase",
        }}
      >
        Predicted
      </div>
      <div style={{ display: "flex" }}>
        {/* Vertical axis label */}
        <div
          style={{
            writingMode: "vertical-rl",
            transform: "rotate(180deg)",
            fontSize: 9,
            fontFamily: "'Inter', sans-serif",
            color: "var(--text-muted)",
            fontWeight: 600,
            letterSpacing: "0.05em",
            textTransform: "uppercase",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "0 2px",
            flexShrink: 0,
          }}
        >
          Actual
        </div>
        <table
          style={{
            borderCollapse: "collapse",
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10,
            flex: 1,
          }}
        >
          <thead>
            <tr>
              <th
                style={{
                  padding: "4px 6px",
                  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                  borderBottom: "1px solid var(--border-default)",
                  borderRight: "1px solid var(--border-default)",
                  position: "sticky",
                  left: 0,
                  zIndex: 1,
                }}
              />
              {labels.map((l) => (
                <th
                  key={l}
                  style={{
                    padding: "4px 6px",
                    fontWeight: 600,
                    color: "var(--text-secondary)",
                    backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                    borderBottom: "1px solid var(--border-default)",
                    whiteSpace: "nowrap",
                    maxWidth: 70,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    textAlign: "center",
                  }}
                  title={l}
                >
                  {l}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {matrix.map((row, i) => (
              <tr key={i}>
                <td
                  style={{
                    padding: "4px 6px",
                    fontWeight: 600,
                    color: "var(--text-secondary)",
                    borderRight: "1px solid var(--border-default)",
                    whiteSpace: "nowrap",
                    maxWidth: 80,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    position: "sticky",
                    left: 0,
                    backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                    zIndex: 1,
                  }}
                  title={labels[i]}
                >
                  {labels[i]}
                </td>
                {row.map((val, j) => {
                  const isDiag = i === j;
                  const bg = heatmapCellColor(val, maxVal, isDiag);
                  const highIntensity = maxVal > 0 && val / maxVal > 0.65;
                  return (
                    <td
                      key={j}
                      style={{
                        padding: "6px 8px",
                        textAlign: "center",
                        backgroundColor: bg,
                        color: highIntensity
                          ? "#ffffff"
                          : "var(--text-primary)",
                        borderBottom: "1px solid var(--border-default)",
                        borderRight: "1px solid var(--border-default)",
                        fontWeight: isDiag ? 700 : 400,
                        minWidth: 44,
                      }}
                      title={`True: ${labels[i]}, Pred: ${labels[j]} = ${normalized ? `${(val * 100).toFixed(1)}%` : val}`}
                    >
                      {normalized ? `${(val * 100).toFixed(1)}%` : val}
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

/* ---------- Per-class metrics table ---------- */

const cellStyle: React.CSSProperties = {
  padding: "4px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

function PerClassTable({
  perClass,
}: {
  perClass: {
    label: string;
    precision: number;
    recall: number;
    f1: number;
    support: number;
  }[];
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
            {["Class", "Precision", "Recall", "F1", "Support"].map((h) => (
              <th
                key={h}
                style={{
                  padding: "6px 8px",
                  textAlign: "left",
                  fontFamily: "'Inter', sans-serif",
                  fontSize: 10,
                  fontWeight: 600,
                  color: "var(--text-secondary)",
                  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                  borderBottom: "1px solid var(--border-default)",
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {perClass.map((pc, i) => (
            <tr
              key={pc.label}
              style={{
                backgroundColor:
                  i % 2 === 1
                    ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                    : "transparent",
              }}
            >
              <td style={{ ...cellStyle, fontWeight: 600 }}>{pc.label}</td>
              <td style={cellStyle}>
                <MetricWithBar value={pc.precision} />
              </td>
              <td style={cellStyle}>
                <MetricWithBar value={pc.recall} />
              </td>
              <td style={cellStyle}>
                <MetricWithBar value={pc.f1} />
              </td>
              <td style={cellStyle}>{pc.support.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

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
      <span style={{ width: 38, textAlign: "right" }}>{(pct).toFixed(1)}%</span>
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
