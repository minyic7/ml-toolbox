import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface RocPrReportProps {
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

/* ---------- SVG Curve Chart ---------- */

const CHART_W = 300;
const CHART_H = 240;
const PAD = { top: 16, right: 16, bottom: 32, left: 40 };
const PLOT_W = CHART_W - PAD.left - PAD.right;
const PLOT_H = CHART_H - PAD.top - PAD.bottom;

function CurveChart({
  xValues,
  yValues,
  xLabel,
  yLabel,
  title,
  annotation,
  diagonalGuide,
  baselineY,
  color,
}: {
  xValues: number[];
  yValues: number[];
  xLabel: string;
  yLabel: string;
  title: string;
  annotation: string;
  diagonalGuide?: boolean;
  baselineY?: number;
  color: string;
}) {
  if (xValues.length < 2) return null;

  const toX = (v: number) => PAD.left + v * PLOT_W;
  const toY = (v: number) => PAD.top + (1 - v) * PLOT_H;

  const pathD = xValues
    .map((x, i) => `${i === 0 ? "M" : "L"}${toX(x).toFixed(1)},${toY(yValues[i]).toFixed(1)}`)
    .join(" ");

  const ticks = [0, 0.25, 0.5, 0.75, 1.0];

  return (
    <div
      style={{
        border: "1px solid var(--border-default)",
        borderRadius: 6,
        padding: 8,
        marginBottom: 12,
        flex: "1 1 280px",
        minWidth: 260,
      }}
    >
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          fontFamily: "'Inter', sans-serif",
          color: "var(--text-primary)",
          marginBottom: 4,
          textAlign: "center",
        }}
      >
        {title}
      </div>
      <svg
        viewBox={`0 0 ${CHART_W} ${CHART_H}`}
        style={{ width: "100%", maxWidth: CHART_W, display: "block", margin: "0 auto" }}
      >
        {/* Grid lines */}
        {ticks.map((t) => (
          <g key={t}>
            <line
              x1={toX(t)}
              y1={PAD.top}
              x2={toX(t)}
              y2={PAD.top + PLOT_H}
              stroke="var(--border-default)"
              strokeWidth={0.5}
              strokeDasharray={t > 0 && t < 1 ? "2,2" : undefined}
            />
            <line
              x1={PAD.left}
              y1={toY(t)}
              x2={PAD.left + PLOT_W}
              y2={toY(t)}
              stroke="var(--border-default)"
              strokeWidth={0.5}
              strokeDasharray={t > 0 && t < 1 ? "2,2" : undefined}
            />
            {/* X axis labels */}
            <text
              x={toX(t)}
              y={CHART_H - PAD.bottom + 14}
              textAnchor="middle"
              fontSize={8}
              fontFamily="'JetBrains Mono', monospace"
              fill="var(--text-muted)"
            >
              {t.toFixed(1)}
            </text>
            {/* Y axis labels */}
            <text
              x={PAD.left - 6}
              y={toY(t) + 3}
              textAnchor="end"
              fontSize={8}
              fontFamily="'JetBrains Mono', monospace"
              fill="var(--text-muted)"
            >
              {t.toFixed(1)}
            </text>
          </g>
        ))}

        {/* Diagonal guide (ROC random baseline) */}
        {diagonalGuide && (
          <line
            x1={toX(0)}
            y1={toY(0)}
            x2={toX(1)}
            y2={toY(1)}
            stroke="var(--text-muted)"
            strokeWidth={1}
            strokeDasharray="4,3"
            opacity={0.5}
          />
        )}

        {/* Horizontal baseline (PR random baseline) */}
        {baselineY !== undefined && (
          <line
            x1={toX(0)}
            y1={toY(baselineY)}
            x2={toX(1)}
            y2={toY(baselineY)}
            stroke="var(--text-muted)"
            strokeWidth={1}
            strokeDasharray="4,3"
            opacity={0.5}
          />
        )}

        {/* Curve */}
        <path d={pathD} fill="none" stroke={color} strokeWidth={2} />

        {/* Axis labels */}
        <text
          x={PAD.left + PLOT_W / 2}
          y={CHART_H - 2}
          textAnchor="middle"
          fontSize={9}
          fontFamily="'Inter', sans-serif"
          fill="var(--text-secondary)"
        >
          {xLabel}
        </text>
        <text
          x={10}
          y={PAD.top + PLOT_H / 2}
          textAnchor="middle"
          fontSize={9}
          fontFamily="'Inter', sans-serif"
          fill="var(--text-secondary)"
          transform={`rotate(-90, 10, ${PAD.top + PLOT_H / 2})`}
        >
          {yLabel}
        </text>

        {/* AUC annotation */}
        <text
          x={PAD.left + PLOT_W - 4}
          y={PAD.top + 12}
          textAnchor="end"
          fontSize={10}
          fontWeight={700}
          fontFamily="'JetBrains Mono', monospace"
          fill={color}
        >
          {annotation}
        </text>
      </svg>
    </div>
  );
}

/* ---------- Multi-class table ---------- */

const cellStyle: React.CSSProperties = {
  padding: "4px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

function PerClassTable({ perClass }: { perClass: Record<string, unknown>[] }) {
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
            {["Class", "AUC-ROC", "Avg Precision", "Prevalence"].map((h) => (
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
          {perClass.map((c, i) => (
            <tr
              key={i}
              style={{
                backgroundColor:
                  i % 2 === 1 ? "var(--output-row-hover, rgba(0,0,0,0.02))" : "transparent",
              }}
            >
              <td style={cellStyle}>{String(c.class ?? "")}</td>
              <td style={cellStyle}>{((c.roc_auc as number) ?? 0).toFixed(4)}</td>
              <td style={cellStyle}>{((c.average_precision as number) ?? 0).toFixed(4)}</td>
              <td style={cellStyle}>{(((c.prevalence as number) ?? 0) * 100).toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ---------- Multi-class curve colors ---------- */

const CLASS_COLORS = [
  "#2563eb", // blue
  "#dc2626", // red
  "#16a34a", // green
  "#d97706", // amber
  "#7c3aed", // violet
  "#0891b2", // cyan
  "#e11d48", // rose
  "#4f46e5", // indigo
];

/* ---------- Main component ---------- */

export function RocPrReport({ data, analysis }: RocPrReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const warnings = (data.warnings ?? []) as { type: string; column?: string; message: string }[];
  const aiWarnings = analysis?.warnings ?? [];
  const task = data.task as string;

  if (task === "binary") {
    const rocCurve = data.roc_curve as { fpr: number[]; tpr: number[] } | undefined;
    const prCurve = data.pr_curve as { recall: number[]; precision: number[] } | undefined;
    const rocAuc = (summary?.roc_auc as number) ?? 0;
    const ap = (summary?.average_precision as number) ?? 0;
    const prevalence = (summary?.prevalence as number) ?? 0;
    const nSamples = (summary?.n_samples as number) ?? 0;

    const summaryItems = [
      { label: "AUC-ROC", value: rocAuc },
      { label: "Avg Precision", value: ap },
      { label: "Prevalence", value: `${(prevalence * 100).toFixed(1)}%` },
      { label: "Samples", value: nSamples },
    ];

    return (
      <div style={{ padding: 12 }}>
        <SummaryCards items={summaryItems} />

        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {rocCurve && (
            <CurveChart
              xValues={rocCurve.fpr}
              yValues={rocCurve.tpr}
              xLabel="False Positive Rate"
              yLabel="True Positive Rate"
              title="ROC Curve"
              annotation={`AUC = ${rocAuc.toFixed(3)}`}
              diagonalGuide
              color="#2563eb"
            />
          )}
          {prCurve && (
            <CurveChart
              xValues={prCurve.recall}
              yValues={prCurve.precision}
              xLabel="Recall"
              yLabel="Precision"
              title="Precision-Recall Curve"
              annotation={`AP = ${ap.toFixed(3)}`}
              baselineY={prevalence}
              color="#dc2626"
            />
          )}
        </div>

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

  // Multi-class
  const perClass = (data.per_class ?? []) as Record<string, unknown>[];
  const macroAuc = (summary?.macro_roc_auc as number) ?? 0;
  const macroAp = (summary?.macro_average_precision as number) ?? 0;
  const nClasses = (summary?.n_classes as number) ?? 0;
  const nSamples = (summary?.n_samples as number) ?? 0;

  const summaryItems = [
    { label: "Macro AUC-ROC", value: macroAuc },
    { label: "Macro Avg Precision", value: macroAp },
    { label: "Classes", value: nClasses },
    { label: "Samples", value: nSamples },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      <div style={SECTION_HEADER}>Per-Class Summary</div>
      <PerClassTable perClass={perClass} />

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        {/* Multi-class ROC: overlay all class curves */}
        <MultiClassCurveChart
          perClass={perClass}
          curveKey="roc_curve"
          xKey="fpr"
          yKey="tpr"
          xLabel="False Positive Rate"
          yLabel="True Positive Rate"
          title="ROC Curves (One-vs-Rest)"
          metricKey="roc_auc"
          metricLabel="AUC"
          diagonalGuide
        />
        <MultiClassCurveChart
          perClass={perClass}
          curveKey="pr_curve"
          xKey="recall"
          yKey="precision"
          xLabel="Recall"
          yLabel="Precision"
          title="PR Curves (One-vs-Rest)"
          metricKey="average_precision"
          metricLabel="AP"
        />
      </div>

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

/* ---------- Multi-class overlay chart ---------- */

function MultiClassCurveChart({
  perClass,
  curveKey,
  xKey,
  yKey,
  xLabel,
  yLabel,
  title,
  metricKey,
  metricLabel,
  diagonalGuide,
}: {
  perClass: Record<string, unknown>[];
  curveKey: string;
  xKey: string;
  yKey: string;
  xLabel: string;
  yLabel: string;
  title: string;
  metricKey: string;
  metricLabel: string;
  diagonalGuide?: boolean;
}) {
  const toX = (v: number) => PAD.left + v * PLOT_W;
  const toY = (v: number) => PAD.top + (1 - v) * PLOT_H;
  const ticks = [0, 0.25, 0.5, 0.75, 1.0];

  return (
    <div
      style={{
        border: "1px solid var(--border-default)",
        borderRadius: 6,
        padding: 8,
        marginBottom: 12,
        flex: "1 1 280px",
        minWidth: 260,
      }}
    >
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          fontFamily: "'Inter', sans-serif",
          color: "var(--text-primary)",
          marginBottom: 4,
          textAlign: "center",
        }}
      >
        {title}
      </div>
      <svg
        viewBox={`0 0 ${CHART_W} ${CHART_H}`}
        style={{ width: "100%", maxWidth: CHART_W, display: "block", margin: "0 auto" }}
      >
        {/* Grid */}
        {ticks.map((t) => (
          <g key={t}>
            <line
              x1={toX(t)} y1={PAD.top} x2={toX(t)} y2={PAD.top + PLOT_H}
              stroke="var(--border-default)" strokeWidth={0.5}
              strokeDasharray={t > 0 && t < 1 ? "2,2" : undefined}
            />
            <line
              x1={PAD.left} y1={toY(t)} x2={PAD.left + PLOT_W} y2={toY(t)}
              stroke="var(--border-default)" strokeWidth={0.5}
              strokeDasharray={t > 0 && t < 1 ? "2,2" : undefined}
            />
            <text x={toX(t)} y={CHART_H - PAD.bottom + 14} textAnchor="middle"
              fontSize={8} fontFamily="'JetBrains Mono', monospace" fill="var(--text-muted)">
              {t.toFixed(1)}
            </text>
            <text x={PAD.left - 6} y={toY(t) + 3} textAnchor="end"
              fontSize={8} fontFamily="'JetBrains Mono', monospace" fill="var(--text-muted)">
              {t.toFixed(1)}
            </text>
          </g>
        ))}

        {diagonalGuide && (
          <line x1={toX(0)} y1={toY(0)} x2={toX(1)} y2={toY(1)}
            stroke="var(--text-muted)" strokeWidth={1} strokeDasharray="4,3" opacity={0.5} />
        )}

        {/* Per-class curves */}
        {perClass.map((cls, idx) => {
          const curve = cls[curveKey] as Record<string, number[]> | undefined;
          if (!curve) return null;
          const xs = curve[xKey];
          const ys = curve[yKey];
          if (!xs || !ys || xs.length < 2) return null;
          const color = CLASS_COLORS[idx % CLASS_COLORS.length];
          const d = xs
            .map((x, i) => `${i === 0 ? "M" : "L"}${toX(x).toFixed(1)},${toY(ys[i]).toFixed(1)}`)
            .join(" ");
          return <path key={idx} d={d} fill="none" stroke={color} strokeWidth={1.5} />;
        })}

        {/* Axis labels */}
        <text x={PAD.left + PLOT_W / 2} y={CHART_H - 2} textAnchor="middle"
          fontSize={9} fontFamily="'Inter', sans-serif" fill="var(--text-secondary)">
          {xLabel}
        </text>
        <text x={10} y={PAD.top + PLOT_H / 2} textAnchor="middle"
          fontSize={9} fontFamily="'Inter', sans-serif" fill="var(--text-secondary)"
          transform={`rotate(-90, 10, ${PAD.top + PLOT_H / 2})`}>
          {yLabel}
        </text>
      </svg>

      {/* Legend */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "center", marginTop: 4 }}>
        {perClass.map((cls, idx) => {
          const color = CLASS_COLORS[idx % CLASS_COLORS.length];
          const metric = (cls[metricKey] as number) ?? 0;
          return (
            <div key={idx} style={{ display: "flex", alignItems: "center", gap: 3, fontSize: 9,
              fontFamily: "'JetBrains Mono', monospace", color: "var(--text-secondary)" }}>
              <span style={{ display: "inline-block", width: 10, height: 3,
                backgroundColor: color, borderRadius: 1 }} />
              {String(cls.class ?? idx)} ({metricLabel}={metric.toFixed(3)})
            </div>
          );
        })}
      </div>
    </div>
  );
}
