import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";

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
  tree_importance: "tree",
  coefficient_magnitude: "coeff magnitude",
};

const MAX_VISIBLE = 25;
const BAR_COLOR = "rgba(180, 160, 130, 0.7)";

interface Feature {
  name: string;
  importance: number;
  raw_importance: number;
}

interface Warning {
  type: string;
  severity?: string;
  column?: string;
  message: string;
}

export function FeatureImportanceReport({ data, analysis }: FeatureImportanceReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const features = (data.features ?? []) as Feature[];
  const method = (data.method ?? "") as string;
  const warnings = (data.warnings ?? []) as Warning[];
  const aiWarnings = analysis?.warnings ?? [];

  const featureCount = (summary?.feature_count as number) ?? features.length;
  const modelType = (summary?.model_type as string) ?? "—";
  const methodLabel = METHOD_LABELS[method] ?? method;
  const topFeature = (summary?.top_feature as string) ?? "—";
  const topImportance = (summary?.top_importance as number) ?? 0;

  const summaryItems: { label: string; value: string }[] = [
    {
      label: `features · ${methodLabel || modelType.toLowerCase().replace("classifier", "").replace("regressor", "").trim()}`,
      value: String(featureCount),
    },
    {
      label: `top feature · ${(topImportance * 100).toFixed(1)}%`,
      value: topFeature,
    },
  ];

  const allWarnings = aiWarnings.length > 0
    ? aiWarnings.map((w) => ({ type: w.type, severity: "medium", column: w.column ?? undefined, message: w.message }))
    : warnings;

  // Truncate features
  const visible = features.slice(0, MAX_VISIBLE);
  const hidden = features.slice(MAX_VISIBLE);
  const hiddenShare = hidden.reduce((sum, f) => sum + f.importance, 0);

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {features.length > 0 && (
        <>
          <div style={SECTION_HEADER}>
            Feature Importances · {METHOD_LABELS[method] ?? method}
          </div>
          <FeatureBarChart features={visible} />
          {hidden.length > 0 && (
            <div style={{
              fontSize: 10,
              fontFamily: "'Inter', sans-serif",
              color: "var(--text-muted)",
              textAlign: "center",
              padding: "6px 0 12px",
            }}>
              … and {hidden.length} more feature{hidden.length > 1 ? "s" : ""} (total {(hiddenShare * 100).toFixed(1)}%)
            </div>
          )}
        </>
      )}

      {allWarnings.length > 0 && (
        <SeverityWarnings warnings={allWarnings} />
      )}
    </div>
  );
}

// ── Bar Chart ────────────────────────────────────────────────────

function FeatureBarChart({ features }: { features: Feature[] }) {
  const maxImportance = features.length > 0 ? features[0].importance : 1;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2, marginBottom: 4 }}>
      {features.map((f) => {
        const barWidth = maxImportance > 0 ? (f.importance / maxImportance) * 100 : 0;
        const pct = (f.importance * 100).toFixed(1);

        return (
          <div
            key={f.name}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              height: 20,
            }}
            title={`${f.name}: ${pct}% (raw: ${f.raw_importance})`}
          >
            <span style={{
              width: 90,
              fontSize: 10,
              fontFamily: "'JetBrains Mono', monospace",
              color: "var(--text-primary)",
              textAlign: "right",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
              flexShrink: 0,
            }}>
              {f.name}
            </span>
            <div style={{ flex: 1, height: 14 }}>
              <div style={{
                width: `${barWidth}%`,
                height: "100%",
                backgroundColor: BAR_COLOR,
                borderRadius: 2,
                minWidth: barWidth > 0 ? 2 : 0,
              }} />
            </div>
            <span style={{
              width: 40,
              fontSize: 10,
              fontFamily: "'JetBrains Mono', monospace",
              color: "var(--text-muted)",
              textAlign: "right",
              flexShrink: 0,
            }}>
              {pct}%
            </span>
          </div>
        );
      })}
    </div>
  );
}

// ── Severity-based Warnings ──────────────────────────────────────

function SeverityWarnings({ warnings }: { warnings: Warning[] }) {
  const order: Record<string, number> = { high: 0, medium: 1, low: 2 };
  const sorted = [...warnings].sort(
    (a, b) => (order[a.severity ?? "medium"] ?? 1) - (order[b.severity ?? "medium"] ?? 1),
  );

  return (
    <>
      <div style={SECTION_HEADER}>Warnings</div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {sorted.map((w, i) => {
          const sev = w.severity ?? "medium";
          const borderColor = sev === "high" ? "#dc2626"
            : sev === "medium" ? "#d97706"
            : "#9ca3af";

          return (
            <div
              key={i}
              style={{
                padding: "8px 12px",
                borderRadius: 6,
                borderLeft: `3px solid ${borderColor}`,
                background: "var(--ghost-hover-bg, rgba(0,0,0,0.02))",
                fontSize: 11,
                fontFamily: "'Inter', sans-serif",
              }}
            >
              <div style={{
                fontSize: 10,
                fontWeight: 700,
                color: borderColor,
                textTransform: "uppercase",
                letterSpacing: "0.03em",
                marginBottom: 2,
              }}>
                {sev}{w.column ? ` · ${w.column}` : ""}
              </div>
              <div style={{ color: "var(--text-secondary)", lineHeight: 1.4 }}>
                {w.message}
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
