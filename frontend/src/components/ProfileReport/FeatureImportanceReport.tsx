import { useMemo } from "react";
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

/** Stable color palette for feature groups. */
const GROUP_COLORS = [
  "#3b82f6", // blue
  "#22c55e", // green
  "#f59e0b", // amber
  "#8b5cf6", // purple
  "#6b7280", // gray
  "#ef4444", // red
  "#06b6d4", // cyan
  "#ec4899", // pink
  "#14b8a6", // teal
  "#f97316", // orange
];

interface Feature {
  name: string;
  importance: number;
  raw_importance: number;
  group?: string;
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
  const groupShares = data.group_shares as Record<string, number> | undefined;
  const warnings = (data.warnings ?? []) as Warning[];
  const aiWarnings = analysis?.warnings ?? [];

  // Assign colors to groups
  const groupColorMap = useMemo(() => {
    const groups = new Set(features.map((f) => f.group ?? f.name));
    const map: Record<string, string> = {};
    let i = 0;
    for (const g of groups) {
      map[g] = GROUP_COLORS[i % GROUP_COLORS.length];
      i++;
    }
    return map;
  }, [features]);

  // Summary cards
  const featureCount = (summary?.feature_count as number) ?? features.length;
  const modelType = (summary?.model_type as string) ?? "—";
  const methodLabel = METHOD_LABELS[method] ?? method;
  const topFeature = (summary?.top_feature as string) ?? "—";
  const topImportance = (summary?.top_importance as number) ?? 0;
  const topGroup = (summary?.top_group as string) ?? "";
  const topGroupShare = (summary?.top_group_share as number) ?? 0;

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
  if (topGroup && topGroupShare > 0.1) {
    summaryItems.push({
      label: `${topGroup}* group share`,
      value: `~${Math.round(topGroupShare * 100)}%`,
    });
  }

  // Merge warnings
  const allWarnings = aiWarnings.length > 0
    ? aiWarnings.map((w) => ({ type: w.type, severity: "medium", column: w.column ?? undefined, message: w.message }))
    : warnings;

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {features.length > 0 && (
        <>
          <div style={{ ...SECTION_HEADER, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>Feature Importances · {METHOD_LABELS[method] ?? method}</span>
          </div>
          <GroupLegend groupColorMap={groupColorMap} groupShares={groupShares} />
          <FeatureBarChart features={features} groupColorMap={groupColorMap} />
        </>
      )}

      {allWarnings.length > 0 && (
        <SeverityWarnings warnings={allWarnings} />
      )}
    </div>
  );
}

// ── Group Legend ──────────────────────────────────────────────────

function GroupLegend({
  groupColorMap,
  groupShares,
}: {
  groupColorMap: Record<string, string>;
  groupShares?: Record<string, number>;
}) {
  const groups = Object.entries(groupColorMap);
  if (groups.length <= 1) return null;

  return (
    <div style={{
      display: "flex",
      flexWrap: "wrap",
      gap: "6px 14px",
      marginBottom: 10,
      fontSize: 10,
      fontFamily: "'Inter', sans-serif",
      color: "var(--text-secondary)",
    }}>
      {groups.map(([group, color]) => (
        <span key={group} style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <span style={{
            width: 10,
            height: 10,
            borderRadius: 2,
            backgroundColor: color,
            display: "inline-block",
            flexShrink: 0,
          }} />
          {group.toLowerCase()}
          {groupShares?.[group] != null && (
            <span style={{ color: "var(--text-muted)" }}>
              {(groupShares[group] * 100).toFixed(0)}%
            </span>
          )}
        </span>
      ))}
    </div>
  );
}

// ── Bar Chart ────────────────────────────────────────────────────

function FeatureBarChart({
  features,
  groupColorMap,
}: {
  features: Feature[];
  groupColorMap: Record<string, string>;
}) {
  const maxImportance = features.length > 0 ? features[0].importance : 1;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2, marginBottom: 12 }}>
      {features.map((f) => {
        const barWidth = maxImportance > 0 ? (f.importance / maxImportance) * 100 : 0;
        const isLow = f.importance < 0.01;
        const color = groupColorMap[f.group ?? f.name] ?? "#6b7280";
        const pct = (f.importance * 100).toFixed(1);

        return (
          <div
            key={f.name}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              opacity: isLow ? 0.45 : 1,
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
            <div style={{ flex: 1, height: 14, position: "relative" }}>
              <div style={{
                width: `${barWidth}%`,
                height: "100%",
                backgroundColor: color,
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
  // Sort: high > medium > low
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
