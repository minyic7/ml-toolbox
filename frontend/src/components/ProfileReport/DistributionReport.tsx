import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { ColumnTable, type Header } from "./ColumnTable";
import { WarningList } from "./WarningList";
import { formatStat } from "./formatStat";

interface DistributionReportProps {
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

/** Flatten nested stats into top-level keys for table display. */
function flattenColumn(col: Record<string, unknown>): Record<string, unknown> {
  const stats = col.stats as Record<string, unknown> | undefined;
  if (!stats) return col;
  return {
    ...col,
    // Only spread stats fields that aren't already top-level
    ...Object.fromEntries(
      Object.entries(stats).filter(([k]) => !(k in col)),
    ),
  };
}

export function DistributionReport({ data, analysis }: DistributionReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const rawColumns = (data.columns ?? []) as Record<string, unknown>[];
  const columns = rawColumns.map(flattenColumn);
  const target = data.target as Record<string, unknown> | undefined;
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];
  const aiWarnings = analysis?.warnings ?? [];

  const numericCols = columns.filter((c) => c.dtype !== "object" && c.dtype !== "category");
  const catCols = columns.filter((c) => c.dtype === "object" || c.dtype === "category");

  const summaryItems = [
    { label: "Total Rows", value: (summary?.total_rows as number) ?? 0 },
    { label: "Numeric Cols", value: numericCols.length },
    { label: "Categorical Cols", value: catCols.length },
  ];

  const numStatRender = (v: unknown) => formatStat(v, 2);

  const numericHeaders: Header[] = [
    { key: "name", label: "Column" },
    { key: "dtype", label: "Type" },
    { key: "count", label: "Count", render: (v) => formatStat(v) },
    { key: "mean", label: "Mean", render: numStatRender },
    { key: "median", label: "Median", render: numStatRender },
    { key: "std", label: "Std", render: numStatRender },
    { key: "skewness", label: "Skew", render: numStatRender },
    { key: "min", label: "Min", render: numStatRender },
    { key: "max", label: "Max", render: numStatRender },
    { key: "q25", label: "Q25", render: numStatRender },
    { key: "q75", label: "Q75", render: numStatRender },
  ];

  const catHeaders: Header[] = [
    { key: "name", label: "Column" },
    { key: "dtype", label: "Type" },
    { key: "cardinality", label: "Unique" },
    { key: "top_value", label: "Top Value" },
    { key: "top_freq", label: "Top Freq" },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {numericCols.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Numeric Columns</div>
          <ColumnTable columns={numericCols} headers={numericHeaders} />
          <HistogramBars columns={numericCols} />
        </>
      )}

      {catCols.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Categorical Columns</div>
          <ColumnTable columns={catCols} headers={catHeaders} />
          <CategoricalBars columns={catCols} />
        </>
      )}

      {target && <TargetSection target={target} />}

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

/* ---------- Histogram bars (48px height, x-axis labels) ---------- */

function HistogramBars({
  columns,
}: {
  columns: Record<string, unknown>[];
}) {
  const withHistograms = columns.filter(
    (c) => Array.isArray(c.histogram_counts) && (c.histogram_counts as number[]).length > 0,
  );
  if (withHistograms.length === 0) return null;

  return (
    <div style={{ marginTop: 8, marginBottom: 12 }}>
      <div
        style={{
          fontSize: 10,
          color: "var(--text-muted)",
          fontFamily: "'Inter', sans-serif",
          marginBottom: 6,
        }}
      >
        Distributions (bin counts)
      </div>
      {withHistograms.map((col) => {
        const counts = col.histogram_counts as number[];
        const edges = Array.isArray(col.histogram_edges)
          ? (col.histogram_edges as number[])
          : null;
        const max = Math.max(...counts);
        return (
          <div key={String(col.name)} style={{ marginBottom: 10 }}>
            <div
              style={{
                fontSize: 10,
                fontFamily: "'JetBrains Mono', monospace",
                color: "var(--text-secondary)",
                marginBottom: 2,
              }}
            >
              {String(col.name)}
            </div>
            <div style={{ display: "flex", gap: 1, alignItems: "flex-end", height: 48 }}>
              {counts.map((c, i) => (
                <div
                  key={i}
                  title={`${c.toLocaleString()} rows${edges ? ` [${formatStat(edges[i], 1)}\u2013${formatStat(edges[i + 1], 1)}]` : ""}`}
                  style={{
                    flex: 1,
                    height: max > 0 ? `${(c / max) * 100}%` : "0%",
                    backgroundColor: "var(--accent-primary, #3b82f6)",
                    borderRadius: "1px 1px 0 0",
                    minHeight: c > 0 ? 2 : 0,
                    opacity: 0.7,
                  }}
                />
              ))}
            </div>
            {/* X-axis labels (first, mid, last bin edges) */}
            {edges && edges.length >= 2 && (
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  fontSize: 9,
                  fontFamily: "'JetBrains Mono', monospace",
                  color: "var(--text-muted)",
                  marginTop: 1,
                }}
              >
                <span>{formatStat(edges[0], 1)}</span>
                {edges.length > 2 && (
                  <span>{formatStat(edges[Math.floor(edges.length / 2)], 1)}</span>
                )}
                <span>{formatStat(edges[edges.length - 1], 1)}</span>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

/* ---------- Categorical value bars ---------- */

function CategoricalBars({
  columns,
}: {
  columns: Record<string, unknown>[];
}) {
  const withValues = columns.filter((c) => {
    const vd = c.value_distribution as Record<string, number> | undefined;
    return vd && Object.keys(vd).length > 0;
  });
  if (withValues.length === 0) return null;

  return (
    <div style={{ marginTop: 8, marginBottom: 12 }}>
      <div
        style={{
          fontSize: 10,
          color: "var(--text-muted)",
          fontFamily: "'Inter', sans-serif",
          marginBottom: 6,
        }}
      >
        Top Values
      </div>
      {withValues.map((col) => {
        const dist = col.value_distribution as Record<string, number>;
        const entries = Object.entries(dist)
          .sort(([, a], [, b]) => b - a)
          .slice(0, 8);
        const total = Object.values(dist).reduce((s, v) => s + v, 0);
        const maxCount = entries.length > 0 ? entries[0][1] : 1;

        return (
          <div key={String(col.name)} style={{ marginBottom: 10 }}>
            <div
              style={{
                fontSize: 10,
                fontFamily: "'JetBrains Mono', monospace",
                color: "var(--text-secondary)",
                marginBottom: 4,
              }}
            >
              {String(col.name)}
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              {entries.map(([label, count]) => {
                const pct = total > 0 ? (count / total) * 100 : 0;
                const widthPct = maxCount > 0 ? (count / maxCount) * 100 : 0;
                return (
                  <div
                    key={label}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      fontSize: 10,
                      fontFamily: "'JetBrains Mono', monospace",
                    }}
                  >
                    <span
                      style={{
                        width: 80,
                        color: "var(--text-secondary)",
                        flexShrink: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                      title={label}
                    >
                      {label}
                    </span>
                    <div
                      style={{
                        flex: 1,
                        height: 10,
                        backgroundColor: "var(--border-default)",
                        borderRadius: 3,
                        overflow: "hidden",
                      }}
                    >
                      <div
                        style={{
                          width: `${widthPct}%`,
                          height: "100%",
                          backgroundColor: "var(--accent-primary, #3b82f6)",
                          borderRadius: 3,
                          opacity: 0.7,
                        }}
                      />
                    </div>
                    <span
                      style={{
                        width: 90,
                        textAlign: "right",
                        color: "var(--text-primary)",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {count.toLocaleString()} ({pct.toFixed(1)}%)
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ---------- Target section ---------- */

function TargetSection({ target }: { target: Record<string, unknown> }) {
  const classBalance = target.class_balance as
    | Record<string, number>
    | undefined;
  if (!classBalance) return null;

  const entries = Object.entries(classBalance);
  const total = entries.reduce((s, [, v]) => s + v, 0);

  return (
    <div style={{ marginTop: 16 }}>
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          fontFamily: "'Inter', sans-serif",
          textTransform: "uppercase",
          color: "var(--text-secondary)",
          borderBottom: "1px solid var(--border-default)",
          paddingBottom: 6,
          marginBottom: 8,
        }}
      >
        Target: {String(target.name ?? "")}
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {entries.map(([label, count]) => {
          const pct = total > 0 ? (count / total) * 100 : 0;
          return (
            <div
              key={label}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                fontSize: 10,
                fontFamily: "'JetBrains Mono', monospace",
              }}
            >
              <span
                style={{
                  width: 80,
                  color: "var(--text-secondary)",
                  flexShrink: 0,
                }}
              >
                {label}
              </span>
              <div
                style={{
                  flex: 1,
                  height: 12,
                  backgroundColor: "var(--border-default)",
                  borderRadius: 3,
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    width: `${pct}%`,
                    height: "100%",
                    backgroundColor: "var(--accent-primary, #3b82f6)",
                    borderRadius: 3,
                  }}
                />
              </div>
              <span
                style={{
                  width: 60,
                  textAlign: "right",
                  color: "var(--text-primary)",
                }}
              >
                {pct.toFixed(1)}%
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
