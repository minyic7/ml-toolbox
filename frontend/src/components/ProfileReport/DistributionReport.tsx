import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { ColumnTable } from "./ColumnTable";
import { WarningList } from "./WarningList";

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

export function DistributionReport({ data, analysis }: DistributionReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const columns = (data.columns ?? []) as Record<string, unknown>[];
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

  const numericHeaders = [
    { key: "name", label: "Column" },
    { key: "dtype", label: "Type" },
    { key: "mean", label: "Mean" },
    { key: "median", label: "Median" },
    { key: "std", label: "Std" },
    { key: "skewness", label: "Skew" },
    { key: "cardinality", label: "Unique" },
  ];

  const catHeaders = [
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
        </>
      )}

      {target && <TargetSection target={target} />}

      {/* Single warnings section: AI if available, else hardcoded */}
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
        const max = Math.max(...counts);
        return (
          <div key={String(col.name)} style={{ marginBottom: 6 }}>
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
            <div style={{ display: "flex", gap: 1, alignItems: "flex-end", height: 24 }}>
              {counts.map((c, i) => (
                <div
                  key={i}
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
          </div>
        );
      })}
    </div>
  );
}

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
