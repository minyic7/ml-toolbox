import type { ReactNode } from "react";
import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { ColumnTable, type Header } from "./ColumnTable";
import { WarningList } from "./WarningList";
import { formatStat } from "./formatStat";

interface MissingReportProps {
  data: Record<string, unknown>;
  analysis?: CcAnalysis | null;
}

const SEVERITY_BADGE_COLORS: Record<string, { bg: string; text: string }> = {
  none: { bg: "#dcfce7", text: "#166534" },
  low: { bg: "#fef9c3", text: "#854d0e" },
  medium: { bg: "#fed7aa", text: "#9a3412" },
  high: { bg: "#fecaca", text: "#991b1b" },
};

function renderSeverityBadge(value: unknown): ReactNode {
  const severity = String(value ?? "none").toLowerCase();
  const colors = SEVERITY_BADGE_COLORS[severity] ?? SEVERITY_BADGE_COLORS.none;
  return (
    <span
      style={{
        padding: "2px 8px",
        borderRadius: 9999,
        backgroundColor: colors.bg,
        color: colors.text,
        fontSize: 10,
        fontWeight: 700,
        textTransform: "uppercase",
      }}
    >
      {severity}
    </span>
  );
}

export function MissingReport({ data, analysis }: MissingReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const columns = (data.columns ?? []) as Record<string, unknown>[];
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];
  const aiWarnings = analysis?.warnings ?? [];

  const totalRows = (summary?.total_rows as number) ?? 0;
  const totalMissing = (summary?.total_missing as number) ?? 0;
  const completeRows = (summary?.complete_rows_pct as number) ?? 0;
  const colsWithMissing = columns.filter(
    (c) => (c.missing_count as number) > 0,
  ).length;

  const summaryItems = [
    { label: "Total Rows", value: totalRows },
    { label: "Total Missing", value: totalMissing },
    { label: "Complete Rows %", value: `${(completeRows * 100).toFixed(1)}%` },
    { label: "Cols w/ Missing", value: colsWithMissing },
  ];

  const tableHeaders: Header[] = [
    { key: "name", label: "Column" },
    { key: "missing_count", label: "Missing", render: (v) => formatStat(v) },
    {
      key: "missing_pct",
      label: "Missing %",
      render: (v) => {
        if (v == null) return "\u2014";
        if (typeof v === "number") return `${(v * 100).toFixed(1)}%`;
        return String(v);
      },
    },
    {
      key: "severity",
      label: "Severity",
      render: renderSeverityBadge,
    },
    { key: "present_count", label: "Present", render: (v) => formatStat(v) },
  ];

  // Zero-missing success message
  const hasNoMissing = colsWithMissing === 0 && totalMissing === 0;

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {hasNoMissing ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "12px 16px",
            borderRadius: 6,
            backgroundColor: "#dcfce7",
            color: "#166534",
            fontSize: 12,
            fontFamily: "'Inter', sans-serif",
            fontWeight: 500,
            marginBottom: 12,
          }}
        >
          <span style={{ fontSize: 16 }}>&#10003;</span>
          No missing values — dataset is complete
        </div>
      ) : (
        <>
          <ColumnTable columns={columns} headers={tableHeaders} />
          <MissingBars columns={columns} totalRows={totalRows} />
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

function MissingBars({
  columns,
  totalRows,
}: {
  columns: Record<string, unknown>[];
  totalRows: number;
}) {
  const withMissing = columns.filter((c) => (c.missing_count as number) > 0);
  if (withMissing.length === 0) return null;

  return (
    <div style={{ marginTop: 12 }}>
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
        Missing Proportions
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {withMissing.map((col) => {
          const count = (col.missing_count as number) ?? 0;
          const pct = totalRows > 0 ? (count / totalRows) * 100 : 0;
          const severity = String(col.severity ?? "none").toLowerCase();
          const colors = SEVERITY_BADGE_COLORS[severity] ?? SEVERITY_BADGE_COLORS.none;

          return (
            <div
              key={String(col.name)}
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
                  width: 100,
                  color: "var(--text-secondary)",
                  flexShrink: 0,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {String(col.name)}
              </span>
              <div
                style={{
                  flex: 1,
                  height: 16,
                  backgroundColor: "var(--border-default)",
                  borderRadius: 3,
                  overflow: "hidden",
                  position: "relative",
                }}
              >
                <div
                  style={{
                    width: `${pct}%`,
                    height: "100%",
                    backgroundColor: colors.text,
                    borderRadius: 3,
                    opacity: 0.7,
                  }}
                />
              </div>
              <span
                style={{
                  width: 50,
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
