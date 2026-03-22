import type { CcAnalysis } from "../../lib/types";
import { SummaryCards } from "./SummaryCards";
import { ColumnTable } from "./ColumnTable";
import { WarningList } from "./WarningList";

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

  const tableHeaders = [
    { key: "name", label: "Column" },
    { key: "missing_count", label: "Missing" },
    { key: "missing_pct", label: "Missing %" },
    { key: "severity", label: "Severity" },
    { key: "present_count", label: "Present" },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      <ColumnTable
        columns={columns.map((c) => ({
          ...c,
          missing_pct:
            typeof c.missing_pct === "number"
              ? `${(c.missing_pct * 100).toFixed(1)}%`
              : c.missing_pct,
        }))}
        headers={tableHeaders}
      />

      <MissingBars columns={columns} totalRows={totalRows} />

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
