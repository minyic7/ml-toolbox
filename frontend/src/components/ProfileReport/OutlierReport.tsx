import { Fragment, useState, useCallback } from "react";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface OutlierReportProps {
  data: Record<string, unknown>;
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

export function OutlierReport({ data }: OutlierReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const columns = (data.columns ?? []) as Record<string, unknown>[];
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];

  const colsWithOutliers = columns.filter(
    (c) => (c.outlier_count as number) > 0,
  ).length;
  const totalOutlierCells = columns.reduce(
    (s, c) => s + ((c.outlier_count as number) ?? 0),
    0,
  );

  const summaryItems = [
    { label: "Total Rows", value: (summary?.total_rows as number) ?? 0 },
    { label: "Cols w/ Outliers", value: colsWithOutliers },
    { label: "Total Outlier Cells", value: totalOutlierCells },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      <div style={SECTION_HEADER}>Outlier Details</div>
      <OutlierTable columns={columns} />

      <WarningList warnings={warnings} />
    </div>
  );
}

function OutlierTable({
  columns,
}: {
  columns: Record<string, unknown>[];
}) {
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  const toggleExpand = useCallback((idx: number) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  }, []);

  const headers = ["Column", "Outliers", "Outlier %", "Fence Range", "Max"];

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
            {headers.map((h) => (
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
          {columns.map((col, i) => {
            const hasSamples =
              Array.isArray(col.sample_values) &&
              (col.sample_values as unknown[]).length > 0;
            const isExpanded = expanded.has(i);
            const fenceLower = col.fence_lower as number | undefined;
            const fenceUpper = col.fence_upper as number | undefined;
            const fenceRange =
              fenceLower != null && fenceUpper != null
                ? `[${formatNum(fenceLower)}, ${formatNum(fenceUpper)}]`
                : "\u2014";

            return (
              <Fragment key={i}>
                <tr
                  style={{
                    backgroundColor:
                      i % 2 === 1
                        ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                        : "transparent",
                    cursor: hasSamples ? "pointer" : "default",
                  }}
                  onClick={() => hasSamples && toggleExpand(i)}
                >
                  <td style={tdStyle}>
                    {hasSamples && (
                      <span style={{ marginRight: 4, fontSize: 8 }}>
                        {isExpanded ? "\u25bc" : "\u25b6"}
                      </span>
                    )}
                    {String(col.name)}
                  </td>
                  <td style={tdStyle}>
                    {((col.outlier_count as number) ?? 0).toLocaleString()}
                  </td>
                  <td style={tdStyle}>
                    {col.outlier_pct != null
                      ? `${(col.outlier_pct as number).toFixed(2)}%`
                      : "\u2014"}
                  </td>
                  <td style={tdStyle}>{fenceRange}</td>
                  <td style={tdStyle}>
                    {col.max != null ? formatNum(col.max as number) : "\u2014"}
                  </td>
                </tr>
                {isExpanded && hasSamples && (
                  <tr>
                    <td
                      colSpan={5}
                      style={{
                        padding: "4px 8px 8px 24px",
                        fontSize: 10,
                        color: "var(--text-muted)",
                        borderBottom: "1px solid var(--border-default)",
                        fontFamily: "'JetBrains Mono', monospace",
                      }}
                    >
                      Sample outliers: [
                      {(col.sample_values as unknown[])
                        .map((v) => String(v))
                        .join(", ")}
                      ]
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const tdStyle: React.CSSProperties = {
  padding: "4px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

function formatNum(n: number): string {
  if (Number.isInteger(n)) return n.toLocaleString();
  return n.toFixed(4);
}
