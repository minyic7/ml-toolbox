import { Fragment, useState, useCallback } from "react";
import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface OutlierReportProps {
  data: Record<string, unknown>;
}

type Method = "iqr" | "zscore" | "both";
type ViewMode = "iqr" | "zscore" | "combined";

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
  const method = (data.method as string as Method) || "iqr";
  const params = data.params as Record<string, unknown> | undefined;
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
    { label: "Method", value: method.toUpperCase() },
    { label: "Total Rows", value: (summary?.total_rows as number) ?? 0 },
    { label: "Cols w/ Outliers", value: colsWithOutliers },
    { label: "Total Outlier Cells", value: totalOutlierCells },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {params && <ParamsBanner method={method} params={params} />}

      <div style={SECTION_HEADER}>Outlier Details</div>
      <OutlierTable columns={columns} method={method} />

      <WarningList warnings={warnings} />
    </div>
  );
}

/* ---------- Params banner ---------- */

function ParamsBanner({
  method,
  params,
}: {
  method: Method;
  params: Record<string, unknown>;
}) {
  const items: string[] = [];
  items.push(`Method: ${method.toUpperCase()}`);
  if (method !== "zscore" && params.iqr_multiplier != null)
    items.push(`IQR Multiplier: ${params.iqr_multiplier}`);
  if (method !== "iqr" && params.zscore_threshold != null)
    items.push(`Z-Score Threshold: ${params.zscore_threshold}`);

  return (
    <div
      style={{
        fontSize: 10,
        fontFamily: "'JetBrains Mono', monospace",
        color: "var(--text-secondary)",
        padding: "6px 10px",
        borderRadius: 6,
        border: "1px solid var(--border-default)",
        marginBottom: 12,
        display: "flex",
        gap: 16,
        flexWrap: "wrap",
      }}
    >
      {items.map((t) => (
        <span key={t}>{t}</span>
      ))}
    </div>
  );
}

/* ---------- View mode tab switcher ---------- */

function ViewTabs({
  view,
  onViewChange,
}: {
  view: ViewMode;
  onViewChange: (v: ViewMode) => void;
}) {
  const tabs: { key: ViewMode; label: string }[] = [
    { key: "combined", label: "Combined" },
    { key: "iqr", label: "IQR" },
    { key: "zscore", label: "Z-Score" },
  ];

  return (
    <div
      style={{
        display: "flex",
        gap: 2,
        marginBottom: 8,
        borderRadius: 6,
        border: "1px solid var(--border-default)",
        overflow: "hidden",
        width: "fit-content",
      }}
    >
      {tabs.map((t) => (
        <button
          key={t.key}
          onClick={() => onViewChange(t.key)}
          style={{
            padding: "4px 12px",
            fontSize: 10,
            fontFamily: "'Inter', sans-serif",
            fontWeight: view === t.key ? 600 : 400,
            border: "none",
            cursor: "pointer",
            backgroundColor:
              view === t.key
                ? "var(--output-thead-bg, #e8e8e8)"
                : "transparent",
            color:
              view === t.key
                ? "var(--text-primary)"
                : "var(--text-secondary)",
          }}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}

/* ---------- Outlier table ---------- */

function getHeaders(view: ViewMode): string[] {
  const common = ["Column", "Outliers", "Outlier %"];
  const iqrCols = ["Q1", "Q3", "IQR", "Lower Fence", "Upper Fence"];
  const zCols = ["Mean", "Std", "Z-max", "Threshold"];
  const tail = ["Max"];

  if (view === "iqr") return [...common, ...iqrCols, ...tail];
  if (view === "zscore") return [...common, ...zCols, ...tail];
  return [...common, ...iqrCols, ...zCols, ...tail];
}

function getEffectiveView(method: Method): ViewMode {
  if (method === "both") return "combined";
  return method;
}

function OutlierTable({
  columns,
  method,
}: {
  columns: Record<string, unknown>[];
  method: Method;
}) {
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [view, setView] = useState<ViewMode>(getEffectiveView(method));

  const toggleExpand = useCallback((idx: number) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  }, []);

  const headers = getHeaders(view);

  return (
    <div>
      {method === "both" && <ViewTabs view={view} onViewChange={setView} />}

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
                    whiteSpace: "nowrap",
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
                Array.isArray(col.outlier_values_sample) &&
                (col.outlier_values_sample as unknown[]).length > 0;
              const isExpanded = expanded.has(i);

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

                    {/* IQR columns */}
                    {(view === "iqr" || view === "combined") && (
                      <>
                        <td style={tdStyle}>{fmtOpt(col.q1)}</td>
                        <td style={tdStyle}>{fmtOpt(col.q3)}</td>
                        <td style={tdStyle}>{fmtOpt(col.iqr)}</td>
                        <td style={tdStyle}>{fmtOpt(col.lower_fence)}</td>
                        <td style={tdStyle}>{fmtOpt(col.upper_fence)}</td>
                      </>
                    )}

                    {/* Z-score columns */}
                    {(view === "zscore" || view === "combined") && (
                      <>
                        <td style={tdStyle}>{fmtOpt(col.mean)}</td>
                        <td style={tdStyle}>{fmtOpt(col.std)}</td>
                        <td style={tdStyle}>{fmtOpt(col.z_max)}</td>
                        <td style={tdStyle}>{fmtOpt(col.zscore_threshold)}</td>
                      </>
                    )}

                    <td style={tdStyle}>{fmtOpt(col.max_value)}</td>
                  </tr>
                  {isExpanded && hasSamples && (
                    <tr>
                      <td
                        colSpan={headers.length}
                        style={{
                          padding: "6px 8px 8px 24px",
                          fontSize: 10,
                          color: "var(--text-muted)",
                          borderBottom: "1px solid var(--border-default)",
                          fontFamily: "'JetBrains Mono', monospace",
                        }}
                      >
                        <ExpandedRowDetail col={col} view={view} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ---------- Expanded row detail ---------- */

function ExpandedRowDetail({
  col,
  view,
}: {
  col: Record<string, unknown>;
  view: ViewMode;
}) {
  const samples = (col.outlier_values_sample as unknown[]) ?? [];
  const minVal = col.min_value as number | undefined;
  const maxVal = col.max_value as number | undefined;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <div>
        Sample outliers: [{samples.map((v) => String(v)).join(", ")}]
      </div>

      {minVal != null && maxVal != null && (
        <div style={{ color: "var(--text-secondary)", fontSize: 10 }}>
          Range: {formatNum(minVal)} ... {formatNum(maxVal)}
        </div>
      )}

      {(view === "iqr" || view === "combined") &&
        col.lower_fence != null &&
        col.upper_fence != null && (
          <div style={{ fontSize: 10, color: "var(--text-secondary)" }}>
            IQR fence: {formatNum(col.lower_fence as number)} {"<"} normal{" "}
            {">"} {formatNum(col.upper_fence as number)}
          </div>
        )}

      {(view === "zscore" || view === "combined") &&
        col.mean != null &&
        col.std != null && (
          <div style={{ fontSize: 10, color: "var(--text-secondary)" }}>
            Z-score range: {formatNum(col.mean as number)} {"\u00b1"}{" "}
            {col.zscore_threshold != null
              ? `${col.zscore_threshold}\u00d7`
              : ""}
            {formatNum(col.std as number)}
            {col.zscore_threshold != null && (
              <span>
                {" "}
                = [{formatNum((col.mean as number) - (col.zscore_threshold as number) * (col.std as number))},{" "}
                {formatNum((col.mean as number) + (col.zscore_threshold as number) * (col.std as number))}]
              </span>
            )}
          </div>
        )}
    </div>
  );
}

/* ---------- Helpers ---------- */

const tdStyle: React.CSSProperties = {
  padding: "4px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

function formatNum(n: number): string {
  if (Number.isInteger(n)) return n.toLocaleString();
  if (Math.abs(n) < 0.01 && n !== 0) return n.toFixed(4);
  return n.toFixed(2);
}

function fmtOpt(v: unknown): string {
  if (v == null) return "\u2014";
  if (typeof v === "number") return formatNum(v);
  return String(v);
}
