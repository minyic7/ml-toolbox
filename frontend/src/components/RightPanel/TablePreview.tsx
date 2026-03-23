import { useMemo } from "react";
import { SummaryCards } from "../ProfileReport/SummaryCards";

const NUMERIC_DTYPES = new Set([
  "int8", "int16", "int32", "int64",
  "uint8", "uint16", "uint32", "uint64",
  "float16", "float32", "float64",
  "Int8", "Int16", "Int32", "Int64",
  "UInt8", "UInt16", "UInt32", "UInt64",
  "Float32", "Float64",
]);

function isNumericDtype(dtype: string): boolean {
  return NUMERIC_DTYPES.has(dtype) || /^(int|uint|float)\d*$/i.test(dtype);
}

interface PredictionSummary {
  task: "classification" | "regression";
  n_samples: number;
  // Classification fields
  n_classes?: number;
  class_labels?: string[];
  confusion_matrix?: number[][];
  accuracy?: number;
  correct?: number;
  // Regression fields
  mae?: number;
  rmse?: number;
  r2?: number;
}

interface TablePreviewProps {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
  dtypes?: Record<string, string>;
  hasMetadata?: boolean;
  predictionSummary?: PredictionSummary;
}

export function TablePreview({ columns, rows, totalRows, dtypes, hasMetadata, predictionSummary }: TablePreviewProps) {
  const { numericCount, catCount } = useMemo(() => {
    if (!dtypes) return { numericCount: 0, catCount: 0 };
    let numeric = 0;
    let cat = 0;
    for (const col of columns) {
      const dt = dtypes[col];
      if (dt && isNumericDtype(dt)) numeric++;
      else cat++;
    }
    return { numericCount: numeric, catCount: cat };
  }, [columns, dtypes]);

  // Prediction table — show prediction summary instead of generic stats
  if (predictionSummary) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 4 }}>
        {predictionSummary.task === "classification" ? (
          <ClassificationSummary summary={predictionSummary} />
        ) : (
          <RegressionSummary summary={predictionSummary} />
        )}
        {hasMetadata && (
          <div style={{ fontSize: 9, color: "var(--accent-primary)", marginBottom: 4 }}>
            ✓ Schema available
          </div>
        )}
        <DataTable columns={columns} rows={rows} totalRows={totalRows} dtypes={dtypes} />
      </div>
    );
  }

  // Generic table — show full stats
  const summaryItems = useMemo(() => {
    const items: { label: string; value: string | number }[] = [
      { label: "Rows", value: totalRows >= 0 ? totalRows.toLocaleString() : "—" },
      { label: "Columns", value: columns.length },
    ];
    if (dtypes) {
      items.push(
        { label: "Numeric", value: numericCount },
        { label: "Categorical", value: catCount },
      );
    }
    items.push({ label: "Preview", value: `${rows.length} rows` });
    return items;
  }, [totalRows, columns.length, dtypes, numericCount, catCount, rows.length]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 4 }}>
      <SummaryCards items={summaryItems} />
      {hasMetadata && (
        <div style={{ fontSize: 9, color: "var(--accent-primary)", marginBottom: 4 }}>
          ✓ Schema available
        </div>
      )}
      <DataTable columns={columns} rows={rows} totalRows={totalRows} dtypes={dtypes} />
    </div>
  );
}

// ── Classification Summary ──────────────────────────────────────

function ClassificationSummary({ summary }: { summary: PredictionSummary }) {
  const { n_samples, n_classes, class_labels, confusion_matrix: cm, accuracy, correct } = summary;

  const summaryItems = [
    { label: "Rows", value: n_samples.toLocaleString() },
    { label: "Accuracy", value: accuracy != null ? `${(accuracy * 100).toFixed(1)}%` : "—" },
    { label: "Correct", value: correct != null ? `${correct.toLocaleString()} / ${n_samples.toLocaleString()}` : "—" },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <SummaryCards items={summaryItems} />
      {cm && class_labels && n_classes != null && n_classes <= 10 && (
        <ConfusionMatrixTable cm={cm} labels={class_labels} />
      )}
    </div>
  );
}

// ── Regression Summary ──────────────────────────────────────────

function RegressionSummary({ summary }: { summary: PredictionSummary }) {
  const { n_samples, mae, rmse, r2 } = summary;

  const summaryItems: { label: string; value: string | number; color?: string }[] = [
    { label: "Rows", value: n_samples.toLocaleString() },
    { label: "MAE", value: mae ?? "—" },
    { label: "RMSE", value: rmse ?? "—" },
    {
      label: "R²",
      value: r2 ?? "—",
      color: r2 != null ? (r2 >= 0.7 ? "var(--output-healthy-text)" : r2 < 0 ? "var(--error-red)" : undefined) : undefined,
    },
  ];

  return <SummaryCards items={summaryItems} />;
}

// ── Confusion Matrix ────────────────────────────────────────────

function ConfusionMatrixTable({ cm, labels }: { cm: number[][]; labels: string[] }) {
  // Find max value for color intensity scaling
  const maxVal = Math.max(...cm.flat(), 1);

  return (
    <div style={{ overflow: "auto" }}>
      <div style={{
        fontFamily: "'Inter', sans-serif",
        fontSize: 9,
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        color: "var(--text-muted)",
        marginBottom: 4,
      }}>
        Confusion Matrix
        <span style={{ fontWeight: 400, textTransform: "none", letterSpacing: "normal", marginLeft: 6 }}>
          rows = actual, cols = predicted
        </span>
      </div>
      <table style={{
        borderCollapse: "collapse",
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 10,
      }}>
        <thead>
          <tr>
            <th style={{ ...cmHeaderStyle, borderRight: "2px solid var(--border-default)" }} />
            {labels.map((label) => (
              <th key={label} style={cmHeaderStyle}>
                {label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {cm.map((row, i) => {
            const rowSum = row.reduce((a, b) => a + b, 0);
            return (
              <tr key={i}>
                <td style={{
                  ...cmHeaderStyle,
                  borderRight: "2px solid var(--border-default)",
                  textAlign: "right",
                  fontWeight: 600,
                }}>
                  {labels[i]}
                </td>
                {row.map((val, j) => {
                  const isDiagonal = i === j;
                  const intensity = val / maxVal;
                  const pct = rowSum > 0 ? (val / rowSum * 100).toFixed(0) : "0";
                  return (
                    <td
                      key={j}
                      style={{
                        ...cmCellStyle,
                        background: isDiagonal
                          ? `rgba(34, 139, 34, ${0.08 + intensity * 0.25})`
                          : val > 0
                            ? `rgba(220, 53, 69, ${0.05 + intensity * 0.15})`
                            : "transparent",
                        fontWeight: isDiagonal ? 700 : 400,
                        color: isDiagonal ? "var(--text-primary)" : val > 0 ? "var(--text-muted)" : "var(--text-muted)",
                      }}
                      title={`Actual: ${labels[i]}, Predicted: ${labels[j]} — ${val} (${pct}%)`}
                    >
                      {val.toLocaleString()}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const cmHeaderStyle: React.CSSProperties = {
  padding: "4px 8px",
  fontSize: 9,
  fontWeight: 600,
  color: "var(--text-muted)",
  textAlign: "center",
  fontFamily: "'Inter', sans-serif",
  textTransform: "uppercase",
  letterSpacing: "0.03em",
};

const cmCellStyle: React.CSSProperties = {
  padding: "6px 10px",
  textAlign: "center",
  border: "1px solid var(--border-default)",
  minWidth: 48,
};

// ── Data Table ──────────────────────────────────────────────────

function DataTable({
  columns, rows, totalRows, dtypes,
}: {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
  dtypes?: Record<string, string>;
}) {
  return (
    <>
      <div
        style={{
          overflow: "auto",
          maxHeight: 260,
          borderRadius: 6,
          border: "1px solid var(--border-default)",
        }}
      >
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              {columns.map((col) => (
                <th
                  key={col}
                  className="output-thead-th"
                >
                  <div>{col}</div>
                  {dtypes?.[col] && (
                    <span style={{ fontSize: 9, color: "var(--text-muted)", fontWeight: 400 }}>
                      {dtypes[col]}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} className="output-row">
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className={`output-td${j === 0 ? " output-td-first" : ""}`}
                  >
                    {cell === null ? (
                      <span style={{ color: "var(--text-muted)", fontStyle: "italic" }}>null</span>
                    ) : (
                      String(cell)
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length < totalRows && (
        <div className="output-table-footer">
          Showing {rows.length} of {totalRows.toLocaleString()} rows
          {columns.length > 5 && <span> · {columns.length} columns</span>}
        </div>
      )}
      {rows.length >= totalRows && columns.length > 5 && (
        <div className="output-table-footer">
          {columns.length} columns
        </div>
      )}
    </>
  );
}
