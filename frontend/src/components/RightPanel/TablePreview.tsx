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

interface TablePreviewProps {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
  dtypes?: Record<string, string>;
  hasMetadata?: boolean;
}

export function TablePreview({ columns, rows, totalRows, dtypes, hasMetadata }: TablePreviewProps) {
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
    </div>
  );
}
