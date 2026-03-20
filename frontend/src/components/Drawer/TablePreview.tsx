interface TablePreviewProps {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
}

export function TablePreview({ columns, rows, totalRows }: TablePreviewProps) {
  return (
    <div className="flex flex-col gap-2">
      <div className="overflow-x-auto rounded-md border" style={{ borderColor: "var(--border-default)", maxHeight: 200 }}>
        <table className="w-full text-xs">
          <thead className="sticky top-0" style={{ backgroundColor: "var(--canvas-bg)" }}>
            <tr>
              {columns.map((col) => (
                <th
                  key={col}
                  className="px-2.5 py-1 text-left whitespace-nowrap"
                  style={{
                    fontFamily: "'Inter', sans-serif",
                    fontWeight: 700,
                    fontSize: 9,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "var(--text-muted)",
                  }}
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr
                key={i}
                className="transition-colors"
                style={{ borderTop: "1px solid var(--canvas-bg)" }}
              >
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className="px-2.5 py-1 whitespace-nowrap"
                    style={{
                      color: "var(--text-primary)",
                      fontFamily: j === 0 ? "'JetBrains Mono', monospace" : "'Inter', sans-serif",
                      fontSize: 10,
                      fontWeight: 400,
                    }}
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
        <div style={{
          fontFamily: "'Inter', sans-serif",
          fontSize: 9,
          fontWeight: 400,
          color: "var(--text-muted)",
          padding: "2px 0",
        }}>
          Showing {rows.length} of {totalRows.toLocaleString()} rows
        </div>
      )}
    </div>
  );
}
