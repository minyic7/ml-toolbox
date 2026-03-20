interface TablePreviewProps {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
}

export function TablePreview({ columns, rows, totalRows }: TablePreviewProps) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 4 }}>
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
                <th key={col} className="output-thead-th">
                  {col}
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
