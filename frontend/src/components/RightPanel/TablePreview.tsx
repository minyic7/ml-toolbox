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
                <th
                  key={col}
                  style={{
                    position: "sticky",
                    top: 0,
                    zIndex: 1,
                    background: "var(--output-thead-bg)",
                    fontFamily: "'Inter', sans-serif",
                    fontSize: 9,
                    fontWeight: 700,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "var(--text-muted)",
                    padding: "4px 10px",
                    textAlign: "left",
                    whiteSpace: "nowrap",
                    borderBottom: "1px solid var(--border-default)",
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
                style={{ borderBottom: "1px solid var(--output-thead-bg)" }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = "var(--output-row-hover)";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = "transparent";
                }}
              >
                {row.map((cell, j) => (
                  <td
                    key={j}
                    style={{
                      padding: "3px 10px",
                      whiteSpace: "nowrap",
                      fontFamily: j === 0 ? "'JetBrains Mono', monospace" : "'Inter', sans-serif",
                      fontSize: 10,
                      fontWeight: 400,
                      color: j === 0 ? "var(--output-first-col)" : "var(--text-primary)",
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
