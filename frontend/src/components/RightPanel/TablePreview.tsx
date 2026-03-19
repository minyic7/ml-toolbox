interface TablePreviewProps {
  columns: string[];
  rows: unknown[][];
  totalRows: number;
}

export function TablePreview({ columns, rows, totalRows }: TablePreviewProps) {
  return (
    <div className="flex flex-col gap-2">
      <div
        className="text-xs"
        style={{ color: "var(--text-muted)" }}
      >
        {totalRows.toLocaleString()} rows
      </div>
      <div className="overflow-x-auto rounded-md border" style={{ borderColor: "var(--border-default)" }}>
        <table className="w-full text-xs">
          <thead>
            <tr style={{ backgroundColor: "var(--canvas-bg)" }}>
              {columns.map((col) => (
                <th
                  key={col}
                  className="px-2 py-1.5 text-left font-medium whitespace-nowrap"
                  style={{ color: "var(--text-secondary)", borderColor: "var(--border-default)" }}
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
                className="border-t"
                style={{ borderColor: "var(--border-default)" }}
              >
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className="px-2 py-1 whitespace-nowrap"
                    style={{ color: "var(--text-primary)" }}
                  >
                    {cell === null ? (
                      <span style={{ color: "var(--text-muted)" }}>null</span>
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
        <div
          className="text-xs"
          style={{ color: "var(--text-muted)" }}
        >
          Showing {rows.length} of {totalRows.toLocaleString()} rows
        </div>
      )}
    </div>
  );
}
