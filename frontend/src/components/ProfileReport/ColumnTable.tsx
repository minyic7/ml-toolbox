import { useState, useCallback } from "react";

interface Header {
  key: string;
  label: string;
}

interface ColumnTableProps {
  columns: Record<string, unknown>[];
  headers: Header[];
}

export function ColumnTable({ columns, headers }: ColumnTableProps) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  const handleSort = useCallback(
    (key: string) => {
      if (sortKey === key) {
        setSortAsc((prev) => !prev);
      } else {
        setSortKey(key);
        setSortAsc(true);
      }
    },
    [sortKey],
  );

  const sorted = sortKey
    ? [...columns].sort((a, b) => {
        const av = a[sortKey];
        const bv = b[sortKey];
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (typeof av === "number" && typeof bv === "number") {
          return sortAsc ? av - bv : bv - av;
        }
        const cmp = String(av).localeCompare(String(bv));
        return sortAsc ? cmp : -cmp;
      })
    : columns;

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
                key={h.key}
                onClick={() => handleSort(h.key)}
                style={{
                  position: "sticky",
                  top: 0,
                  padding: "6px 8px",
                  textAlign: "left",
                  fontFamily: "'Inter', sans-serif",
                  fontSize: 10,
                  fontWeight: 600,
                  color: "var(--text-secondary)",
                  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                  borderBottom: "1px solid var(--border-default)",
                  cursor: "pointer",
                  userSelect: "none",
                  whiteSpace: "nowrap",
                }}
              >
                {h.label}
                {sortKey === h.key ? (sortAsc ? " \u25b2" : " \u25bc") : ""}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr
              key={i}
              style={{
                backgroundColor:
                  i % 2 === 1
                    ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                    : "transparent",
              }}
            >
              {headers.map((h) => (
                <td
                  key={h.key}
                  style={{
                    padding: "4px 8px",
                    color: "var(--text-primary)",
                    borderBottom: "1px solid var(--border-default)",
                    whiteSpace: "nowrap",
                  }}
                >
                  {formatCell(row[h.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value == null) return "\u2014";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toLocaleString();
    return value.toFixed(4);
  }
  return String(value);
}
