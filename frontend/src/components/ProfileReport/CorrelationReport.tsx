import { SummaryCards } from "./SummaryCards";
import { WarningList } from "./WarningList";

interface CorrelationReportProps {
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

export function CorrelationReport({ data }: CorrelationReportProps) {
  const summary = data.summary as Record<string, unknown> | undefined;
  const topPairs = (data.top_pairs ?? []) as Record<string, unknown>[];
  const targetCorrelations = (data.target_correlations ?? null) as
    | Record<string, unknown>[]
    | null;
  const matrix = data.matrix as Record<string, unknown> | undefined;
  const warnings = (data.warnings ?? []) as {
    type: string;
    column?: string;
    message: string;
  }[];

  const summaryItems = [
    { label: "Numeric Cols", value: (summary?.numeric_columns as number) ?? 0 },
    { label: "Total Pairs", value: (summary?.total_pairs as number) ?? 0 },
    {
      label: "High Correlations",
      value: (summary?.high_correlation_count as number) ?? 0,
      color: ((summary?.high_correlation_count as number) ?? 0) > 0 ? "#dc2626" : undefined,
    },
  ];

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {topPairs.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Top Correlated Pairs</div>
          <TopPairsTable pairs={topPairs} />
        </>
      )}

      {targetCorrelations && targetCorrelations.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Target Correlations</div>
          <TargetCorrelationsTable correlations={targetCorrelations} />
        </>
      )}

      {matrix && (
        <>
          <div style={SECTION_HEADER}>Correlation Matrix</div>
          <CorrelationMatrix matrix={matrix} />
        </>
      )}

      <WarningList warnings={warnings} />
    </div>
  );
}

function correlationBadge(r: number): { label: string; bg: string; text: string } {
  const absR = Math.abs(r);
  if (absR >= 0.7) return { label: "high", bg: "#fecaca", text: "#991b1b" };
  if (absR >= 0.4) return { label: "medium", bg: "#fed7aa", text: "#9a3412" };
  return { label: "low", bg: "#dcfce7", text: "#166534" };
}

function correlationCellColor(r: number): string {
  const absR = Math.abs(r);
  const opacity = absR * 0.6;
  if (r > 0) return `rgba(220, 38, 38, ${opacity})`;
  if (r < 0) return `rgba(37, 99, 235, ${opacity})`;
  return "transparent";
}

function TopPairsTable({ pairs }: { pairs: Record<string, unknown>[] }) {
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
            {["Feature A", "Feature B", "r", "Strength"].map((h) => (
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
          {pairs.map((p, i) => {
            const r = (p.correlation as number) ?? 0;
            const badge = correlationBadge(r);
            return (
              <tr
                key={i}
                style={{
                  backgroundColor:
                    i % 2 === 1
                      ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                      : "transparent",
                }}
              >
                <td style={cellStyle}>{String(p.feature_a ?? "")}</td>
                <td style={cellStyle}>{String(p.feature_b ?? "")}</td>
                <td style={cellStyle}>{r.toFixed(4)}</td>
                <td style={cellStyle}>
                  <span
                    style={{
                      padding: "2px 8px",
                      borderRadius: 9999,
                      backgroundColor: badge.bg,
                      color: badge.text,
                      fontSize: 10,
                      fontWeight: 700,
                      textTransform: "uppercase",
                    }}
                  >
                    {badge.label}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const cellStyle: React.CSSProperties = {
  padding: "4px 8px",
  color: "var(--text-primary)",
  borderBottom: "1px solid var(--border-default)",
  whiteSpace: "nowrap",
};

function TargetCorrelationsTable({
  correlations,
}: {
  correlations: Record<string, unknown>[];
}) {
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
            {["Feature", "r", ""].map((h, i) => (
              <th
                key={i}
                style={{
                  padding: "6px 8px",
                  textAlign: "left",
                  fontFamily: "'Inter', sans-serif",
                  fontSize: 10,
                  fontWeight: 600,
                  color: "var(--text-secondary)",
                  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                  borderBottom: "1px solid var(--border-default)",
                  width: i === 2 ? "40%" : undefined,
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {correlations.map((c, i) => {
            const r = (c.correlation as number) ?? 0;
            const absR = Math.abs(r);
            return (
              <tr
                key={i}
                style={{
                  backgroundColor:
                    i % 2 === 1
                      ? "var(--output-row-hover, rgba(0,0,0,0.02))"
                      : "transparent",
                }}
              >
                <td style={cellStyle}>{String(c.feature ?? "")}</td>
                <td style={cellStyle}>{r.toFixed(4)}</td>
                <td style={cellStyle}>
                  <div
                    style={{
                      height: 10,
                      backgroundColor: "var(--border-default)",
                      borderRadius: 3,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        width: `${absR * 100}%`,
                        height: "100%",
                        backgroundColor:
                          r >= 0
                            ? "rgba(220, 38, 38, 0.6)"
                            : "rgba(37, 99, 235, 0.6)",
                        borderRadius: 3,
                      }}
                    />
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function CorrelationMatrix({ matrix }: { matrix: Record<string, unknown> }) {
  const labels = (matrix.labels ?? []) as string[];
  const values = (matrix.values ?? []) as number[][];

  if (labels.length === 0 || values.length === 0) return null;

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
          borderCollapse: "collapse",
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 9,
        }}
      >
        <thead>
          <tr>
            <th
              style={{
                padding: "4px 6px",
                backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                borderBottom: "1px solid var(--border-default)",
                borderRight: "1px solid var(--border-default)",
              }}
            />
            {labels.map((l) => (
              <th
                key={l}
                style={{
                  padding: "4px 6px",
                  fontWeight: 600,
                  color: "var(--text-secondary)",
                  backgroundColor: "var(--output-thead-bg, #f5f5f5)",
                  borderBottom: "1px solid var(--border-default)",
                  whiteSpace: "nowrap",
                  maxWidth: 60,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
                title={l}
              >
                {l}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {values.map((row, i) => (
            <tr key={i}>
              <td
                style={{
                  padding: "4px 6px",
                  fontWeight: 600,
                  color: "var(--text-secondary)",
                  borderRight: "1px solid var(--border-default)",
                  whiteSpace: "nowrap",
                  maxWidth: 80,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
                title={labels[i]}
              >
                {labels[i]}
              </td>
              {row.map((val, j) => (
                <td
                  key={j}
                  style={{
                    padding: "4px 6px",
                    textAlign: "center",
                    backgroundColor: correlationCellColor(val),
                    color:
                      Math.abs(val) > 0.5
                        ? "#ffffff"
                        : "var(--text-primary)",
                    borderBottom: "1px solid var(--border-default)",
                    borderRight: "1px solid var(--border-default)",
                  }}
                  title={`${labels[i]} × ${labels[j]} = ${val.toFixed(4)}`}
                >
                  {val.toFixed(2)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
