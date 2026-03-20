interface MetricsDisplayProps {
  data: Record<string, unknown>;
}

export function MetricsDisplay({ data }: MetricsDisplayProps) {
  const entries = Object.entries(data);

  if (entries.length === 0) {
    return (
      <div style={{
        fontFamily: "'Inter', sans-serif",
        fontSize: 11,
        fontWeight: 600,
        color: "var(--text-muted)",
        textAlign: "center",
        padding: "24px 0",
      }}>
        No metrics
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0, marginTop: 4 }}>
      {entries.map(([key, value]) => (
        <div
          key={key}
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "6px 0",
            borderBottom: "1px solid var(--output-thead-bg)",
          }}
        >
          <span style={{
            fontFamily: "'Inter', sans-serif",
            fontSize: 10,
            fontWeight: 400,
            color: "var(--text-muted)",
          }}>
            {key}
          </span>
          <span style={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10,
            fontWeight: 600,
            color: "var(--text-primary)",
            textAlign: "right",
          }}>
            {typeof value === "number"
              ? Number.isInteger(value)
                ? value.toLocaleString()
                : value.toFixed(4)
              : String(value)}
          </span>
        </div>
      ))}
    </div>
  );
}
