interface MetricsDisplayProps {
  data: Record<string, unknown>;
}

export function MetricsDisplay({ data }: MetricsDisplayProps) {
  const entries = Object.entries(data);

  if (entries.length === 0) {
    return (
      <div className="output-empty">
        No metrics
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0, marginTop: 4 }}>
      {entries.map(([key, value]) => (
        <div key={key} className="output-metrics-row">
          <span className="output-metrics-key">
            {key}
          </span>
          <span className="output-metrics-value">
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
