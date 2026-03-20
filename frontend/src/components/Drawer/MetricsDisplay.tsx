interface MetricsDisplayProps {
  data: Record<string, unknown>;
}

export function MetricsDisplay({ data }: MetricsDisplayProps) {
  const entries = Object.entries(data);

  if (entries.length === 0) {
    return (
      <div
        className="text-sm"
        style={{ color: "var(--text-muted)" }}
      >
        No metrics
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-2">
      {entries.map(([key, value]) => (
        <div
          key={key}
          className="flex items-center justify-between rounded-md border px-3 py-2"
          style={{ borderColor: "var(--border-default)" }}
        >
          <span
            className="text-xs font-medium"
            style={{ color: "var(--text-secondary)" }}
          >
            {key}
          </span>
          <span
            className="text-sm font-mono tabular-nums"
            style={{ color: "var(--text-primary)" }}
          >
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
