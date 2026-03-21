interface SummaryItem {
  label: string;
  value: string | number;
  color?: string;
}

interface SummaryCardsProps {
  items: SummaryItem[];
}

export function SummaryCards({ items }: SummaryCardsProps) {
  return (
    <div
      style={{
        display: "flex",
        gap: 8,
        flexWrap: "wrap",
        marginBottom: 16,
      }}
    >
      {items.map((item) => (
        <div
          key={item.label}
          style={{
            flex: "1 1 100px",
            minWidth: 90,
            padding: "10px 12px",
            borderRadius: 8,
            border: "1px solid var(--border-default)",
            boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
          }}
        >
          <div
            style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 16,
              fontWeight: 700,
              color: item.color ?? "var(--text-primary)",
              lineHeight: 1.2,
            }}
          >
            {typeof item.value === "number"
              ? Number.isInteger(item.value)
                ? item.value.toLocaleString()
                : item.value.toFixed(4)
              : item.value}
          </div>
          <div
            style={{
              fontFamily: "'Inter', sans-serif",
              fontSize: 10,
              fontWeight: 400,
              color: "var(--text-muted)",
              marginTop: 4,
              textTransform: "uppercase",
              letterSpacing: "0.03em",
            }}
          >
            {item.label}
          </div>
        </div>
      ))}
    </div>
  );
}
