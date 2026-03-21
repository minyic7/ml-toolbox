interface Warning {
  type: string;
  column?: string;
  message: string;
}

interface WarningListProps {
  warnings: Warning[];
}

const SEVERITY_COLORS: Record<string, { bg: string; text: string }> = {
  low: { bg: "#fef9c3", text: "#854d0e" },
  medium: { bg: "#fed7aa", text: "#9a3412" },
  high: { bg: "#fecaca", text: "#991b1b" },
  critical: { bg: "#fee2e2", text: "#7f1d1d" },
};

function getSeverity(type: string): string {
  const lower = type.toLowerCase();
  if (lower.includes("critical")) return "critical";
  if (lower.includes("high")) return "high";
  if (lower.includes("medium")) return "medium";
  return "low";
}

export function WarningList({ warnings }: WarningListProps) {
  if (!warnings || warnings.length === 0) return null;

  return (
    <div style={{ marginTop: 16 }}>
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          fontFamily: "'Inter', sans-serif",
          textTransform: "uppercase",
          color: "var(--text-secondary)",
          borderBottom: "1px solid var(--border-default)",
          paddingBottom: 6,
          marginBottom: 8,
        }}
      >
        Warnings
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {warnings.map((w, i) => {
          const severity = getSeverity(w.type);
          const colors = SEVERITY_COLORS[severity] ?? SEVERITY_COLORS.low;
          return (
            <div
              key={i}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                fontSize: 11,
                fontFamily: "'Inter', sans-serif",
              }}
            >
              <span
                style={{
                  display: "inline-block",
                  padding: "2px 8px",
                  borderRadius: 9999,
                  backgroundColor: colors.bg,
                  color: colors.text,
                  fontSize: 10,
                  fontWeight: 700,
                  textTransform: "uppercase",
                  whiteSpace: "nowrap",
                  flexShrink: 0,
                }}
              >
                {w.type}
              </span>
              {w.column && (
                <span
                  style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: 10,
                    color: "var(--text-secondary)",
                    flexShrink: 0,
                  }}
                >
                  {w.column}
                </span>
              )}
              <span style={{ color: "var(--text-primary)" }}>{w.message}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
