interface ErrorTracebackProps {
  error: string;
}

export function ErrorTraceback({ error }: ErrorTracebackProps) {
  return (
    <div
      style={{
        borderRadius: 6,
        padding: 12,
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 10,
        lineHeight: 1.6,
        whiteSpace: "pre-wrap",
        overflowY: "auto",
        maxHeight: 200,
        background: "var(--error-bg-light)",
        color: "var(--error-red)",
        marginTop: 4,
      }}
    >
      {error}
    </div>
  );
}
