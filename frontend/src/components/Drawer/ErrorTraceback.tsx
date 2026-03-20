interface ErrorTracebackProps {
  error: string;
}

export function ErrorTraceback({ error }: ErrorTracebackProps) {
  return (
    <div
      className="rounded-md border p-3 font-mono text-xs leading-relaxed whitespace-pre-wrap"
      style={{
        borderColor: "var(--error-red)",
        backgroundColor: "color-mix(in srgb, var(--error-red) 8%, transparent)",
        color: "var(--error-red)",
      }}
    >
      {error}
    </div>
  );
}
