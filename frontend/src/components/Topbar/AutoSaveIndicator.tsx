export type SaveStatus = "saved" | "saving" | "error";

interface AutoSaveIndicatorProps {
  status: SaveStatus;
  onRetry?: () => void;
}

export default function AutoSaveIndicator({ status, onRetry }: AutoSaveIndicatorProps) {
  return (
    <span
      className="flex items-center gap-1.5 text-xs select-none"
      style={{
        color:
          status === "error"
            ? "var(--error-red)"
            : "var(--text-muted)",
      }}
    >
      {status === "saving" && (
        <>
          <span
            className="inline-block h-1.5 w-1.5 rounded-full animate-pulse"
            style={{ backgroundColor: "var(--warning-amber)" }}
          />
          Saving…
        </>
      )}
      {status === "saved" && (
        <>
          <span
            className="inline-block h-1.5 w-1.5 rounded-full"
            style={{ backgroundColor: "var(--success-green)" }}
          />
          Saved
        </>
      )}
      {status === "error" && (
        <>
          <span
            className="inline-block h-1.5 w-1.5 rounded-full"
            style={{ backgroundColor: "var(--error-red)" }}
          />
          Save failed
          {onRetry && (
            <button
              type="button"
              onClick={onRetry}
              className="underline cursor-pointer ml-1"
              style={{ color: "var(--error-red)", background: "none", border: "none", padding: 0, font: "inherit", fontSize: "inherit" }}
            >
              Retry
            </button>
          )}
        </>
      )}
    </span>
  );
}
