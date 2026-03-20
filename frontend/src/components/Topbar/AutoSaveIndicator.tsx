export type SaveStatus = "saved" | "saving" | "error";

interface AutoSaveIndicatorProps {
  status: SaveStatus;
  onRetry?: () => void;
  retryDisabled?: boolean;
}

export default function AutoSaveIndicator({ status, onRetry, retryDisabled }: AutoSaveIndicatorProps) {
  if (status === "saving") {
    return (
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5 select-none"
        style={{
          background: "#F1EFE8",
          color: "#888780",
          fontFamily: "'Inter', sans-serif",
          fontWeight: 700,
          fontSize: 10,
          lineHeight: "16px",
        }}
      >
        Saving…
      </span>
    );
  }

  if (status === "saved") {
    return (
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5 select-none"
        style={{
          background: "#EAF3DE",
          color: "#166534",
          fontFamily: "'Inter', sans-serif",
          fontWeight: 700,
          fontSize: 10,
          lineHeight: "16px",
        }}
      >
        Saved ✓
      </span>
    );
  }

  // error state
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 select-none"
      style={{
        background: "#FFF7F7",
        color: "var(--error-red)",
        fontFamily: "'Inter', sans-serif",
        fontWeight: 700,
        fontSize: 10,
        lineHeight: "16px",
      }}
    >
      Save failed
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          disabled={retryDisabled}
          className="underline cursor-pointer"
          style={{
            color: "var(--error-red)",
            background: "none",
            border: "none",
            padding: 0,
            font: "inherit",
            fontSize: "inherit",
            fontWeight: "inherit",
            opacity: retryDisabled ? 0.5 : 1,
          }}
        >
          Retry
        </button>
      )}
    </span>
  );
}
