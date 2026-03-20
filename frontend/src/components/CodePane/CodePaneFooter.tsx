interface CodePaneFooterProps {
  unsaved: boolean;
}

export default function CodePaneFooter({ unsaved }: CodePaneFooterProps) {
  const isMac = typeof navigator !== "undefined" && /Mac/.test(navigator.userAgent);
  const modKey = isMac ? "\u2318" : "Ctrl+";

  return (
    <div
      className="flex items-center px-3 shrink-0"
      style={{
        height: 28,
        background: "var(--codepane-footer-bg)",
        borderTop: "1px solid var(--codepane-border)",
      }}
    >
      {/* Language */}
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 500,
          fontSize: 9,
          color: "var(--codepane-footer-lang)",
        }}
      >
        Python 3.13
      </span>

      <div style={{ flex: 1 }} />

      {/* Unsaved indicator */}
      {unsaved && (
        <span
          style={{
            fontFamily: "'Inter', sans-serif",
            fontWeight: 600,
            fontSize: 9,
            color: "var(--codepane-unsaved-amber)",
            marginRight: 12,
          }}
        >
          ● unsaved
        </span>
      )}

      {/* Keyboard hints */}
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 400,
          fontSize: 9,
          color: "var(--codepane-icon-color)",
          opacity: 0.5,
        }}
      >
        {modKey}S save · Esc close
      </span>
    </div>
  );
}
