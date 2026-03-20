interface CodePaneFooterProps {
  unsaved: boolean;
  readOnly: boolean;
}

export function CodePaneFooter({ unsaved, readOnly }: CodePaneFooterProps) {
  return (
    <div
      className="flex items-center justify-between px-3 shrink-0"
      style={{
        backgroundColor: "var(--codepane-footer-bg)",
        borderTop: "1px solid var(--codepane-border)",
        height: 28,
        minHeight: 28,
      }}
    >
      {/* Language indicator */}
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

      {/* Unsaved indicator + shortcuts */}
      <div className="flex items-center gap-3">
        {unsaved && (
          <span
            style={{
              fontFamily: "'Inter', sans-serif",
              fontWeight: 600,
              fontSize: 9,
              color: "var(--codepane-unsaved-amber)",
            }}
          >
            ● Unsaved
          </span>
        )}

        {!readOnly && (
          <span
            style={{
              fontFamily: "'Inter', sans-serif",
              fontWeight: 500,
              fontSize: 9,
              color: "var(--codepane-footer-lang)",
              opacity: 0.5,
            }}
          >
            ⌘S save · Esc close
          </span>
        )}
      </div>
    </div>
  );
}
