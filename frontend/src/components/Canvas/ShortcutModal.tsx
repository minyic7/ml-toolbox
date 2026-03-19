interface ShortcutModalProps {
  open: boolean;
  onClose: () => void;
}

const SHORTCUTS = [
  { keys: "Delete / Backspace", action: "Delete selected node or edge" },
  { keys: "Ctrl + A", action: "Select all nodes" },
  { keys: "Ctrl + F", action: "Fit view" },
  { keys: "?", action: "Show keyboard shortcuts" },
  { keys: "Escape", action: "Deselect / Close modal" },
];

export default function ShortcutModal({ open, onClose }: ShortcutModalProps) {
  if (!open) return null;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 100,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "rgba(0,0,0,0.3)",
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: "var(--node-bg)",
          borderRadius: 12,
          border: "1px solid var(--border-default)",
          boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
          padding: 24,
          minWidth: 340,
          maxWidth: 420,
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <h2
          style={{
            margin: "0 0 16px",
            fontSize: 16,
            fontWeight: 600,
            color: "var(--text-primary)",
          }}
        >
          Keyboard shortcuts
        </h2>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {SHORTCUTS.map((s) => (
            <div
              key={s.keys}
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: 16,
              }}
            >
              <span
                style={{
                  fontSize: 13,
                  color: "var(--text-primary)",
                }}
              >
                {s.action}
              </span>
              <kbd
                style={{
                  fontSize: 12,
                  fontFamily: "inherit",
                  padding: "2px 8px",
                  borderRadius: 4,
                  background: "var(--canvas-bg)",
                  border: "1px solid var(--border-default)",
                  color: "var(--text-secondary)",
                  whiteSpace: "nowrap",
                }}
              >
                {s.keys}
              </kbd>
            </div>
          ))}
        </div>
        <div style={{ marginTop: 20, textAlign: "right" }}>
          <button
            onClick={onClose}
            style={{
              padding: "6px 16px",
              borderRadius: 6,
              border: "1px solid var(--border-default)",
              background: "var(--node-bg)",
              color: "var(--text-primary)",
              cursor: "pointer",
              fontSize: 13,
              fontFamily: "inherit",
            }}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
