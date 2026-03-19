interface ContextMenuProps {
  x: number;
  y: number;
  nodeId: string;
  onRunFrom: (nodeId: string) => void;
  onDelete: (nodeId: string) => void;
  onClose: () => void;
}

const menuStyle: React.CSSProperties = {
  position: "fixed",
  zIndex: 50,
  background: "var(--node-bg)",
  border: "1px solid var(--border-default)",
  borderRadius: 8,
  boxShadow: "0 4px 16px rgba(0,0,0,0.1)",
  padding: "4px 0",
  minWidth: 160,
  fontSize: 13,
};

const itemStyle: React.CSSProperties = {
  padding: "8px 14px",
  cursor: "pointer",
  color: "var(--text-primary)",
  display: "block",
  width: "100%",
  textAlign: "left",
  border: "none",
  background: "none",
  fontFamily: "inherit",
  fontSize: "inherit",
};

export default function ContextMenu({
  x,
  y,
  nodeId,
  onRunFrom,
  onDelete,
  onClose,
}: ContextMenuProps) {
  return (
    <div
      style={{ position: "fixed", inset: 0, zIndex: 49 }}
      onClick={onClose}
    >
      <div style={{ ...menuStyle, left: x, top: y }}>
        <button
          style={itemStyle}
          onMouseEnter={(e) =>
            (e.currentTarget.style.background = "rgba(0,0,0,0.04)")
          }
          onMouseLeave={(e) => (e.currentTarget.style.background = "none")}
          onClick={() => {
            onRunFrom(nodeId);
            onClose();
          }}
        >
          Run from here
        </button>
        <button
          style={{
            ...itemStyle,
            color: "var(--error-red)",
          }}
          onMouseEnter={(e) =>
            (e.currentTarget.style.background = "rgba(226,75,74,0.06)")
          }
          onMouseLeave={(e) => (e.currentTarget.style.background = "none")}
          onClick={() => {
            onDelete(nodeId);
            onClose();
          }}
        >
          Delete node
        </button>
      </div>
    </div>
  );
}
