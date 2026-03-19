interface CanvasContextMenuProps {
  x: number;
  y: number;
  onFitView: () => void;
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
  minWidth: 140,
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

export default function CanvasContextMenu({
  x,
  y,
  onFitView,
  onClose,
}: CanvasContextMenuProps) {
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
            onFitView();
            onClose();
          }}
        >
          Fit view
        </button>
      </div>
    </div>
  );
}
