import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { X, BookOpen } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { NodeInstance, NodeDefinition } from "../../lib/types";

interface InfoPanelProps {
  node: NodeInstance;
  definition: NodeDefinition;
  onClose: () => void;
}

export default function InfoPanel({ node, definition, onClose }: InfoPanelProps) {
  const displayName = node.name || definition.label || node.type;

  // ── Resizable width ──────────────────────────────────────────
  const [width, setWidth] = useState(340);
  const [dragging, setDragging] = useState(false);
  const [handleHovered, setHandleHovered] = useState(false);
  const dragStartX = useRef(0);
  const dragStartWidth = useRef(340);

  const minWidth = 300;
  const maxWidth = useMemo(() => Math.floor(window.innerWidth * 0.5), []);

  useEffect(() => {
    if (!dragging) return;
    const handleMouseMove = (e: MouseEvent) => {
      e.preventDefault();
      const delta = dragStartX.current - e.clientX;
      const newWidth = Math.min(maxWidth, Math.max(minWidth, dragStartWidth.current + delta));
      setWidth(newWidth);
    };
    const handleMouseUp = () => {
      setDragging(false);
    };
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
  }, [dragging, maxWidth]);

  const handleDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragStartX.current = e.clientX;
    dragStartWidth.current = width;
    setDragging(true);
  }, [width]);

  const guide = definition.guide;

  return (
    <div
      className="flex flex-col h-full"
      style={{
        width,
        minWidth: width,
        background: "var(--canvas-bg)",
        borderLeft: "1px solid var(--border-default)",
        position: "relative",
      }}
    >
      {/* Drag handle for resizing */}
      <div
        onMouseDown={handleDragStart}
        onMouseEnter={() => setHandleHovered(true)}
        onMouseLeave={() => setHandleHovered(false)}
        style={{
          position: "absolute",
          top: 0,
          left: 0,
          bottom: 0,
          width: 4,
          cursor: "col-resize",
          zIndex: 10,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <div
          style={{
            width: 2,
            height: 32,
            borderRadius: 1,
            background: dragging || handleHovered ? "var(--text-muted)" : "var(--border-default)",
            transition: "background 150ms",
          }}
        />
      </div>

      {/* Header */}
      <div
        className="flex items-center gap-2 px-3 shrink-0"
        style={{
          height: 38,
          background: "var(--node-bg)",
          borderBottom: "1px solid var(--border-default)",
        }}
      >
        <BookOpen size={14} style={{ color: "var(--accent-primary)", flexShrink: 0 }} />
        <span
          style={{
            fontFamily: "'Inter', sans-serif",
            fontWeight: 600,
            fontSize: 11,
            color: "var(--text-primary)",
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
            maxWidth: 200,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
        >
          {displayName} — Guide
        </span>

        <div style={{ flex: 1 }} />

        {/* Close button */}
        <button
          onClick={onClose}
          title="Close (Esc)"
          style={{
            width: 24,
            height: 24,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid var(--border-default)",
            borderRadius: 4,
            background: "transparent",
            cursor: "pointer",
            color: "var(--text-muted)",
            flexShrink: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "var(--ghost-hover-bg)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "transparent";
          }}
        >
          <X size={13} />
        </button>
      </div>

      {/* Content */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "16px 20px",
          background: "var(--canvas-bg)",
        }}
      >
        {guide ? (
          <div className="info-prose">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{guide}</ReactMarkdown>
          </div>
        ) : (
          <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
            No guide available for this node.
          </p>
        )}
      </div>
    </div>
  );
}
