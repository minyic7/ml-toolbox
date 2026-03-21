import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { X, Maximize2, Minimize2 } from "lucide-react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import { OutputTab } from "../RightPanel/OutputTab";

interface OutputPanelProps {
  node: NodeInstance;
  definition: NodeDefinition;
  pipelineId: string;
  onClose: () => void;
  onRunFrom?: (nodeId: string) => void;
  requestedRunId?: string | null;
  onRequestedRunHandled?: () => void;
}

export default function OutputPanel({
  node,
  definition,
  pipelineId,
  onClose,
  onRunFrom,
  requestedRunId,
  onRequestedRunHandled,
}: OutputPanelProps) {
  const displayName = node.name || definition.label || node.type;

  // ── Fullscreen ─────────────────────────────────────────────
  const [isFullscreen, setIsFullscreen] = useState(false);

  useEffect(() => {
    if (!isFullscreen) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsFullscreen(false);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isFullscreen]);

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

  return (
    <div
      className="flex flex-col h-full"
      style={
        isFullscreen
          ? {
              position: "fixed" as const,
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              width: "100vw",
              zIndex: 50,
              background: "var(--canvas-bg)",
            }
          : {
              width,
              minWidth: width,
              background: "var(--canvas-bg)",
              borderLeft: "1px solid var(--border-default)",
              position: "relative" as const,
            }
      }
    >
      {/* Drag handle for resizing */}
      {!isFullscreen && <div
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
      </div>}

      {/* Header — light themed, node name + close button only */}
      <div
        className="flex items-center gap-2 px-3 shrink-0"
        style={{
          height: 38,
          background: "var(--node-bg)",
          borderBottom: "1px solid var(--border-default)",
        }}
      >
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
          {displayName} — Output
        </span>

        <div style={{ flex: 1 }} />

        {/* Fullscreen toggle */}
        <button
          onClick={() => setIsFullscreen(!isFullscreen)}
          aria-label={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
          title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
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
          {isFullscreen ? <Minimize2 size={13} /> : <Maximize2 size={13} />}
        </button>

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

      {/* Output content — scrollable */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          background: "var(--canvas-bg)",
        }}
      >
        <OutputTab
          pipelineId={pipelineId}
          nodeId={node.id}
          onRunFrom={onRunFrom ? () => onRunFrom(node.id) : undefined}
          requestedRunId={requestedRunId}
          onRequestedRunHandled={onRequestedRunHandled}
        />
      </div>
    </div>
  );
}
