import { useState } from "react";
import type { NodeInstance, NodeDefinition, NodeStatus } from "../../lib/types";
import { useExecutionStore } from "../../store/executionStore";
import { X, Code2, Table, BookOpen } from "lucide-react";

const STATUS_DOT_COLORS: Record<NodeStatus, string> = {
  idle: "var(--status-idle)",
  dirty: "var(--status-idle)",
  pending: "var(--status-pending)",
  running: "var(--accent-primary)",
  done: "var(--success-green)",
  error: "var(--error-red)",
  skipped: "var(--warning-amber)",
  cached: "var(--success-green)",
};

interface DrawerHeaderProps {
  node: NodeInstance;
  definition: NodeDefinition;
  onClose: () => void;
  onCodeClick: () => void;
  onOutputClick: () => void;
  onInfoClick: () => void;
  rightPanelOpen: boolean;
  rightPanelMode: "code" | "output" | "info" | "terminal";
}

export default function DrawerHeader({
  node,
  definition,
  onClose,
  onCodeClick,
  onOutputClick,
  onInfoClick,
  rightPanelOpen,
  rightPanelMode,
}: DrawerHeaderProps) {
  const status = useExecutionStore(
    (s) => s.nodeStatuses[node.id] ?? "idle",
  );
  const dotColor = STATUS_DOT_COLORS[status];
  const displayName = node.name || definition.label || node.type;
  const nodeType = `${definition.category} · ${node.type}`;

  const codeActive = rightPanelOpen && rightPanelMode === "code";
  const outputActive = rightPanelOpen && rightPanelMode === "output";
  const infoActive = rightPanelOpen && rightPanelMode === "info";

  return (
    <div
      className="flex items-center gap-3 px-4 shrink-0"
      style={{
        height: 38,
        borderBottom: "1px solid var(--border-default)",
        background: "var(--node-bg)",
      }}
    >
      {/* Status dot */}
      <span
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: dotColor,
          flexShrink: 0,
        }}
      />

      {/* Node name — Manrope 700 12px uppercase */}
      <span
        style={{
          fontFamily: "'Manrope', sans-serif",
          fontWeight: 700,
          fontSize: 12,
          textTransform: "uppercase",
          color: "var(--text-primary)",
          letterSpacing: "0.04em",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          maxWidth: 180,
        }}
      >
        {displayName}
      </span>

      {/* Node type — Inter 500 10px */}
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 500,
          fontSize: 10,
          color: "var(--text-muted)",
          whiteSpace: "nowrap",
        }}
      >
        {nodeType}
      </span>

      {/* Short ID — click to copy full ID */}
      <ShortId nodeId={node.id} />

      {/* Spacer */}
      <div style={{ flex: 1 }} />

      {/* Params label — always active */}
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 700,
          fontSize: 10,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          color: "var(--accent-primary)",
          padding: "4px 8px",
        }}
      >
        Params
      </span>

      {/* Info icon button */}
      {definition.guide && (
        <button
          onClick={onInfoClick}
          aria-label="Toggle info panel"
          style={{
            width: 28,
            height: 28,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid var(--border-default)",
            borderRadius: 4,
            background: infoActive ? "var(--ghost-hover-bg)" : "transparent",
            cursor: "pointer",
            flexShrink: 0,
            color: infoActive ? "var(--accent-primary)" : "var(--text-muted)",
          }}
          onMouseEnter={(e) => {
            if (!infoActive) {
              (e.currentTarget as HTMLElement).style.background = "var(--ghost-hover-bg)";
            }
          }}
          onMouseLeave={(e) => {
            if (!infoActive) {
              (e.currentTarget as HTMLElement).style.background = "transparent";
            }
          }}
        >
          <BookOpen size={14} />
        </button>
      )}

      {/* Code icon button */}
      <button
        onClick={onCodeClick}
        aria-label="Toggle code panel"
        style={{
          width: 28,
          height: 28,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          border: "1px solid var(--border-default)",
          borderRadius: 4,
          background: codeActive ? "var(--ghost-hover-bg)" : "transparent",
          cursor: "pointer",
          flexShrink: 0,
          color: codeActive ? "var(--accent-primary)" : "var(--text-muted)",
        }}
        onMouseEnter={(e) => {
          if (!codeActive) {
            (e.currentTarget as HTMLElement).style.background = "var(--ghost-hover-bg)";
          }
        }}
        onMouseLeave={(e) => {
          if (!codeActive) {
            (e.currentTarget as HTMLElement).style.background = "transparent";
          }
        }}
      >
        <Code2 size={14} />
      </button>

      {/* Output icon button */}
      <button
        onClick={onOutputClick}
        aria-label="Toggle output panel"
        style={{
          width: 28,
          height: 28,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          border: "1px solid var(--border-default)",
          borderRadius: 4,
          background: outputActive ? "var(--ghost-hover-bg)" : "transparent",
          cursor: "pointer",
          flexShrink: 0,
          color: outputActive ? "var(--accent-primary)" : "var(--text-muted)",
        }}
        onMouseEnter={(e) => {
          if (!outputActive) {
            (e.currentTarget as HTMLElement).style.background = "var(--ghost-hover-bg)";
          }
        }}
        onMouseLeave={(e) => {
          if (!outputActive) {
            (e.currentTarget as HTMLElement).style.background = "transparent";
          }
        }}
      >
        <Table size={14} />
      </button>

      {/* Close button — 24x24px */}
      <button
        onClick={onClose}
        aria-label="Close drawer"
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
          flexShrink: 0,
          color: "var(--text-muted)",
        }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLElement).style.background = "var(--ghost-hover-bg)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLElement).style.background = "transparent";
        }}
      >
        <X size={14} />
      </button>
    </div>
  );
}

function ShortId({ nodeId }: { nodeId: string }) {
  const [copied, setCopied] = useState(false);

  const handleClick = () => {
    navigator.clipboard.writeText(nodeId).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };

  return (
    <span
      style={{
        fontSize: 9,
        fontFamily: "'JetBrains Mono', monospace",
        color: copied ? "var(--success-green)" : "var(--text-muted)",
        cursor: "pointer",
        opacity: 0.7,
        whiteSpace: "nowrap",
        transition: "color 150ms ease",
      }}
      onClick={handleClick}
      title={`${nodeId}\nClick to copy`}
    >
      {copied ? "copied!" : nodeId.slice(0, 8)}
    </span>
  );
}
