import { memo } from "react";
import { useExecutionStore } from "../../store/executionStore";

interface NodeActionBarProps {
  visible: boolean;
  nodeId: string;
  onRun?: () => void;
  onDelete?: () => void;
  onOpenTerminal?: () => void;
}

function NodeActionBar({ visible, nodeId, onRun, onDelete, onOpenTerminal }: NodeActionBarProps) {
  const isRunning = useExecutionStore((s) => s.isRunning);
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);
  const thisNodeRunning =
    nodeStatuses[nodeId] === "running" || nodeStatuses[nodeId] === "pending";
  const runDisabled = isRunning;
  return (
    <div
      className="nodrag nopan"
      style={{
        display: "flex",
        borderTop: "1px solid var(--border-default)",
        height: 28,
        opacity: visible ? 1 : 0,
        pointerEvents: visible ? "auto" : "none",
        transition: "opacity 150ms ease",
        borderRadius: "0 0 8px 0",
        overflow: "hidden",
      }}
    >
      <button
        className="node-action-btn"
        title="Run from this node"
        onClick={(e) => {
          e.stopPropagation();
          if (!runDisabled) onRun?.();
        }}
        style={{
          opacity: runDisabled ? 0.4 : undefined,
          cursor: runDisabled ? "not-allowed" : undefined,
        }}
      >
        {thisNodeRunning ? (
          <svg
            className="topbar-spinner"
            width="12"
            height="12"
            viewBox="0 0 12 12"
            fill="none"
          >
            <circle
              cx="6"
              cy="6"
              r="5"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeDasharray="20 10"
              opacity="0.8"
            />
          </svg>
        ) : (
          <svg width="12" height="14" viewBox="0 0 8 9" fill="currentColor">
            <path d="M1 1.5v6l6-3-6-3z" />
          </svg>
        )}
      </button>
      <button
        className="node-action-btn"
        title="Configure with CC"
        onClick={(e) => {
          e.stopPropagation();
          onOpenTerminal?.();
        }}
        style={{ color: "#bb9af7" }}
      >
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 3l1.9 5.5H20l-4.6 3.4 1.8 5.5L12 14l-5.2 3.4 1.8-5.5L4 8.5h6.1z" />
        </svg>
      </button>
      <button
        className="node-action-btn"
        title="Delete node"
        onClick={(e) => {
          e.stopPropagation();
          onDelete?.();
        }}
        style={{ color: "var(--error-red)" }}
      >
        <svg width="12" height="12" viewBox="0 0 9 10" fill="none" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round">
          <path d="M1 2.5h7M3 2.5V1.5h3v1M2 2.5l.5 6h4l.5-6" />
        </svg>
      </button>
    </div>
  );
}

export default memo(NodeActionBar);
