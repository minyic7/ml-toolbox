import { memo } from "react";
import { useExecutionStore } from "../../store/executionStore";

interface NodeActionBarProps {
  visible: boolean;
  nodeId: string;
  onRun?: () => void;
  onCode?: () => void;
}

function NodeActionBar({ visible, nodeId, onRun, onCode }: NodeActionBarProps) {
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
        className="node-action-btn node-action-run"
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
            width="8"
            height="8"
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
          <svg width="8" height="9" viewBox="0 0 8 9" fill="currentColor">
            <path d="M1 1.5v6l6-3-6-3z" />
          </svg>
        )}
        Run
      </button>
      <button
        className="node-action-btn node-action-code"
        onClick={(e) => {
          e.stopPropagation();
          onCode?.();
        }}
      >
        <svg width="10" height="8" viewBox="0 0 10 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
          <path d="M3 1L0.5 4L3 7" />
          <path d="M7 1L9.5 4L7 7" />
        </svg>
        Code
      </button>
    </div>
  );
}

export default memo(NodeActionBar);
