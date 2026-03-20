import { memo } from "react";

interface NodeActionBarProps {
  visible: boolean;
  onRun?: () => void;
  onCode?: () => void;
}

function NodeActionBar({ visible, onRun, onCode }: NodeActionBarProps) {
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
          onRun?.();
        }}
      >
        <svg width="8" height="9" viewBox="0 0 8 9" fill="currentColor">
          <path d="M1 1.5v6l6-3-6-3z" />
        </svg>
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
