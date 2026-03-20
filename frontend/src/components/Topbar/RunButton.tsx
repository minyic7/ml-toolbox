import { useEffect, useRef } from "react";
import { useExecutionStore } from "../../store/executionStore";
import { useRunPipeline } from "../../hooks/useExecution";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface RunButtonProps {
  pipelineId: string;
  nodeIds: string[];
  currentNodeLabel?: string | null;
}

export default function RunButton({ pipelineId, nodeIds, currentNodeLabel }: RunButtonProps) {
  const isRunning = useExecutionStore((s) => s.isRunning);
  const pendingNodeIds = useExecutionStore((s) => s.pendingNodeIds);
  const setRunning = useExecutionStore((s) => s.setRunning);
  const setCurrentNodeId = useExecutionStore((s) => s.setCurrentNodeId);

  const runMutation = useRunPipeline(pipelineId, nodeIds);

  // Track initial pending count for completion detection
  const initialCountRef = useRef(0);

  useEffect(() => {
    if (isRunning && pendingNodeIds.length > 0 && initialCountRef.current === 0) {
      initialCountRef.current = pendingNodeIds.length;
    }
    if (!isRunning) {
      initialCountRef.current = 0;
    }
  }, [isRunning, pendingNodeIds.length]);

  // Detect pipeline completion
  useEffect(() => {
    if (!isRunning || initialCountRef.current === 0) return;
    if (pendingNodeIds.length > 0) return;

    setRunning(false);
    setCurrentNodeId(null);

    const nodeStatuses = useExecutionStore.getState().nodeStatuses;
    const hasError = Object.values(nodeStatuses).some((s) => s === "error");

    if (hasError) {
      console.warn("Pipeline finished with errors");
    } else {
      console.info("Pipeline completed successfully");
    }
  }, [isRunning, pendingNodeIds.length, setRunning, setCurrentNodeId]);

  const disabled = isRunning || nodeIds.length === 0;
  const showSpinner = isRunning;

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          onClick={() => runMutation.mutate()}
          disabled={disabled}
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 5,
            padding: "0 12px",
            height: 28,
            borderRadius: 7,
            border: "none",
            background: "var(--accent-primary)",
            color: "var(--node-bg)",
            fontFamily: "'Inter', sans-serif",
            fontWeight: 700,
            fontSize: 11,
            cursor: disabled ? "not-allowed" : "pointer",
            opacity: disabled && !isRunning ? 0.5 : 1,
            transition: "background 0.15s, transform 0.1s",
            maxWidth: isRunning ? 160 : undefined,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
          onMouseEnter={(e) => {
            if (!disabled) e.currentTarget.style.background = "var(--primary-dark)";
          }}
          onMouseLeave={(e) => {
            if (!disabled) e.currentTarget.style.background = "var(--accent-primary)";
          }}
          onMouseDown={(e) => {
            if (!disabled) e.currentTarget.style.transform = "scale(0.97)";
          }}
          onMouseUp={(e) => {
            if (!disabled) e.currentTarget.style.transform = "scale(1)";
          }}
        >
          {showSpinner ? (
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
            <svg width="10" height="12" viewBox="0 0 10 12" fill="currentColor">
              <path d="M1 1.5v9l8-4.5z" />
            </svg>
          )}
          {isRunning ? (currentNodeLabel ?? "Running…") : "Run All"}
        </button>
      </TooltipTrigger>
      {nodeIds.length === 0 && !isRunning && (
        <TooltipContent>Add nodes to the pipeline first</TooltipContent>
      )}
    </Tooltip>
  );
}
