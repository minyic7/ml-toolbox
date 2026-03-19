import { useEffect, useRef } from "react";
import { useExecutionStore } from "../../store/executionStore";
import { useRunPipeline, useCancelPipeline } from "../../hooks/useExecution";

interface RunButtonProps {
  pipelineId: string;
  nodeIds: string[];
}

export default function RunButton({ pipelineId, nodeIds }: RunButtonProps) {
  const isRunning = useExecutionStore((s) => s.isRunning);
  const pendingNodeIds = useExecutionStore((s) => s.pendingNodeIds);
  const setRunning = useExecutionStore((s) => s.setRunning);
  const setCurrentNodeId = useExecutionStore((s) => s.setCurrentNodeId);

  const runMutation = useRunPipeline(pipelineId, nodeIds);
  const cancelMutation = useCancelPipeline(pipelineId);

  // Track initial pending count for progress calculation
  const initialCountRef = useRef(0);

  useEffect(() => {
    if (isRunning && pendingNodeIds.length > 0 && initialCountRef.current === 0) {
      initialCountRef.current = pendingNodeIds.length;
    }
    if (!isRunning) {
      initialCountRef.current = 0;
    }
  }, [isRunning, pendingNodeIds.length]);

  // Detect pipeline completion: store removes nodes from pendingNodeIds
  // when they reach terminal status, so length === 0 means all done.
  useEffect(() => {
    if (!isRunning || initialCountRef.current === 0) return;
    if (pendingNodeIds.length > 0) return;

    // All pending nodes have completed
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

  // Compute progress from how many nodes have been resolved
  const progress =
    isRunning && initialCountRef.current > 0
      ? (initialCountRef.current - pendingNodeIds.length) / initialCountRef.current
      : 0;

  return (
    <div className="flex items-center gap-2">
      {isRunning && (
        <span
          className="text-xs tabular-nums"
          style={{ color: "var(--text-secondary)" }}
        >
          {Math.round(progress * 100)}%
        </span>
      )}

      <button
        type="button"
        onClick={() =>
          isRunning ? cancelMutation.mutate() : runMutation.mutate()
        }
        disabled={!isRunning && nodeIds.length === 0}
        className="flex items-center gap-1.5 px-3 py-1 rounded-md text-sm font-medium text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        style={{
          backgroundColor: isRunning
            ? "var(--error-red)"
            : "var(--success-green)",
        }}
      >
        {isRunning ? (
          <>
            <svg
              width="14"
              height="14"
              viewBox="0 0 14 14"
              fill="currentColor"
            >
              <rect x="3" y="3" width="8" height="8" rx="1" />
            </svg>
            Cancel
          </>
        ) : (
          <>
            <svg
              width="14"
              height="14"
              viewBox="0 0 14 14"
              fill="currentColor"
            >
              <path d="M4 2.5v9l7-4.5-7-4.5z" />
            </svg>
            Run
          </>
        )}
      </button>
    </div>
  );
}
