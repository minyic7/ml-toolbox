import { useEffect, useCallback } from "react";
import { useExecutionStore, isTerminalStatus } from "../../store/executionStore";
import { runPipeline, cancelPipeline } from "../../lib/api";

interface RunButtonProps {
  pipelineId: string;
  nodeIds: string[];
}

export default function RunButton({ pipelineId, nodeIds }: RunButtonProps) {
  const isRunning = useExecutionStore((s) => s.isRunning);
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);
  const pendingNodeIds = useExecutionStore((s) => s.pendingNodeIds);
  const setRunning = useExecutionStore((s) => s.setRunning);
  const setRunId = useExecutionStore((s) => s.setRunId);
  const setPendingNodeIds = useExecutionStore((s) => s.setPendingNodeIds);
  const setNodeStatus = useExecutionStore((s) => s.setNodeStatus);

  // Detect pipeline completion: all pending nodes have terminal status
  useEffect(() => {
    if (!isRunning || pendingNodeIds.length === 0) return;

    const allDone = pendingNodeIds.every((id) => {
      const status = nodeStatuses[id];
      return status !== undefined && isTerminalStatus(status);
    });

    if (allDone) {
      setRunning(false);

      const hasError = pendingNodeIds.some(
        (id) => nodeStatuses[id] === "error",
      );

      // Log completion status (toast integration point)
      if (hasError) {
        console.warn("Pipeline finished with errors");
      } else {
        console.info("Pipeline completed successfully");
      }
    }
  }, [isRunning, pendingNodeIds, nodeStatuses, setRunning]);

  const handleRun = useCallback(async () => {
    try {
      // Mark all nodes as pending
      for (const id of nodeIds) {
        setNodeStatus(id, "pending");
      }
      setPendingNodeIds(nodeIds);
      setRunning(true);

      const result = await runPipeline(pipelineId);
      setRunId(result.run_id);
    } catch (err) {
      console.error("Failed to start pipeline:", err);
      setRunning(false);
      setPendingNodeIds([]);
    }
  }, [pipelineId, nodeIds, setNodeStatus, setPendingNodeIds, setRunning, setRunId]);

  const handleCancel = useCallback(async () => {
    try {
      await cancelPipeline(pipelineId);
    } catch (err) {
      console.error("Failed to cancel pipeline:", err);
    }
  }, [pipelineId]);

  // Compute progress
  const progress =
    isRunning && pendingNodeIds.length > 0
      ? pendingNodeIds.filter((id) => {
          const s = nodeStatuses[id];
          return s !== undefined && isTerminalStatus(s);
        }).length / pendingNodeIds.length
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
        onClick={isRunning ? handleCancel : handleRun}
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
