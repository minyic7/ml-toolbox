import { useState, useCallback, useRef } from "react";
import type { NodeStatus, WsMessage, NodeOutputState } from "@/lib/types";
import * as api from "@/lib/api";

export interface ExecutionStatus {
  isRunning: boolean;
  currentNodeId: string | undefined;
  lastRunId: string | undefined;
}

export interface UseExecutionReturn {
  /** Current execution status */
  status: ExecutionStatus;
  /** Per-node status map (node_id → NodeStatus) */
  nodeStatuses: Record<string, NodeStatus>;
  /** Per-node output state (node_id → output/error) */
  nodeOutputs: Record<string, NodeOutputState>;
  /** Run all nodes in the pipeline */
  runAll: () => Promise<void>;
  /** Run from a specific node */
  runFrom: (nodeId: string) => Promise<void>;
  /** Cancel the current execution */
  cancel: () => Promise<void>;
  /** Handle an incoming WebSocket message (call from useWebSocket) */
  handleWsMessage: (msg: WsMessage) => void;
  /** Reset all node statuses to idle */
  resetStatuses: () => void;
}

export function useExecution(
  pipelineId: string | undefined,
  nodeIds: string[],
  onError?: (message: string) => void,
): UseExecutionReturn {
  const [status, setStatus] = useState<ExecutionStatus>({
    isRunning: false,
    currentNodeId: undefined,
    lastRunId: undefined,
  });
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, NodeStatus>>({});
  const [nodeOutputs, setNodeOutputs] = useState<Record<string, NodeOutputState>>({});

  const pipelineIdRef = useRef(pipelineId);
  pipelineIdRef.current = pipelineId;

  const resetStatuses = useCallback(() => {
    setNodeStatuses({});
    setNodeOutputs({});
  }, []);

  const runAll = useCallback(async () => {
    if (!pipelineIdRef.current) return;
    try {
      // Reset all statuses before starting
      setNodeStatuses({});
      setNodeOutputs({});
      const { run_id } = await api.runPipeline(pipelineIdRef.current);
      setStatus({ isRunning: true, currentNodeId: undefined, lastRunId: run_id });
    } catch (err) {
      onError?.(`Run failed: ${err instanceof Error ? err.message : "Unknown error"}`);
    }
  }, [onError]);

  const runFrom = useCallback(
    async (nodeId: string) => {
      if (!pipelineIdRef.current) return;
      try {
        setNodeStatuses({});
        setNodeOutputs({});
        const { run_id } = await api.runFromNode(pipelineIdRef.current, nodeId);
        setStatus({ isRunning: true, currentNodeId: undefined, lastRunId: run_id });
      } catch (err) {
        onError?.(`Run failed: ${err instanceof Error ? err.message : "Unknown error"}`);
      }
    },
    [onError],
  );

  const cancel = useCallback(async () => {
    if (!pipelineIdRef.current) return;
    try {
      await api.cancelPipeline(pipelineIdRef.current);
    } catch (err) {
      onError?.(`Cancel failed: ${err instanceof Error ? err.message : "Unknown error"}`);
    }
  }, [onError]);

  const handleWsMessage = useCallback(
    (msg: WsMessage) => {
      // Update per-node status
      setNodeStatuses((prev) => ({ ...prev, [msg.node_id]: msg.status }));

      // Track currently running node
      if (msg.status === "running") {
        setStatus((prev) => ({ ...prev, currentNodeId: msg.node_id }));
      }

      // On completion, fetch output
      if (msg.status === "done" && pipelineIdRef.current) {
        const pid = pipelineIdRef.current;
        api
          .fetchOutput(pid, msg.node_id)
          .then((data) => {
            setNodeOutputs((prev) => ({
              ...prev,
              [msg.node_id]: { output: data as NodeOutputState["output"] },
            }));
          })
          .catch(() => {
            // Output fetch failed — not critical
          });
      }

      // On error, store traceback
      if (msg.status === "error") {
        setNodeOutputs((prev) => ({
          ...prev,
          [msg.node_id]: { error: msg.traceback ?? "Unknown error" },
        }));
      }

      // Detect execution end: check if this is a terminal status and no nodes are still running
      if (msg.status === "done" || msg.status === "error" || msg.status === "skipped") {
        setNodeStatuses((current) => {
          const allTerminal = nodeIds.every((id) => {
            const s = id === msg.node_id ? msg.status : current[id];
            return s === "done" || s === "error" || s === "skipped" || s === undefined;
          });
          if (allTerminal) {
            // Use setTimeout to avoid updating status during nodeStatuses render
            setTimeout(() => {
              setStatus((prev) => ({ ...prev, isRunning: false, currentNodeId: undefined }));
            }, 0);
          }
          return current;
        });
      }
    },
    [nodeIds],
  );

  return {
    status,
    nodeStatuses,
    nodeOutputs,
    runAll,
    runFrom,
    cancel,
    handleWsMessage,
    resetStatuses,
  };
}
