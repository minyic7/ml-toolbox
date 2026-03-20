import { useMutation, useQuery } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  runPipeline,
  runFromNode,
  cancelPipeline,
  getPipelineStatus,
} from "../lib/api";
import { useExecutionStore } from "../store/executionStore";

export function useRunPipeline(pipelineId: string, nodeIds: string[]) {
  const { setRunning, setAllPending, setRunId } = useExecutionStore();

  return useMutation({
    mutationFn: () => runPipeline(pipelineId),
    onSuccess: (data) => {
      setAllPending(nodeIds);
      setRunning(true);
      setRunId(data.run_id);
    },
    onError: (err) => {
      const msg = err instanceof Error ? err.message : "";
      toast.error(msg.includes("409") ? "Already running" : "Failed to start run");
    },
  });
}

export function useRunFromNode(pipelineId: string, downstreamNodeIds: string[]) {
  const { setRunning, setAllPending, setRunId } = useExecutionStore();

  return useMutation({
    mutationFn: (nodeId: string) => runFromNode(pipelineId, nodeId),
    onSuccess: (data) => {
      setAllPending(downstreamNodeIds);
      setRunning(true);
      setRunId(data.run_id);
    },
    onError: (err) => {
      const msg = err instanceof Error ? err.message : "";
      toast.error(msg.includes("409") ? "Already running" : "Failed to start run");
    },
  });
}

export function useCancelPipeline(pipelineId: string) {
  const { setRunning } = useExecutionStore();

  return useMutation({
    mutationFn: () => cancelPipeline(pipelineId),
    onSuccess: () => setRunning(false),
  });
}

export function usePipelineStatus(pipelineId: string) {
  return useQuery({
    queryKey: ["pipelineStatus", pipelineId],
    queryFn: () => getPipelineStatus(pipelineId),
    enabled: !!pipelineId,
  });
}
