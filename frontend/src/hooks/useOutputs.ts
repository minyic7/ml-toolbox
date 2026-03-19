import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import type { OutputPreview, RunInfo } from "../lib/types";
import { getOutput, listRuns, deleteRun } from "../lib/api";

export function useOutput(
  pipelineId: string,
  nodeId: string,
  runId?: string,
) {
  return useQuery<OutputPreview>({
    queryKey: ["output", pipelineId, nodeId, runId],
    queryFn: () => getOutput(pipelineId, nodeId, runId),
    enabled: !!pipelineId && !!nodeId,
  });
}

export function useRuns(pipelineId: string) {
  return useQuery<RunInfo[]>({
    queryKey: ["runs", pipelineId],
    queryFn: () => listRuns(pipelineId),
    enabled: !!pipelineId,
  });
}

export function useDeleteRun(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (runId: string) => deleteRun(pipelineId, runId),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["runs", pipelineId] }),
  });
}
