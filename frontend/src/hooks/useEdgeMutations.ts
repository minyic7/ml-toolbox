import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { AddEdgeRequest, PatchEdgeRequest } from "../lib/types";
import { addEdge, deleteEdge, patchEdge } from "../lib/api";

export function useAddEdge(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: AddEdgeRequest) => addEdge(pipelineId, body),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
  });
}

export function useDeleteEdge(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (edgeId: string) => deleteEdge(pipelineId, edgeId),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
  });
}

export function usePatchEdge(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ edgeId, body }: { edgeId: string; body: PatchEdgeRequest }) =>
      patchEdge(pipelineId, edgeId, body),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
  });
}
