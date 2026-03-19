import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { AddNodeRequest, PatchNodeRequest } from "../lib/types";
import { addNode, deleteNode, patchNode } from "../lib/api";

export function useAddNode(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: AddNodeRequest) => addNode(pipelineId, body),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
  });
}

export function useDeleteNode(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (nodeId: string) => deleteNode(pipelineId, nodeId),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
  });
}

export function usePatchNode(pipelineId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ nodeId, body }: { nodeId: string; body: PatchNodeRequest }) =>
      patchNode(pipelineId, nodeId, body),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
  });
}
