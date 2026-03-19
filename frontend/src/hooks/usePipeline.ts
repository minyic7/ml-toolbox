import {
  useQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import type { Pipeline, PipelineListItem } from "../lib/types";
import {
  listPipelines,
  getPipeline,
  createPipeline,
  updatePipeline,
  deletePipeline,
  duplicatePipeline,
} from "../lib/api";

export function usePipelines() {
  return useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });
}

export function usePipeline(id: string) {
  return useQuery({
    queryKey: ["pipeline", id],
    queryFn: () => getPipeline(id),
    enabled: !!id,
  });
}

export function useCreatePipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => createPipeline({ name }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}

export function useUpdatePipeline(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Pipeline) => updatePipeline(id, body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipeline", id] });
      qc.invalidateQueries({ queryKey: ["pipelines"] });
    },
  });
}

export function useDeletePipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deletePipeline(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}

export function useDuplicatePipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => duplicatePipeline(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}
