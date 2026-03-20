import { useQuery } from "@tanstack/react-query";
import type { GlobalRunRecord, RunFilterParams } from "../lib/types";
import { listAllRuns } from "../lib/api";

export function useAllRuns(params?: RunFilterParams) {
  return useQuery<GlobalRunRecord[]>({
    queryKey: ["allRuns", params],
    queryFn: () => listAllRuns(params),
  });
}
