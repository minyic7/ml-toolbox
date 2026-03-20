import type {
  NodeDefinition,
  Pipeline,
  PipelineListItem,
  PipelineSummary,
  PipelineSettings,
  NodeInstance,
  Edge,
  OutputPreview,
  RunInfo,
  GlobalRunRecord,
  RunFilterParams,
  CreatePipelineRequest,
  AddNodeRequest,
  PatchNodeRequest,
  AddEdgeRequest,
  PatchEdgeRequest,
} from "./types";

// ── Helpers ─────────────────────────────────────────────────────────

const basePath = import.meta.env.BASE_URL.replace(/\/$/, "");

async function request<T>(
  url: string,
  init?: RequestInit,
): Promise<T> {
  const res = await fetch(`${basePath}${url}`, init);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

function json(body: unknown): RequestInit {
  return {
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

// ── Health ──────────────────────────────────────────────────────────

export function getHealth() {
  return request<{ status: string }>("/api/health");
}

// ── Node definitions ────────────────────────────────────────────────

export function getNodeDefinitions() {
  return request<NodeDefinition[]>("/api/nodes");
}

export function getNodeDefinition(nodeType: string) {
  return request<NodeDefinition>(`/api/nodes/${encodeURIComponent(nodeType)}`);
}

// ── Pipelines CRUD ──────────────────────────────────────────────────

export function listPipelines() {
  return request<PipelineListItem[]>("/api/pipelines");
}

export async function getPipeline(pipelineId: string) {
  const pipeline = await request<Pipeline>(`/api/pipelines/${pipelineId}`);
  // Normalize params: backend may return dict format for legacy data.
  // Convert to ParamDefinition[] so the rest of the frontend can assume arrays.
  for (const node of pipeline.nodes) {
    if (!Array.isArray(node.params)) {
      const dict = node.params as Record<string, unknown>;
      node.params = Object.entries(dict).map(([name, value]) => ({
        type: "text" as const,
        name,
        default: value,
      }));
    }
  }
  return pipeline;
}

export function createPipeline(body: CreatePipelineRequest) {
  return request<PipelineSummary>("/api/pipelines", {
    method: "POST",
    ...json(body),
  });
}

export function updatePipeline(pipelineId: string, body: Pipeline) {
  return request<Pipeline>(`/api/pipelines/${pipelineId}`, {
    method: "PUT",
    ...json(body),
  });
}

export function deletePipeline(pipelineId: string) {
  return request<void>(`/api/pipelines/${pipelineId}`, {
    method: "DELETE",
  });
}

export function duplicatePipeline(pipelineId: string) {
  return request<PipelineSummary>(
    `/api/pipelines/${pipelineId}/duplicate`,
    { method: "POST" },
  );
}

// ── Pipeline settings ───────────────────────────────────────────────

export function patchPipelineSettings(
  pipelineId: string,
  body: Partial<PipelineSettings>,
) {
  return request<PipelineSettings>(
    `/api/pipelines/${pipelineId}/settings`,
    { method: "PATCH", ...json(body) },
  );
}

// ── Nodes ───────────────────────────────────────────────────────────

export function addNode(pipelineId: string, body: AddNodeRequest) {
  return request<NodeInstance>(
    `/api/pipelines/${pipelineId}/nodes`,
    { method: "POST", ...json(body) },
  );
}

export function deleteNode(pipelineId: string, nodeId: string) {
  return request<void>(
    `/api/pipelines/${pipelineId}/nodes/${nodeId}`,
    { method: "DELETE" },
  );
}

export function patchNode(
  pipelineId: string,
  nodeId: string,
  body: PatchNodeRequest,
) {
  return request<NodeInstance>(
    `/api/pipelines/${pipelineId}/nodes/${nodeId}`,
    { method: "PATCH", ...json(body) },
  );
}

// ── Edges ───────────────────────────────────────────────────────────

export function addEdge(pipelineId: string, body: AddEdgeRequest) {
  return request<Edge>(
    `/api/pipelines/${pipelineId}/edges`,
    { method: "POST", ...json(body) },
  );
}

export function deleteEdge(pipelineId: string, edgeId: string) {
  return request<void>(
    `/api/pipelines/${pipelineId}/edges/${edgeId}`,
    { method: "DELETE" },
  );
}

export function patchEdge(
  pipelineId: string,
  edgeId: string,
  body: PatchEdgeRequest,
) {
  return request<Edge>(
    `/api/pipelines/${pipelineId}/edges/${edgeId}`,
    { method: "PATCH", ...json(body) },
  );
}

// ── Execution ───────────────────────────────────────────────────────

export function runPipeline(pipelineId: string) {
  return request<{ run_id: string }>(
    `/api/pipelines/${pipelineId}/run`,
    { method: "POST" },
  );
}

export function runFromNode(pipelineId: string, nodeId: string) {
  return request<{ run_id: string }>(
    `/api/pipelines/${pipelineId}/run/${nodeId}`,
    { method: "POST" },
  );
}

export function cancelPipeline(pipelineId: string) {
  return request<{ status: string }>(
    `/api/pipelines/${pipelineId}/cancel`,
    { method: "POST" },
  );
}

export function getPipelineStatus(pipelineId: string) {
  return request<{
    is_running: boolean;
    current_node_id: string | null;
    last_run_id: string | null;
  }>(`/api/pipelines/${pipelineId}/status`);
}

// ── Runs ────────────────────────────────────────────────────────────

export function listRuns(pipelineId: string) {
  return request<RunInfo[]>(`/api/pipelines/${pipelineId}/runs`);
}

export function listAllRuns(params?: RunFilterParams) {
  const qs = new URLSearchParams();
  if (params?.pipeline_id) qs.set("pipeline_id", params.pipeline_id);
  if (params?.status) qs.set("status", params.status);
  if (params?.search) qs.set("search", params.search);
  if (params?.limit) qs.set("limit", String(params.limit));
  if (params?.offset) qs.set("offset", String(params.offset));
  const query = qs.toString();
  return request<GlobalRunRecord[]>(`/api/runs${query ? `?${query}` : ""}`);
}

export function deleteRun(pipelineId: string, runId: string) {
  return request<void>(
    `/api/pipelines/${pipelineId}/runs/${runId}`,
    { method: "DELETE" },
  );
}

// ── Outputs ─────────────────────────────────────────────────────────

export function getOutput(
  pipelineId: string,
  nodeId: string,
  runId?: string,
) {
  const params = runId ? `?run_id=${encodeURIComponent(runId)}` : "";
  return request<OutputPreview>(
    `/api/pipelines/${pipelineId}/outputs/${nodeId}${params}`,
  );
}

export function getOutputDownloadUrl(
  pipelineId: string,
  nodeId: string,
  runId?: string,
) {
  const params = runId ? `?run_id=${encodeURIComponent(runId)}` : "";
  return `${basePath}/api/pipelines/${pipelineId}/outputs/${nodeId}/download${params}`;
}

export function getRunOutput(
  pipelineId: string,
  runId: string,
  nodeId: string,
) {
  return request<OutputPreview>(
    `/api/pipelines/${pipelineId}/runs/${runId}/outputs/${nodeId}`,
  );
}

export function getRunOutputDownloadUrl(
  pipelineId: string,
  runId: string,
  nodeId: string,
) {
  return `${basePath}/api/pipelines/${pipelineId}/runs/${runId}/outputs/${nodeId}/download`;
}
