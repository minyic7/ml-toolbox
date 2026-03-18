import type {
  NodeDefinition,
  Pipeline,
  PipelineNode,
  PipelineEdge,
  PipelineSettings,
  RunStatus,
} from "./types";

// Use Vite's BASE_URL (set by `base` in vite.config.ts) so API requests
// go through the correct sub-path (e.g. /ml-toolbox/api/...).
// Remove trailing slash to avoid double-slash in URLs.
const BASE = (import.meta.env.BASE_URL ?? "/").replace(/\/$/, "");

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

// ── Node library ────────────────────────────────────────────────────

export function fetchNodes(): Promise<NodeDefinition[]> {
  return request("/api/nodes");
}

export function fetchNode(type: string): Promise<NodeDefinition> {
  return request(`/api/nodes/${encodeURIComponent(type)}`);
}

// ── Pipeline CRUD ───────────────────────────────────────────────────

export function createPipeline(
  data?: Partial<Pipeline>,
): Promise<Pipeline> {
  return request("/api/pipelines", {
    method: "POST",
    body: JSON.stringify(data ?? {}),
  });
}

export function fetchPipelines(): Promise<Pipeline[]> {
  return request("/api/pipelines");
}

export function fetchPipeline(id: string): Promise<Pipeline> {
  return request(`/api/pipelines/${id}`);
}

export function savePipeline(
  id: string,
  data: Partial<Pipeline>,
): Promise<Pipeline> {
  return request(`/api/pipelines/${id}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
}

export function deletePipeline(id: string): Promise<void> {
  return request(`/api/pipelines/${id}`, { method: "DELETE" });
}

export function duplicatePipeline(id: string): Promise<Pipeline> {
  return request(`/api/pipelines/${id}/duplicate`, { method: "POST" });
}

// ── Node operations ─────────────────────────────────────────────────

export function addNode(
  pipelineId: string,
  node: Partial<PipelineNode>,
): Promise<PipelineNode> {
  return request(`/api/pipelines/${pipelineId}/nodes`, {
    method: "POST",
    body: JSON.stringify(node),
  });
}

export function removeNode(
  pipelineId: string,
  nodeId: string,
): Promise<void> {
  return request(`/api/pipelines/${pipelineId}/nodes/${nodeId}`, {
    method: "DELETE",
  });
}

export function updateNode(
  pipelineId: string,
  nodeId: string,
  data: Partial<PipelineNode>,
): Promise<PipelineNode> {
  return request(`/api/pipelines/${pipelineId}/nodes/${nodeId}`, {
    method: "PATCH",
    body: JSON.stringify(data),
  });
}

// ── Edge operations ─────────────────────────────────────────────────

export function addEdge(
  pipelineId: string,
  edge: Partial<PipelineEdge>,
): Promise<PipelineEdge> {
  return request(`/api/pipelines/${pipelineId}/edges`, {
    method: "POST",
    body: JSON.stringify(edge),
  });
}

export function removeEdge(
  pipelineId: string,
  edgeId: string,
): Promise<void> {
  return request(`/api/pipelines/${pipelineId}/edges/${edgeId}`, {
    method: "DELETE",
  });
}

export function updateEdge(
  pipelineId: string,
  edgeId: string,
  data: Partial<PipelineEdge>,
): Promise<PipelineEdge> {
  return request(`/api/pipelines/${pipelineId}/edges/${edgeId}`, {
    method: "PATCH",
    body: JSON.stringify(data),
  });
}

// ── Settings ────────────────────────────────────────────────────────

export function updateSettings(
  pipelineId: string,
  settings: Partial<PipelineSettings>,
): Promise<PipelineSettings> {
  return request(`/api/pipelines/${pipelineId}/settings`, {
    method: "PATCH",
    body: JSON.stringify(settings),
  });
}

// ── Execution ───────────────────────────────────────────────────────

export function runPipeline(id: string): Promise<{ run_id: string }> {
  return request(`/api/pipelines/${id}/run`, { method: "POST" });
}

export function runFromNode(
  id: string,
  nodeId: string,
): Promise<{ run_id: string }> {
  return request(`/api/pipelines/${id}/run/${nodeId}`, { method: "POST" });
}

export function cancelPipeline(id: string): Promise<void> {
  return request(`/api/pipelines/${id}/cancel`, { method: "POST" });
}

export function getPipelineStatus(
  id: string,
): Promise<{ status: RunStatus }> {
  return request(`/api/pipelines/${id}/status`);
}

// ── Runs ────────────────────────────────────────────────────────────

interface Run {
  id: string;
  status: RunStatus;
  started_at: string;
  finished_at?: string;
}

export function fetchRuns(pipelineId: string): Promise<Run[]> {
  return request(`/api/pipelines/${pipelineId}/runs`);
}

export function deleteRun(
  pipelineId: string,
  runId: string,
): Promise<void> {
  return request(`/api/pipelines/${pipelineId}/runs/${runId}`, {
    method: "DELETE",
  });
}

// ── Outputs ─────────────────────────────────────────────────────────

export function fetchOutput(
  pipelineId: string,
  nodeId: string,
  runId?: string,
): Promise<unknown> {
  const params = runId ? `?run_id=${runId}` : "";
  return request(`/api/pipelines/${pipelineId}/outputs/${nodeId}${params}`);
}

export function downloadOutput(
  pipelineId: string,
  nodeId: string,
  runId?: string,
): string {
  const params = runId ? `?run_id=${runId}` : "";
  return `${BASE}/api/pipelines/${pipelineId}/outputs/${nodeId}/download${params}`;
}
