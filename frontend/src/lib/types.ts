// ── Port & Node Status ──────────────────────────────────────────────

export type PortType =
  | "TABLE"
  | "MODEL"
  | "METRICS"
  | "ARRAY"
  | "VALUE"
  | "TENSOR";

export type NodeStatus =
  | "idle"
  | "dirty"
  | "pending"
  | "running"
  | "done"
  | "error"
  | "skipped"
  | "cached";

// ── Param & Port Definitions ────────────────────────────────────────

export interface ParamDefinition {
  type: "select" | "slider" | "text" | "toggle";
  name: string;
  default: unknown;
  options?: string[];
  min?: number;
  max?: number;
  step?: number;
  description?: string;
  placeholder?: string;
}

export interface PortDefinition {
  name: string;
  type: PortType;
}

// ── Node Definition (GET /api/nodes) ────────────────────────────────

export interface NodeDefinition {
  type: string;
  label: string;
  category: string;
  description: string;
  guide: string;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  params: ParamDefinition[];
  default_code: string;
  allowed_upstream: string[];
}

// ── Pipeline types ──────────────────────────────────────────────────

export interface NodeInstance {
  id: string;
  seq?: number;
  type: string;
  name?: string | null;
  position: { x: number; y: number };
  params: ParamDefinition[];
  code: string;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
}

export interface Edge {
  id: string;
  source: string;
  source_port: string;
  target: string;
  target_port: string;
  condition?: string;
}

export interface PipelineSettings {
  keep_outputs: boolean;
}

export interface Pipeline {
  id: string;
  name: string;
  settings: PipelineSettings;
  nodes: NodeInstance[];
  edges: Edge[];
}

export interface PipelineListItem {
  id: string;
  name: string;
  node_count: number;
}

export interface PipelineSummary {
  id: string;
  name: string;
}

// ── Output & Run ────────────────────────────────────────────────────

export interface OutputPortPreview {
  file: string;
  type: string;
  size: number;
  port: string;
  preview: {
    columns?: string[];
    rows?: unknown[][];
    total_rows?: number;
    shape?: number[];
    dtype?: string;
    values?: number[];
    total_elements?: number;
    format?: string;
    file_size?: number;
  } | null;
}

export interface OutputPreview {
  node_id: string;
  file: string;
  type: string;
  size: number;
  preview: {
    columns?: string[];
    rows?: unknown[][];
    total_rows?: number;
    // ARRAY (.npy) previews
    shape?: number[];
    dtype?: string;
    values?: number[];
    total_elements?: number;
    // MODEL / TENSOR previews
    format?: string;
    file_size?: number;
  } | null;
  error: string | null;
  logs?: string | null;
  outputs?: OutputPortPreview[];
}

export interface RunInfo {
  id: string;
  started_at: string;
  completed_at: string | null;
  status: string;
  duration: number | null; // seconds
}

// ── Global Runs (cross-pipeline) ──────────────────────────────────

export interface DagNodeSnapshot {
  node_id: string;
  node_name: string;
  node_type: string;
  status: string; // "done" | "error" | "pending"
}

export interface RunArtifact {
  node_id: string;
  node_name: string;
  filename: string;
  type: string; // "parquet" | "pkl" | "json" | "npy" | "png" | "svg"
  size: number;
  bars?: number[];
}

export interface GlobalRunRecord {
  id: string;
  pipeline_id: string;
  pipeline_name: string;
  status: string; // "done" | "error" | "cancelled" | "unknown"
  started_at: string;
  completed_at: string | null;
  duration: number | null;
  dag_snapshot: DagNodeSnapshot[];
  artifacts: RunArtifact[];
}

export interface RunFilterParams {
  pipeline_id?: string;
  status?: string;
  search?: string;
  limit?: number;
  offset?: number;
}

// ── WebSocket ───────────────────────────────────────────────────────

export interface WsMessage {
  node_id?: string;
  status: string;
  run_id: string;
  outputs?: string[];
  cached?: boolean;
  traceback?: string | null;
}

// ── Request types ───────────────────────────────────────────────────

export interface CreatePipelineRequest {
  name: string;
}

export interface AddNodeRequest {
  type: string;
  position: { x: number; y: number };
  params?: ParamDefinition[] | Record<string, unknown>;
  code?: string;
  name?: string;
}

export interface PatchNodeRequest {
  params?: Record<string, unknown>;
  code?: string;
  position?: { x: number; y: number };
  name?: string;
}

export interface AddEdgeRequest {
  source: string;
  source_port: string;
  target: string;
  target_port: string;
  condition?: string;
}

export interface PatchEdgeRequest {
  condition?: string;
}
