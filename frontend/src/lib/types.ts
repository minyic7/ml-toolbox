// ── Port & Param types ──────────────────────────────────────────────

export enum PortType {
  TABLE = "TABLE",
  MODEL = "MODEL",
  METRICS = "METRICS",
  ARRAY = "ARRAY",
  VALUE = "VALUE",
  TENSOR = "TENSOR",
}

export const PORT_COLORS: Record<PortType, string> = {
  [PortType.TABLE]: "#9CA3AF",
  [PortType.MODEL]: "#22C55E",
  [PortType.METRICS]: "#EAB308",
  [PortType.ARRAY]: "#3B82F6",
  [PortType.VALUE]: "#A855F7",
  [PortType.TENSOR]: "#F97316",
};

export type ParamType = "select" | "slider" | "text" | "toggle";

// ── Node definitions (library) ──────────────────────────────────────

export interface NodePort {
  name: string;
  type: PortType;
}

export interface NodeParam {
  name: string;
  type: ParamType;
  default: string | number | boolean;
  options?: string[];
  min?: number;
  max?: number;
  step?: number;
}

export interface NodeDefinition {
  type: string;
  label: string;
  category: string;
  description: string;
  inputs: NodePort[];
  outputs: NodePort[];
  params: NodeParam[];
  default_code?: string;
}

// ── Pipeline ────────────────────────────────────────────────────────

export interface PipelineNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  params: Record<string, string | number | boolean>;
  code?: string;
}

export interface PipelineEdge {
  id: string;
  source: string;
  source_port: string;
  target: string;
  target_port: string;
  condition?: string | null;
}

export interface PipelineSettings {
  keep_outputs: boolean;
}

export interface Pipeline {
  id: string;
  name: string;
  settings: PipelineSettings;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}

// ── Execution ───────────────────────────────────────────────────────

export type RunStatus = "idle" | "running" | "done" | "error" | "cancelled";
export type NodeStatus = "pending" | "running" | "done" | "error" | "skipped";

export interface WsMessage {
  node_id: string;
  status: NodeStatus;
  outputs?: Record<string, string>;
  traceback?: string;
}
