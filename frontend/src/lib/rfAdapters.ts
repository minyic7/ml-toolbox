import type { Node, Edge } from "@xyflow/react";
import type {
  NodeInstance,
  Edge as PipelineEdge,
  NodeStatus,
  NodeDefinition,
  PortDefinition,
  ParamDefinition,
} from "./types";

export interface NodeCardData extends Record<string, unknown> {
  label: string;
  type: string;
  category: string;
  status: NodeStatus;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  params: ParamDefinition[] | Record<string, unknown>;
  code: string;
  onTabClick?: (nodeId: string, tab: string) => void;
  onRunFrom?: (nodeId: string) => void;
  onDeleteNode?: (nodeId: string) => void;
}

/**
 * Convert a backend NodeInstance to a React Flow Node.
 * Resolves `label` from `nodeDefinitions` using the node's type.
 */
export function toRFNode(
  node: NodeInstance,
  statuses: Record<string, NodeStatus>,
  nodeDefinitions: Record<string, NodeDefinition>,
  onTabClick?: (nodeId: string, tab: string) => void,
  onRunFrom?: (nodeId: string) => void,
  onDeleteNode?: (nodeId: string) => void,
): Node<NodeCardData> {
  const def = nodeDefinitions[node.type];
  // Derive category from definition, or from the node type path (e.g. "transform/clean" → "transform")
  const category = def?.category ?? node.type.split("/")[0] ?? "demo";
  return {
    id: node.id,
    type: "nodeCard",
    position: node.position,
    data: {
      label: node.name || def?.label || node.type,
      type: node.type,
      category,
      status: statuses[node.id] ?? "idle",
      inputs: node.inputs,
      outputs: node.outputs,
      params: node.params,
      code: node.code,
      onTabClick,
      onRunFrom,
      onDeleteNode,
    },
  };
}

/**
 * Convert a backend Edge to a React Flow Edge.
 */
export function toRFEdge(
  edge: PipelineEdge,
  onDeleteEdge?: (edgeId: string) => void,
  onPatchEdge?: (edgeId: string, condition: string) => void,
): Edge {
  return {
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: edge.source_port,
    targetHandle: edge.target_port,
    data: {
      condition: edge.condition,
      onDeleteEdge,
      onPatchEdge,
    },
  };
}
