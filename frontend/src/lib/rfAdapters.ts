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
  status: NodeStatus;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  params: ParamDefinition[];
  code: string;
}

/**
 * Convert a backend NodeInstance to a React Flow Node.
 * Resolves `label` from `nodeDefinitions` using the node's type.
 */
export function toRFNode(
  node: NodeInstance,
  statuses: Record<string, NodeStatus>,
  nodeDefinitions: Record<string, NodeDefinition>,
): Node<NodeCardData> {
  const def = nodeDefinitions[node.type];
  return {
    id: node.id,
    type: "nodeCard",
    position: node.position,
    data: {
      label: def?.label ?? node.type,
      type: node.type,
      status: statuses[node.id] ?? "idle",
      inputs: node.inputs,
      outputs: node.outputs,
      params: node.params,
      code: node.code,
    },
  };
}

/**
 * Convert a backend Edge to a React Flow Edge.
 */
export function toRFEdge(
  edge: PipelineEdge,
  onDeleteEdge?: (edgeId: string) => void,
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
    },
  };
}
