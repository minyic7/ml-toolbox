import { useState, useEffect, useCallback, useRef } from "react";
import {
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type Connection,
  applyNodeChanges,
  applyEdgeChanges,
} from "@xyflow/react";
import type { NodeCardData, NodeTab } from "@/components/Canvas";
import type {
  Pipeline,
  PipelineNode,
  PipelineEdge,
  NodeDefinition,
} from "@/lib/types";
import * as api from "@/lib/api";
import { arePortTypesCompatible } from "@/components/Canvas";

// ── Helpers: convert between Pipeline types and React Flow types ─────

export function pipelineNodeToFlow(
  pn: PipelineNode,
  definitions: NodeDefinition[],
): Node<NodeCardData> | null {
  const def = definitions.find((d) => d.type === pn.type);
  if (!def) return null;
  return {
    id: pn.id,
    type: "nodeCard",
    position: pn.position,
    data: {
      label: def.label,
      definition: def,
      status: "idle",
      params: { ...pn.params },
      code: pn.code,
    },
  };
}

export function flowNodeToPipeline(node: Node<NodeCardData>): PipelineNode {
  return {
    id: node.id,
    type: node.data.definition.type,
    position: node.position,
    params: { ...node.data.params },
    code: node.data.code,
  };
}

export function pipelineEdgeToFlow(pe: PipelineEdge): Edge {
  return {
    id: pe.id,
    source: pe.source,
    sourceHandle: pe.source_port,
    target: pe.target,
    targetHandle: pe.target_port,
  };
}

export function flowEdgeToPipeline(edge: Edge): PipelineEdge {
  return {
    id: edge.id,
    source: edge.source,
    source_port: edge.sourceHandle ?? "",
    target: edge.target,
    target_port: edge.targetHandle ?? "",
  };
}

// ── Pipeline summary from list endpoint ─────────────────────────────

export interface PipelineSummary {
  id: string;
  name: string;
  node_count?: number;
}

// ── Toast message type ──────────────────────────────────────────────

export interface Toast {
  id: number;
  message: string;
  type: "error" | "info";
}

// ── Hook ────────────────────────────────────────────────────────────

let nodeIdCounter = 0;
let toastIdCounter = 0;

export function usePipeline(definitions: NodeDefinition[]) {
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([]);
  const [currentPipeline, setCurrentPipeline] = useState<Pipeline | null>(null);
  const [nodes, setNodes] = useState<Node<NodeCardData>[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | undefined>();
  const [selectedTab, setSelectedTab] = useState<NodeTab>("params");
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [loading, setLoading] = useState(true);

  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const nodesRef = useRef(nodes);
  const edgesRef = useRef(edges);
  nodesRef.current = nodes;
  edgesRef.current = edges;

  // ── Toast helpers ───────────────────────────────────────────────

  const addToast = useCallback((message: string, type: "error" | "info" = "error") => {
    const id = ++toastIdCounter;
    setToasts((prev) => [...prev, { id, message, type }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 4000);
  }, []);

  const dismissToast = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  // ── Debounced save ────────────────────────────────────────────

  const scheduleSave = useCallback(() => {
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(() => {
      const pipeline = currentPipeline;
      if (!pipeline) return;
      const pipelineNodes = nodesRef.current.map(flowNodeToPipeline);
      const pipelineEdges = edgesRef.current.map(flowEdgeToPipeline);
      api
        .savePipeline(pipeline.id, {
          name: pipeline.name,
          settings: pipeline.settings,
          nodes: pipelineNodes,
          edges: pipelineEdges,
        })
        .catch((err) => {
          addToast(`Save failed: ${err instanceof Error ? err.message : "Unknown error"}`);
        });
    }, 1000);
  }, [currentPipeline, addToast]);

  // ── Fetch pipeline list on mount ──────────────────────────────

  useEffect(() => {
    api
      .fetchPipelines()
      .then((list) => {
        setPipelines(list as unknown as PipelineSummary[]);
      })
      .catch((err) => {
        addToast(`Failed to load pipelines: ${err instanceof Error ? err.message : "Unknown error"}`);
      })
      .finally(() => setLoading(false));
  }, [addToast]);

  // ── CRUD operations ───────────────────────────────────────────

  const createPipeline = useCallback(
    async (name: string) => {
      try {
        const pipeline = await api.createPipeline({ name } as Partial<Pipeline>);
        setPipelines((prev) => [
          { id: pipeline.id, name: pipeline.name, node_count: 0 },
          ...prev,
        ]);
        setCurrentPipeline(pipeline);
        setNodes([]);
        setEdges([]);
        setSelectedNodeId(undefined);
      } catch (err) {
        addToast(`Create failed: ${err instanceof Error ? err.message : "Unknown error"}`);
      }
    },
    [addToast],
  );

  const loadPipeline = useCallback(
    async (id: string) => {
      try {
        const pipeline = await api.fetchPipeline(id);
        setCurrentPipeline(pipeline);
        const flowNodes = pipeline.nodes
          .map((pn) => pipelineNodeToFlow(pn, definitions))
          .filter((n): n is Node<NodeCardData> => n !== null);
        setNodes(flowNodes);
        setEdges(pipeline.edges.map(pipelineEdgeToFlow));
        setSelectedNodeId(undefined);
      } catch (err) {
        addToast(`Load failed: ${err instanceof Error ? err.message : "Unknown error"}`);
      }
    },
    [definitions, addToast],
  );

  const deletePipeline = useCallback(
    async (id: string) => {
      try {
        await api.deletePipeline(id);
        setPipelines((prev) => prev.filter((p) => p.id !== id));
        if (currentPipeline?.id === id) {
          setCurrentPipeline(null);
          setNodes([]);
          setEdges([]);
          setSelectedNodeId(undefined);
        }
      } catch (err) {
        addToast(`Delete failed: ${err instanceof Error ? err.message : "Unknown error"}`);
      }
    },
    [currentPipeline, addToast],
  );

  // ── React Flow change handlers (controlled mode) ──────────────

  const onNodesChange: OnNodesChange<Node<NodeCardData>> = useCallback(
    (changes) => {
      setNodes((nds) => applyNodeChanges(changes, nds));
      // Remove edges connected to deleted nodes
      const removedIds = changes
        .filter((c) => c.type === "remove")
        .map((c) => c.id);
      if (removedIds.length > 0) {
        setEdges((eds) =>
          eds.filter(
            (e) => !removedIds.includes(e.source) && !removedIds.includes(e.target),
          ),
        );
        if (selectedNodeId && removedIds.includes(selectedNodeId)) {
          setSelectedNodeId(undefined);
        }
      }
      scheduleSave();
    },
    [scheduleSave, selectedNodeId],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes) => {
      setEdges((eds) => applyEdgeChanges(changes, eds));
      scheduleSave();
    },
    [scheduleSave],
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      const sourceNode = nodesRef.current.find((n) => n.id === connection.source);
      const targetNode = nodesRef.current.find((n) => n.id === connection.target);

      const sourcePort = sourceNode?.data?.definition?.outputs?.find(
        (p) => p.name === connection.sourceHandle,
      );
      const targetPort = targetNode?.data?.definition?.inputs?.find(
        (p) => p.name === connection.targetHandle,
      );

      if (!arePortTypesCompatible(sourcePort?.type, targetPort?.type)) {
        addToast(
          `Cannot connect: port types do not match (${sourcePort?.type ?? "?"} ≠ ${targetPort?.type ?? "?"})`,
        );
        return;
      }

      const newEdge: Edge = {
        id: `e-${connection.source}-${connection.sourceHandle}-${connection.target}-${connection.targetHandle}`,
        source: connection.source!,
        sourceHandle: connection.sourceHandle,
        target: connection.target!,
        targetHandle: connection.targetHandle,
      };

      setEdges((eds) => [...eds, newEdge]);
      scheduleSave();
    },
    [addToast, scheduleSave],
  );

  // ── Add node (from sidebar drop) ──────────────────────────────

  const addNode = useCallback(
    (definition: NodeDefinition, position: { x: number; y: number }) => {
      nodeIdCounter += 1;
      const newNodeId = `node-${Date.now()}-${nodeIdCounter}`;

      const defaultParams: Record<string, string | number | boolean> = {};
      for (const p of definition.params) {
        defaultParams[p.name] = p.default;
      }

      const newNode: Node<NodeCardData> = {
        id: newNodeId,
        type: "nodeCard",
        position,
        data: {
          label: definition.label,
          definition,
          status: "idle",
          params: defaultParams,
        },
      };

      setNodes((nds) => [...nds, newNode]);
      scheduleSave();
    },
    [scheduleSave],
  );

  // ── Update node params/code ───────────────────────────────────

  const updateNodeData = useCallback(
    (nodeId: string, updates: Partial<Pick<NodeCardData, "params" | "code">>) => {
      setNodes((nds) =>
        nds.map((n) =>
          n.id === nodeId
            ? { ...n, data: { ...n.data, ...updates } }
            : n,
        ),
      );
      scheduleSave();
    },
    [scheduleSave],
  );

  // ── Node selection ────────────────────────────────────────────

  const selectNode = useCallback((nodeId?: string, tab?: NodeTab) => {
    setSelectedNodeId(nodeId);
    if (tab) setSelectedTab(tab);
  }, []);

  // Get the currently selected node
  const selectedNode = selectedNodeId
    ? nodes.find((n) => n.id === selectedNodeId) ?? null
    : null;

  return {
    // State
    pipelines,
    currentPipeline,
    nodes,
    edges,
    selectedNodeId,
    selectedTab,
    selectedNode,
    toasts,
    loading,

    // Pipeline CRUD
    createPipeline,
    loadPipeline,
    deletePipeline,

    // Canvas handlers
    onNodesChange,
    onEdgesChange,
    onConnect,
    addNode,

    // Node editing
    updateNodeData,
    selectNode,

    // Toast
    addToast,
    dismissToast,
  };
}
