import { useCallback, useRef, useState, useEffect } from "react";
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useReactFlow,
  ReactFlowProvider,
  applyNodeChanges,
  applyEdgeChanges,
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  type Connection,
  type NodeTypes,
  type EdgeTypes,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { NodeCard, type NodeCardData } from "./NodeCard";
import { type PortType, type NodeDefinition } from "@/lib/types";

// ── Port-type validation (pure function) ─────────────────────────────

export function arePortTypesCompatible(
  sourceType: PortType | undefined,
  targetType: PortType | undefined,
): boolean {
  if (!sourceType || !targetType) return false;
  return sourceType === targetType;
}

// ── Custom node / edge types ─────────────────────────────────────────

const nodeTypes: NodeTypes = {
  nodeCard: NodeCard,
};

// Placeholder edge type for conditional edges (T12 will implement fully)
const edgeTypes: EdgeTypes = {};

// ── History for undo/redo ────────────────────────────────────────────

interface CanvasSnapshot {
  nodes: Node<NodeCardData>[];
  edges: Edge[];
}

function useHistory(initial: CanvasSnapshot) {
  const stack = useRef<CanvasSnapshot[]>([initial]);
  const pointer = useRef(0);

  const push = useCallback((snapshot: CanvasSnapshot) => {
    stack.current = stack.current.slice(0, pointer.current + 1);
    stack.current.push(snapshot);
    pointer.current = stack.current.length - 1;
  }, []);

  const undo = useCallback((): CanvasSnapshot | null => {
    if (pointer.current > 0) {
      pointer.current -= 1;
      return stack.current[pointer.current] ?? null;
    }
    return null;
  }, []);

  const redo = useCallback((): CanvasSnapshot | null => {
    if (pointer.current < stack.current.length - 1) {
      pointer.current += 1;
      return stack.current[pointer.current] ?? null;
    }
    return null;
  }, []);

  return { push, undo, redo };
}

// ── Canvas inner (needs ReactFlowProvider) ───────────────────────────

export interface CanvasProps {
  nodes?: Node<NodeCardData>[];
  edges?: Edge[];
  onNodesChange?: OnNodesChange<Node<NodeCardData>>;
  onEdgesChange?: OnEdgesChange;
  onConnect?: OnConnect;
  onNodeSelect?: (nodeId?: string, tab?: string) => void;
}

let nodeIdCounter = 0;

function CanvasInner({
  nodes: controlledNodes,
  edges: controlledEdges,
  onNodesChange: controlledOnNodesChange,
  onEdgesChange: controlledOnEdgesChange,
  onConnect: controlledOnConnect,
  onNodeSelect,
}: CanvasProps) {
  const isControlled = controlledNodes !== undefined;

  const [internalNodes, setInternalNodes] = useState<Node<NodeCardData>[]>([]);
  const [internalEdges, setInternalEdges] = useState<Edge[]>([]);

  const nodes = isControlled ? controlledNodes : internalNodes;
  const edges = isControlled ? (controlledEdges ?? []) : internalEdges;

  const { screenToFlowPosition } = useReactFlow();
  const history = useHistory({ nodes: [], edges: [] });

  // Push snapshot to history for undo
  const pushHistory = useCallback(
    (n: Node<NodeCardData>[], e: Edge[]) => {
      history.push({ nodes: n, edges: e });
    },
    [history],
  );

  // ── Node / edge change handlers ──────────────────────────────────

  const onNodesChange: OnNodesChange<Node<NodeCardData>> = useCallback(
    (changes) => {
      if (isControlled) {
        controlledOnNodesChange?.(changes);
        return;
      }
      setInternalNodes((nds) => {
        const next = applyNodeChanges(changes, nds);
        // Push history on remove
        const hasRemove = changes.some((c) => c.type === "remove");
        if (hasRemove) {
          // edges may also change, but we push current edges
          pushHistory(next, internalEdges);
        }
        return next;
      });
    },
    [isControlled, controlledOnNodesChange, pushHistory, internalEdges],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes) => {
      if (isControlled) {
        controlledOnEdgesChange?.(changes);
        return;
      }
      setInternalEdges((eds) => {
        const next = applyEdgeChanges(changes, eds);
        const hasRemove = changes.some((c) => c.type === "remove");
        if (hasRemove) {
          pushHistory(internalNodes, next);
        }
        return next;
      });
    },
    [isControlled, controlledOnEdgesChange, pushHistory, internalNodes],
  );

  // ── Connect handler with port-type validation ────────────────────

  const onConnect: OnConnect = useCallback(
    (connection: Connection) => {
      if (isControlled) {
        controlledOnConnect?.(connection);
        return;
      }

      const sourceNode = internalNodes.find((n) => n.id === connection.source);
      const targetNode = internalNodes.find((n) => n.id === connection.target);

      const sourcePort = sourceNode?.data?.definition?.outputs?.find(
        (p) => p.name === connection.sourceHandle,
      );
      const targetPort = targetNode?.data?.definition?.inputs?.find(
        (p) => p.name === connection.targetHandle,
      );

      if (!arePortTypesCompatible(sourcePort?.type, targetPort?.type)) {
        // Flash the target handle red
        const handleEl = document.querySelector(
          `.react-flow__handle[data-handleid="${connection.targetHandle}"][data-nodeid="${connection.target}"]`,
        );
        if (handleEl) {
          handleEl.classList.add("handle-flash-red");
          setTimeout(() => handleEl.classList.remove("handle-flash-red"), 600);
        }
        return;
      }

      const newEdge: Edge = {
        id: `e-${connection.source}-${connection.sourceHandle}-${connection.target}-${connection.targetHandle}`,
        source: connection.source!,
        sourceHandle: connection.sourceHandle,
        target: connection.target!,
        targetHandle: connection.targetHandle,
      };

      setInternalEdges((eds) => {
        const next = [...eds, newEdge];
        pushHistory(internalNodes, next);
        return next;
      });
    },
    [isControlled, controlledOnConnect, internalNodes, pushHistory],
  );

  // ── Node drag end → push history ─────────────────────────────────

  const onNodeDragStop = useCallback(() => {
    if (!isControlled) {
      // Defer to capture the updated node positions
      setTimeout(() => {
        setInternalNodes((nds) => {
          pushHistory(nds, internalEdges);
          return nds;
        });
      }, 0);
    }
  }, [isControlled, pushHistory, internalEdges]);

  // ── Tab click handler (passed to each node via data) ─────────────

  const handleTabClick = useCallback(
    (nodeId: string, tab: string) => {
      onNodeSelect?.(nodeId, tab);
    },
    [onNodeSelect],
  );

  // Inject onTabClick into node data
  const nodesWithCallbacks = nodes.map((node) => ({
    ...node,
    data: {
      ...node.data,
      onTabClick: handleTabClick,
    },
  }));

  // ── Keyboard shortcuts ───────────────────────────────────────────

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Undo: Ctrl+Z (without shift)
      if (e.key === "z" && (e.ctrlKey || e.metaKey) && !e.shiftKey) {
        e.preventDefault();
        const snapshot = history.undo();
        if (snapshot && !isControlled) {
          setInternalNodes(snapshot.nodes);
          setInternalEdges(snapshot.edges);
        }
      }
      // Redo: Ctrl+Shift+Z
      if (e.key === "z" && (e.ctrlKey || e.metaKey) && e.shiftKey) {
        e.preventDefault();
        const snapshot = history.redo();
        if (snapshot && !isControlled) {
          setInternalNodes(snapshot.nodes);
          setInternalEdges(snapshot.edges);
        }
      }
    };

    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [history, isControlled]);

  // ── Drag-and-drop from sidebar ───────────────────────────────────

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const raw = event.dataTransfer.getData("application/reactflow");
      if (!raw) return;

      let definition: NodeDefinition;
      try {
        definition = JSON.parse(raw);
      } catch {
        return;
      }

      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      nodeIdCounter += 1;
      const newNodeId = `node-${Date.now()}-${nodeIdCounter}`;

      // Build default params from definition
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

      if (isControlled) {
        // In controlled mode, the parent handles adding nodes
        return;
      }

      setInternalNodes((nds) => {
        const next = [...nds, newNode];
        pushHistory(next, internalEdges);
        return next;
      });
    },
    [screenToFlowPosition, isControlled, pushHistory, internalEdges],
  );

  return (
    <div className="h-full w-full" onDragOver={onDragOver} onDrop={onDrop}>
      <ReactFlow
        nodes={nodesWithCallbacks}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeDragStop={onNodeDragStop}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        deleteKeyCode="Delete"
        fitView
        className="bg-[#1a1a1e]"
      >
        <Background color="#333" gap={20} />
        <Controls className="!bg-[#27272a] !border-[#3f3f46] !rounded-lg [&>button]:!bg-[#27272a] [&>button]:!border-[#3f3f46] [&>button]:!fill-[#a1a1aa] [&>button:hover]:!bg-[#3f3f46]" />
        <MiniMap
          position="bottom-right"
          className="!bg-[#27272a] !border-[#3f3f46] !rounded-lg"
          maskColor="rgba(0,0,0,0.5)"
          nodeColor="#3B82F6"
        />
      </ReactFlow>
    </div>
  );
}

// ── Wrapped export with ReactFlowProvider ────────────────────────────

export function Canvas(props: CanvasProps) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}
