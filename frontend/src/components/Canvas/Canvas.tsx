import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  ReactFlow,
  Background,
  Controls,
  useReactFlow,
  useNodesState,
  useEdgesState,
  type Connection,
  type NodeChange,
  type EdgeChange,
  type Node,
  type Edge as RFEdge,
  type OnConnectStartParams,
  type IsValidConnection,
  ReactFlowProvider,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import type {
  NodeDefinition,
  NodeInstance,
  Edge as PipelineEdge,
  PortType,
} from "../../lib/types";
import type { NodeCardData } from "../../lib/rfAdapters";
import { toRFNode, toRFEdge } from "../../lib/rfAdapters";
import { PORT_COLORS } from "../../lib/portColors";
import { useExecutionStore } from "../../store/executionStore";

import NodeCard from "./NodeCard";
import EdgeWithCondition from "./EdgeWithCondition";
import ContextMenu from "./ContextMenu";
import CanvasContextMenu from "./CanvasContextMenu";
import ShortcutModal from "./ShortcutModal";

// ── Types ──────────────────────────────────────────────────────────

type RFNode = Node<NodeCardData>;

interface CanvasProps {
  pipelineId: string;
  pipelineNodes: NodeInstance[];
  pipelineEdges: PipelineEdge[];
  nodeDefinitions: Record<string, NodeDefinition>;
  onNodePositionChange: (nodeId: string, position: { x: number; y: number }) => void;
  onConnect: (connection: {
    source: string;
    sourcePort: string;
    target: string;
    targetPort: string;
  }) => void;
  onDeleteNode: (nodeId: string) => void;
  onDeleteEdge: (edgeId: string) => void;
  onDropNode: (type: string, position: { x: number; y: number }) => void;
  onRunFrom: (nodeId: string) => void;
  onNodeSelect?: (nodeId: string | null) => void;
}

// ── Constants ──────────────────────────────────────────────────────

const POSITION_DEBOUNCE_MS = 300;

const nodeTypes = { nodeCard: NodeCard };
const edgeTypes = { default: EdgeWithCondition };

// ── Undo Toast ─────────────────────────────────────────────────────

interface UndoToastProps {
  message: string;
  onUndo: () => void;
  onDismiss: () => void;
}

function UndoToast({ message, onUndo, onDismiss }: UndoToastProps) {
  useEffect(() => {
    const timer = setTimeout(onDismiss, 5000);
    return () => clearTimeout(timer);
  }, [onDismiss]);

  return (
    <div
      style={{
        position: "fixed",
        bottom: 24,
        left: "50%",
        transform: "translateX(-50%)",
        zIndex: 60,
        background: "var(--text-primary)",
        color: "var(--node-bg)",
        padding: "10px 20px",
        borderRadius: 8,
        fontSize: 13,
        display: "flex",
        alignItems: "center",
        gap: 12,
        boxShadow: "0 4px 16px rgba(0,0,0,0.2)",
      }}
    >
      <span>{message}</span>
      <button
        onClick={onUndo}
        style={{
          background: "none",
          border: "1px solid rgba(255,255,255,0.3)",
          color: "inherit",
          padding: "4px 10px",
          borderRadius: 4,
          cursor: "pointer",
          fontSize: 12,
          fontFamily: "inherit",
        }}
      >
        Undo
      </button>
    </div>
  );
}

// ── Empty State ────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div
      style={{
        position: "absolute",
        inset: 0,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        pointerEvents: "none",
        zIndex: 1,
      }}
    >
      <p
        style={{
          fontSize: 16,
          color: "var(--text-muted)",
          margin: 0,
        }}
      >
        Drag nodes from the sidebar to get started
      </p>
      <p
        style={{
          fontSize: 13,
          color: "var(--text-muted)",
          margin: "8px 0 0",
          opacity: 0.7,
        }}
      >
        Press ? for keyboard shortcuts
      </p>
    </div>
  );
}

// ── Inner Canvas (needs ReactFlow context) ─────────────────────────

function CanvasInner({
  pipelineNodes,
  pipelineEdges,
  nodeDefinitions,
  onNodePositionChange,
  onConnect: onConnectProp,
  onDeleteNode,
  onDeleteEdge,
  onDropNode,
  onRunFrom,
  onNodeSelect,
}: CanvasProps) {
  const reactFlow = useReactFlow();
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);

  // ── Derive React Flow nodes/edges from props ───────────────────
  const rfNodesFromProps = useMemo(
    () =>
      pipelineNodes.map((n) => toRFNode(n, nodeStatuses, nodeDefinitions)),
    [pipelineNodes, nodeStatuses, nodeDefinitions],
  );

  const rfEdgesFromProps = useMemo(
    () => pipelineEdges.map(toRFEdge),
    [pipelineEdges],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState<RFNode>(rfNodesFromProps);
  const [edges, setEdges, onEdgesChange] = useEdgesState(rfEdgesFromProps);

  // Sync when props change
  useEffect(() => {
    setNodes(rfNodesFromProps);
  }, [rfNodesFromProps, setNodes]);

  useEffect(() => {
    setEdges(rfEdgesFromProps);
  }, [rfEdgesFromProps, setEdges]);

  // ── Position drag debounce ─────────────────────────────────────
  const positionTimers = useRef<Record<string, ReturnType<typeof setTimeout>>>(
    {},
  );

  const handleNodesChange = useCallback(
    (changes: NodeChange<RFNode>[]) => {
      onNodesChange(changes);

      for (const change of changes) {
        if (
          change.type === "position" &&
          change.dragging === false &&
          change.position
        ) {
          const nodeId = change.id;
          const position = change.position;
          clearTimeout(positionTimers.current[nodeId]);
          positionTimers.current[nodeId] = setTimeout(() => {
            onNodePositionChange(nodeId, position);
            delete positionTimers.current[nodeId];
          }, POSITION_DEBOUNCE_MS);
        }
      }
    },
    [onNodesChange, onNodePositionChange],
  );

  // ── Selection tracking ─────────────────────────────────────────
  const handleSelectionChange = useCallback(
    ({ nodes: selectedNodes }: { nodes: RFNode[]; edges: RFEdge[] }) => {
      const selectedId = selectedNodes.length === 1 ? selectedNodes[0].id : null;
      onNodeSelect?.(selectedId);
    },
    [onNodeSelect],
  );

  // ── Connection validation ──────────────────────────────────────
  const connectStartRef = useRef<OnConnectStartParams | null>(null);

  const onConnectStart = useCallback(
    (_: unknown, params: OnConnectStartParams) => {
      connectStartRef.current = params;
    },
    [],
  );

  const isValidConnection: IsValidConnection = useCallback(
    (connection) => {
      const source = connection.source;
      const target = connection.target;
      const sourceHandle = connection.sourceHandle ?? null;
      const targetHandle = connection.targetHandle ?? null;
      if (!source || !target || !sourceHandle || !targetHandle) return false;
      if (source === target) return false;

      // Type validation: source port type must match target port type
      const sourceNode = pipelineNodes.find((n) => n.id === source);
      const targetNode = pipelineNodes.find((n) => n.id === target);
      if (!sourceNode || !targetNode) return false;

      const sourcePort = sourceNode.outputs.find(
        (p) => p.name === sourceHandle,
      );
      const targetPort = targetNode.inputs.find(
        (p) => p.name === targetHandle,
      );
      if (!sourcePort || !targetPort) return false;

      return sourcePort.type === targetPort.type;
    },
    [pipelineNodes],
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      if (
        !connection.source ||
        !connection.target ||
        !connection.sourceHandle ||
        !connection.targetHandle
      )
        return;

      onConnectProp({
        source: connection.source,
        sourcePort: connection.sourceHandle,
        target: connection.target,
        targetPort: connection.targetHandle,
      });
    },
    [onConnectProp],
  );

  // ── Delete with undo ───────────────────────────────────────────
  const [undoAction, setUndoAction] = useState<{
    message: string;
    restore: () => void;
  } | null>(null);

  const handleEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      onEdgesChange(changes);
    },
    [onEdgesChange],
  );

  const handleDelete = useCallback(
    (deletedNodes: RFNode[], deletedEdges: RFEdge[]) => {
      if (deletedNodes.length > 0) {
        const node = deletedNodes[0];
        const nodeId = node.id;
        const label = node.data.label ?? nodeId;
        setUndoAction({
          message: `Deleted "${label}"`,
          restore: () => {
            setNodes((prev) => [...prev, node]);
          },
        });
        onDeleteNode(nodeId);
      } else if (deletedEdges.length > 0) {
        const edge = deletedEdges[0];
        const edgeId = edge.id;
        setUndoAction({
          message: "Deleted edge",
          restore: () => {
            setEdges((prev) => [...prev, edge]);
          },
        });
        onDeleteEdge(edgeId);
      }
    },
    [onDeleteNode, onDeleteEdge, setNodes, setEdges],
  );

  // ── Drop zone ──────────────────────────────────────────────────
  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
  }, []);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      const nodeType = e.dataTransfer.getData("application/reactflow");
      if (!nodeType) return;

      const position = reactFlow.screenToFlowPosition({
        x: e.clientX,
        y: e.clientY,
      });
      onDropNode(nodeType, position);
    },
    [reactFlow, onDropNode],
  );

  // ── Context menus ──────────────────────────────────────────────
  const [nodeMenu, setNodeMenu] = useState<{
    x: number;
    y: number;
    nodeId: string;
  } | null>(null);
  const [canvasMenu, setCanvasMenu] = useState<{
    x: number;
    y: number;
  } | null>(null);

  const onNodeContextMenu = useCallback(
    (event: React.MouseEvent, node: RFNode) => {
      event.preventDefault();
      setCanvasMenu(null);
      setNodeMenu({ x: event.clientX, y: event.clientY, nodeId: node.id });
    },
    [],
  );

  const onPaneContextMenu = useCallback(
    (event: MouseEvent | React.MouseEvent) => {
      event.preventDefault();
      setNodeMenu(null);
      setCanvasMenu({ x: event.clientX, y: event.clientY });
    },
    [],
  );

  const closeMenus = useCallback(() => {
    setNodeMenu(null);
    setCanvasMenu(null);
  }, []);

  // ── Keyboard shortcuts ─────────────────────────────────────────
  const [shortcutModalOpen, setShortcutModalOpen] = useState(false);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Ignore if typing in an input
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;

      if (e.key === "?" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setShortcutModalOpen((v) => !v);
        return;
      }

      if (e.key === "Escape") {
        setShortcutModalOpen(false);
        closeMenus();
        return;
      }

      const mod = e.ctrlKey || e.metaKey;

      if (mod && e.key === "f") {
        e.preventDefault();
        reactFlow.fitView({ duration: 300 });
        return;
      }

      if (mod && e.key === "a") {
        e.preventDefault();
        setNodes((nds) => nds.map((n) => ({ ...n, selected: true })));
        return;
      }
    };

    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [reactFlow, closeMenus, setNodes]);

  // ── Connection line color ──────────────────────────────────────
  const connectionLineStyle = useMemo(() => {
    if (!connectStartRef.current) return { stroke: "var(--border-default)" };
    const { nodeId, handleId } = connectStartRef.current;
    if (!nodeId || !handleId) return { stroke: "var(--border-default)" };
    const node = pipelineNodes.find((n) => n.id === nodeId);
    const port = node?.outputs.find((p) => p.name === handleId);
    if (!port) return { stroke: "var(--border-default)" };
    return { stroke: PORT_COLORS[port.type as PortType] ?? "var(--border-default)" };
  }, [pipelineNodes]);

  // ── Render ─────────────────────────────────────────────────────

  const isEmpty = pipelineNodes.length === 0;

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        background: "var(--canvas-bg)",
        position: "relative",
      }}
    >
      <ReactFlow<RFNode>
        nodes={nodes}
        edges={edges}
        onNodesChange={handleNodesChange}
        onEdgesChange={handleEdgesChange}
        onNodesDelete={(deleted) => handleDelete(deleted, [])}
        onEdgesDelete={(deleted) => handleDelete([], deleted)}
        onConnect={onConnect}
        onConnectStart={onConnectStart}
        isValidConnection={isValidConnection}
        onSelectionChange={handleSelectionChange}
        onNodeContextMenu={onNodeContextMenu}
        onPaneContextMenu={onPaneContextMenu}
        onPaneClick={closeMenus}
        onDragOver={onDragOver}
        onDrop={onDrop}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        connectionLineStyle={connectionLineStyle}
        fitView
        fitViewOptions={{ duration: 300, padding: 0.2 }}
        deleteKeyCode={["Backspace", "Delete"]}
        multiSelectionKeyCode="Shift"
        snapToGrid
        snapGrid={[16, 16]}
        defaultEdgeOptions={{ type: "default" }}
        proOptions={{ hideAttribution: true }}
        style={{ background: "var(--canvas-bg)" }}
      >
        <Background gap={16} size={1} color="var(--border-default)" />
        <Controls
          showInteractive={false}
          style={{ borderRadius: 8, border: "1px solid var(--border-default)" }}
        />
      </ReactFlow>

      {isEmpty && <EmptyState />}

      {nodeMenu && (
        <ContextMenu
          x={nodeMenu.x}
          y={nodeMenu.y}
          nodeId={nodeMenu.nodeId}
          onRunFrom={onRunFrom}
          onDelete={onDeleteNode}
          onClose={closeMenus}
        />
      )}

      {canvasMenu && (
        <CanvasContextMenu
          x={canvasMenu.x}
          y={canvasMenu.y}
          onFitView={() => reactFlow.fitView({ duration: 300 })}
          onClose={closeMenus}
        />
      )}

      <ShortcutModal
        open={shortcutModalOpen}
        onClose={() => setShortcutModalOpen(false)}
      />

      {undoAction && (
        <UndoToast
          message={undoAction.message}
          onUndo={() => {
            undoAction.restore();
            setUndoAction(null);
          }}
          onDismiss={() => setUndoAction(null)}
        />
      )}
    </div>
  );
}

// ── Wrapped export ─────────────────────────────────────────────────

export default function Canvas(props: CanvasProps) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}
