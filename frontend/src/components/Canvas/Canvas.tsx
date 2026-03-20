import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
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

import { addNode, addEdge } from "../../lib/api";
import NodeCard from "./NodeCard";
import EdgeWithCondition from "./EdgeWithCondition";
import ContextMenu from "./ContextMenu";
import CanvasContextMenu from "./CanvasContextMenu";
import ShortcutModal from "./ShortcutModal";
import UndoToast, { type UndoToastData } from "./UndoToast";

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
  onPatchEdge: (edgeId: string, condition: string) => void;
  onDropNode: (type: string, position: { x: number; y: number }) => void;
  onRunFrom: (nodeId: string) => void;
  onNodeSelect?: (nodeId: string | null) => void;
  onTabClick?: (nodeId: string, tab: string) => void;
  onRenameNode?: (nodeId: string) => void;
  onDuplicateNode?: (nodeId: string) => void;
  onPasteNodes?: (nodes: Array<{ type: string; position: { x: number; y: number }; params?: unknown; code?: string }>, edges?: Array<{ sourceIdx: number; targetIdx: number; sourcePort: string; targetPort: string; condition?: string }>) => Promise<string[]>;
}

// ── Constants ──────────────────────────────────────────────────────

const POSITION_DEBOUNCE_MS = 300;

const nodeTypes = { nodeCard: NodeCard };
const edgeTypes = { default: EdgeWithCondition };

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
  pipelineId,
  pipelineNodes,
  pipelineEdges,
  nodeDefinitions,
  onNodePositionChange,
  onConnect: onConnectProp,
  onDeleteNode,
  onDeleteEdge,
  onPatchEdge,
  onDropNode,
  onRunFrom,
  onNodeSelect,
  onTabClick,
  onRenameNode,
  onDuplicateNode,
  onPasteNodes,
}: CanvasProps) {
  const reactFlow = useReactFlow();
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);

  // ── Clipboard for copy/paste ────────────────────────────────
  interface ClipboardNode { id: string; type: string; offsetX: number; offsetY: number; params?: unknown; code?: string }
  interface ClipboardEdge { sourceIdx: number; targetIdx: number; sourcePort: string; targetPort: string; condition?: string }
  const clipboardRef = useRef<{ nodes: ClipboardNode[]; edges: ClipboardEdge[] }>({ nodes: [], edges: [] });
  const pasteCountRef = useRef(0);
  const setDraggingPortType = useExecutionStore((s) => s.setDraggingPortType);
  const queryClient = useQueryClient();

  // ── Undo toast state ──────────────────────────────────────────
  const [undoToast, setUndoToast] = useState<UndoToastData | null>(null);
  const dismissToast = useCallback(() => setUndoToast(null), []);

  // ── Edge delete with undo (needed before edge conversion) ────
  const handleDeleteEdgeWithUndo = useCallback(
    (edgeId: string) => {
      const edge = pipelineEdges.find((e) => e.id === edgeId);
      onDeleteEdge(edgeId);
      if (!edge) return;

      const snapshot = {
        source: edge.source,
        source_port: edge.source_port,
        target: edge.target,
        target_port: edge.target_port,
      };

      setUndoToast({
        message: "Edge deleted",
        onUndo: () => {
          addEdge(pipelineId, snapshot).then(() => {
            queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
          });
        },
      });
    },
    [pipelineId, pipelineEdges, onDeleteEdge, queryClient],
  );

  // ── Derive React Flow nodes/edges from props ───────────────────
  const rfNodesFromProps = useMemo(
    () =>
      pipelineNodes.map((n) => toRFNode(n, nodeStatuses, nodeDefinitions, onTabClick)),
    [pipelineNodes, nodeStatuses, nodeDefinitions, onTabClick],
  );

  const rfEdgesFromProps = useMemo(
    () => pipelineEdges.map((e) => toRFEdge(e, handleDeleteEdgeWithUndo, onPatchEdge)),
    [pipelineEdges, handleDeleteEdgeWithUndo, onPatchEdge],
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
  const [connectStartParams, setConnectStartParams] =
    useState<OnConnectStartParams | null>(null);

  const onConnectStart = useCallback(
    (_: unknown, params: OnConnectStartParams) => {
      setConnectStartParams(params);

      // Set dragging port type for visual feedback on PortDots
      if (params.nodeId && params.handleId) {
        const node = pipelineNodes.find((n) => n.id === params.nodeId);
        const port = node?.outputs.find((p) => p.name === params.handleId)
          ?? node?.inputs.find((p) => p.name === params.handleId);
        if (port) {
          setDraggingPortType(port.type);
        }
      }
    },
    [pipelineNodes, setDraggingPortType],
  );

  const onConnectEnd = useCallback(() => {
    setConnectStartParams(null);
    setDraggingPortType(null);
  }, [setDraggingPortType]);

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

  // ── Delete handler ───────────────────────────────────────────
  const handleEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      onEdgesChange(changes);
    },
    [onEdgesChange],
  );

  const handleDeleteNodeWithUndo = useCallback(
    (nodeId: string) => {
      const node = pipelineNodes.find((n) => n.id === nodeId);
      onDeleteNode(nodeId);
      if (!node) return;

      const snapshot = {
        type: node.type,
        position: { ...node.position },
        params: Object.fromEntries(node.params.map((p) => [p.name, p.default])),
      };

      setUndoToast({
        message: "Node deleted (edges removed)",
        onUndo: () => {
          addNode(pipelineId, snapshot).then(() => {
            queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
          });
        },
      });
    },
    [pipelineId, pipelineNodes, onDeleteNode, queryClient],
  );

  const handleDelete = useCallback(
    (deletedNodes: RFNode[], deletedEdges: RFEdge[]) => {
      for (const node of deletedNodes) {
        handleDeleteNodeWithUndo(node.id);
      }
      for (const edge of deletedEdges) {
        handleDeleteEdgeWithUndo(edge.id);
      }
    },
    [handleDeleteNodeWithUndo, handleDeleteEdgeWithUndo],
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
      // Ignore if typing in an input or Monaco editor
      const target = e.target as HTMLElement;
      const tag = target?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (target?.closest?.(".monaco-editor")) return;

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

      if (mod && e.shiftKey && e.key === "f") {
        e.preventDefault();
        reactFlow.fitView({ duration: 300 });
        return;
      }

      if (mod && e.key === "a") {
        e.preventDefault();
        setNodes((nds) => nds.map((n) => ({ ...n, selected: true })));
        return;
      }

      if (mod && e.key === "c") {
        const selected = nodes.filter((n) => n.selected);
        if (selected.length === 0) return;
        const selectedIds = new Set(selected.map((n) => n.id));
        const minX = Math.min(...selected.map((n) => n.position.x));
        const minY = Math.min(...selected.map((n) => n.position.y));
        const copiedNodes: ClipboardNode[] = selected.map((n) => ({
          id: n.id,
          type: n.data.type as string,
          offsetX: n.position.x - minX,
          offsetY: n.position.y - minY,
          params: n.data.params as unknown,
          code: n.data.code as string,
        }));
        // Capture edges where both source and target are in selection
        const copiedEdges: ClipboardEdge[] = edges
          .filter((e) => selectedIds.has(e.source) && selectedIds.has(e.target))
          .map((e) => ({
            sourceIdx: selected.findIndex((n) => n.id === e.source),
            targetIdx: selected.findIndex((n) => n.id === e.target),
            sourcePort: (e.sourceHandle ?? "") as string,
            targetPort: (e.targetHandle ?? "") as string,
            condition: (e.data as Record<string, unknown>)?.condition as string | undefined,
          }));
        clipboardRef.current = { nodes: copiedNodes, edges: copiedEdges };
        pasteCountRef.current = 0;
        toast.info(`Copied ${selected.length} node(s)`);
        return;
      }

      if (mod && e.key === "v") {
        e.preventDefault();
        if (clipboardRef.current.nodes.length === 0 || !onPasteNodes) return;
        pasteCountRef.current += 1;
        const offset = pasteCountRef.current * 30;
        const center = reactFlow.screenToFlowPosition({ x: window.innerWidth / 2, y: window.innerHeight / 2 });
        onPasteNodes(
          clipboardRef.current.nodes.map((c) => ({
            type: c.type,
            position: { x: center.x + c.offsetX + offset, y: center.y + c.offsetY + offset },
            params: c.params,
            code: c.code,
          })),
          clipboardRef.current.edges,
        ).then((newIds) => {
          if (newIds.length > 0) {
            const idSet = new Set(newIds);
            setNodes((nds) => nds.map((n) => ({ ...n, selected: idSet.has(n.id) })));
          }
        });
        return;
      }
    };

    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [reactFlow, closeMenus, setNodes, nodes, onPasteNodes]);

  // ── Connection line color ──────────────────────────────────────
  const connectionLineStyle = useMemo(() => {
    if (!connectStartParams) return { stroke: "var(--border-default)" };
    const { nodeId, handleId } = connectStartParams;
    if (!nodeId || !handleId) return { stroke: "var(--border-default)" };
    const node = pipelineNodes.find((n) => n.id === nodeId);
    const port = node?.outputs.find((p) => p.name === handleId);
    if (!port) return { stroke: "var(--border-default)" };
    return { stroke: PORT_COLORS[port.type as PortType] ?? "var(--border-default)" };
  }, [connectStartParams, pipelineNodes]);

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
        onConnectEnd={onConnectEnd}
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
        <MiniMap
          nodeColor="var(--border-default)"
          maskColor="rgba(0, 0, 0, 0.08)"
          style={{
            background: "var(--canvas-bg)",
            borderRadius: 8,
            border: "1px solid var(--border-default)",
          }}
        />
      </ReactFlow>

      {isEmpty && <EmptyState />}

      {nodeMenu && (
        <ContextMenu
          x={nodeMenu.x}
          y={nodeMenu.y}
          nodeId={nodeMenu.nodeId}
          onRunFrom={onRunFrom}
          onRename={(nodeId) => onRenameNode?.(nodeId)}
          onDuplicate={(nodeId) => onDuplicateNode?.(nodeId)}
          onDelete={handleDeleteNodeWithUndo}
          onClose={closeMenus}
        />
      )}

      {canvasMenu && (
        <CanvasContextMenu
          x={canvasMenu.x}
          y={canvasMenu.y}
          onFitView={() => reactFlow.fitView({ duration: 300 })}
          onPaste={onPasteNodes ? () => {
            if (clipboardRef.current.nodes.length === 0) return;
            const pos = reactFlow.screenToFlowPosition({ x: canvasMenu.x, y: canvasMenu.y });
            onPasteNodes(
              clipboardRef.current.nodes.map((c) => ({
                type: c.type,
                position: { x: pos.x + c.offsetX, y: pos.y + c.offsetY },
                params: c.params,
                code: c.code,
              })),
              clipboardRef.current.edges,
            ).then((newIds) => {
              if (newIds.length > 0) {
                const idSet = new Set(newIds);
                setNodes((nds) => nds.map((n) => ({ ...n, selected: idSet.has(n.id) })));
              }
            });
          } : undefined}
          hasCopied={clipboardRef.current.nodes.length > 0}
          onClose={closeMenus}
        />
      )}

      <ShortcutModal
        open={shortcutModalOpen}
        onClose={() => setShortcutModalOpen(false)}
      />

      <UndoToast data={undoToast} onDismiss={dismissToast} />
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
