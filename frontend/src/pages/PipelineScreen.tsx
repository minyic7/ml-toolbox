import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import * as api from "../lib/api";
import type { NodeDefinition } from "../lib/types";
import { useExecutionSocket } from "../hooks/useExecutionSocket";
import { useOutput } from "../hooks/useOutputs";
import Topbar from "../components/Topbar/Topbar";
import Sidebar from "../components/Sidebar/Sidebar";
import Canvas from "../components/Canvas/Canvas";
import { RightPanel } from "../components/RightPanel/RightPanel";

export default function PipelineScreen() {
  const { id } = useParams<{ id: string }>();
  const pipelineId = id!;
  const queryClient = useQueryClient();

  // ── Server data ────────────────────────────────────────────────
  const {
    data: pipeline,
    isLoading,
    error,
  } = useQuery({
    queryKey: ["pipeline", pipelineId],
    queryFn: () => api.getPipeline(pipelineId),
    enabled: !!id,
  });

  const { data: nodeDefList } = useQuery({
    queryKey: ["nodeDefinitions"],
    queryFn: api.getNodeDefinitions,
  });

  const nodeDefinitions = useMemo(() => {
    const map: Record<string, NodeDefinition> = {};
    for (const def of nodeDefList ?? []) {
      map[def.type] = def;
    }
    return map;
  }, [nodeDefList]);

  // ── WebSocket ─────────────────────────────────────────────────
  useExecutionSocket(pipelineId);

  // ── UI state ──────────────────────────────────────────────────
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  // Clear selection when switching pipelines
  useEffect(() => {
    setSelectedNodeId(null);
  }, [pipelineId]);

  // Escape key clears selection
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        const tag = (e.target as HTMLElement)?.tagName;
        if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
        setSelectedNodeId(null);
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, []);

  // Derive selected node and definition
  const selectedNode = useMemo(
    () => pipeline?.nodes.find((n) => n.id === selectedNodeId) ?? null,
    [pipeline, selectedNodeId],
  );

  const selectedDefinition = useMemo(
    () => (selectedNode ? nodeDefinitions[selectedNode.type] ?? null : null),
    [selectedNode, nodeDefinitions],
  );

  // ── Output for selected node ──────────────────────────────────
  const { data: selectedOutput = null } = useOutput(
    pipelineId,
    selectedNodeId ?? "",
  );

  const downloadUrl = useMemo(
    () =>
      selectedNodeId
        ? api.getOutputDownloadUrl(pipelineId, selectedNodeId)
        : null,
    [pipelineId, selectedNodeId],
  );

  // ── Mutations ─────────────────────────────────────────────────
  const invalidate = useCallback(
    () =>
      queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
    [queryClient, pipelineId],
  );

  const patchNodePosition = useMutation({
    mutationFn: ({
      nodeId,
      position,
    }: {
      nodeId: string;
      position: { x: number; y: number };
    }) => api.patchNode(pipelineId, nodeId, { position }),
    onSuccess: invalidate,
  });

  const addEdgeMutation = useMutation({
    mutationFn: (conn: {
      source: string;
      sourcePort: string;
      target: string;
      targetPort: string;
    }) =>
      api.addEdge(pipelineId, {
        source: conn.source,
        source_port: conn.sourcePort,
        target: conn.target,
        target_port: conn.targetPort,
      }),
    onSuccess: invalidate,
  });

  const deleteNodeMutation = useMutation({
    mutationFn: (nodeId: string) => api.deleteNode(pipelineId, nodeId),
    onSuccess: (_, nodeId) => {
      // Clear selection if the deleted node was selected
      setSelectedNodeId((prev) => (prev === nodeId ? null : prev));
      invalidate();
    },
  });

  const deleteEdgeMutation = useMutation({
    mutationFn: (edgeId: string) => api.deleteEdge(pipelineId, edgeId),
    onSuccess: invalidate,
  });

  const addNodeMutation = useMutation({
    mutationFn: ({
      type,
      position,
    }: {
      type: string;
      position: { x: number; y: number };
    }) => api.addNode(pipelineId, { type, position }),
    onSuccess: invalidate,
  });

  const patchNodeMutation = useMutation({
    mutationFn: ({
      nodeId,
      body,
    }: {
      nodeId: string;
      body: { params?: Record<string, unknown>; code?: string };
    }) => api.patchNode(pipelineId, nodeId, body),
    onSuccess: invalidate,
  });

  const runFromMutation = useMutation({
    mutationFn: (nodeId: string) => api.runFromNode(pipelineId, nodeId),
  });

  // ── Handlers ──────────────────────────────────────────────────
  const handleNodePositionChange = useCallback(
    (nodeId: string, position: { x: number; y: number }) => {
      patchNodePosition.mutate({ nodeId, position });
    },
    [patchNodePosition],
  );

  const handleConnect = useCallback(
    (conn: {
      source: string;
      sourcePort: string;
      target: string;
      targetPort: string;
    }) => {
      addEdgeMutation.mutate(conn);
    },
    [addEdgeMutation],
  );

  const handleDeleteNode = useCallback(
    (nodeId: string) => deleteNodeMutation.mutate(nodeId),
    [deleteNodeMutation],
  );

  const handleDeleteEdge = useCallback(
    (edgeId: string) => deleteEdgeMutation.mutate(edgeId),
    [deleteEdgeMutation],
  );

  const handleDropNode = useCallback(
    (type: string, position: { x: number; y: number }) => {
      addNodeMutation.mutate({ type, position });
    },
    [addNodeMutation],
  );

  const handleRunFrom = useCallback(
    (nodeId: string) => runFromMutation.mutate(nodeId),
    [runFromMutation],
  );

  const handleAddNodeFromSidebar = useCallback(
    (nodeType: string) => {
      addNodeMutation.mutate({
        type: nodeType,
        position: { x: 250, y: 150 },
      });
    },
    [addNodeMutation],
  );

  const handleNodeSelect = useCallback((nodeId: string | null) => {
    setSelectedNodeId(nodeId);
  }, []);

  // Track pending code edits so we can save on blur
  const pendingCodeRef = useRef<Record<string, string>>({});

  const handleParamChange = useCallback(
    (nodeId: string, name: string, value: unknown) => {
      const node = pipeline?.nodes.find((n) => n.id === nodeId);
      if (!node) return;
      const currentValues: Record<string, unknown> = {};
      for (const p of node.params) {
        currentValues[p.name] = p.default;
      }
      currentValues[name] = value;
      patchNodeMutation.mutate({ nodeId, body: { params: currentValues } });
    },
    [pipeline, patchNodeMutation],
  );

  const handleCodeChange = useCallback(
    (nodeId: string, code: string) => {
      pendingCodeRef.current[nodeId] = code;
    },
    [],
  );

  const handleCodeBlur = useCallback(
    (nodeId: string) => {
      const code = pendingCodeRef.current[nodeId];
      if (code === undefined) return;
      delete pendingCodeRef.current[nodeId];
      patchNodeMutation.mutate({ nodeId, body: { code } });
    },
    [patchNodeMutation],
  );

  const handleClosePanel = useCallback(() => {
    setSelectedNodeId(null);
  }, []);

  // ── Loading state ─────────────────────────────────────────────
  if (!id) return null;

  if (isLoading) {
    return (
      <div className="flex flex-col h-screen">
        {/* Topbar placeholder */}
        <div
          className="shrink-0 border-b"
          style={{
            height: 48,
            backgroundColor: "var(--node-bg)",
            borderColor: "var(--border-default)",
          }}
        />
        <div
          className="flex-1 flex items-center justify-center"
          style={{ backgroundColor: "var(--canvas-bg)" }}
        >
          <div
            className="animate-spin h-8 w-8 rounded-full border-2 border-t-transparent"
            style={{ borderColor: "var(--border-default)", borderTopColor: "transparent" }}
          />
        </div>
      </div>
    );
  }

  if (error || !pipeline) {
    return (
      <div className="flex flex-col h-screen">
        <div
          className="shrink-0 border-b"
          style={{
            height: 48,
            backgroundColor: "var(--node-bg)",
            borderColor: "var(--border-default)",
          }}
        />
        <div
          className="flex-1 flex flex-col items-center justify-center gap-4"
          style={{ backgroundColor: "var(--canvas-bg)" }}
        >
          <p style={{ color: "var(--error-red)", fontSize: 16 }}>
            Pipeline not found
          </p>
          <Link
            to="/"
            className="text-sm underline"
            style={{ color: "var(--accent-blue)" }}
          >
            Back to Home
          </Link>
        </div>
      </div>
    );
  }

  // ── Render ────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-screen">
      <Topbar pipelineId={pipelineId} />
      <div className="flex flex-1 min-h-0">
        <Sidebar onAddNode={handleAddNodeFromSidebar} />
        <main
          className="flex-1 min-w-0 overflow-hidden"
          style={{ backgroundColor: "var(--canvas-bg)" }}
        >
          <Canvas
            pipelineId={pipelineId}
            pipelineNodes={pipeline.nodes}
            pipelineEdges={pipeline.edges}
            nodeDefinitions={nodeDefinitions}
            onNodePositionChange={handleNodePositionChange}
            onConnect={handleConnect}
            onDeleteNode={handleDeleteNode}
            onDeleteEdge={handleDeleteEdge}
            onDropNode={handleDropNode}
            onRunFrom={handleRunFrom}
            onNodeSelect={handleNodeSelect}
          />
        </main>
        <RightPanel
          node={selectedNode}
          definition={selectedDefinition}
          output={selectedOutput}
          downloadUrl={downloadUrl}
          onParamChange={handleParamChange}
          onCodeChange={handleCodeChange}
          onCodeBlur={handleCodeBlur}
          onClose={handleClosePanel}
        />
      </div>
    </div>
  );
}
