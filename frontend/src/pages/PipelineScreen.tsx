import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import * as api from "../lib/api";
import type { AddNodeRequest, NodeDefinition } from "../lib/types";
import { useExecutionSocket } from "../hooks/useExecutionSocket";
import { useExecutionStore } from "../store/executionStore";
import Topbar from "../components/Topbar/Topbar";
import Sidebar from "../components/Sidebar/Sidebar";
import Canvas from "../components/Canvas/Canvas";
import DisconnectionBanner from "../components/Canvas/DisconnectionBanner";
import { RightPanel } from "../components/RightPanel/RightPanel";
import ErrorBoundary from "../components/ErrorBoundary";
import { toast } from "sonner";

export default function PipelineScreen() {
  const { id } = useParams<{ id: string }>();
  const pipelineId = id!;
  const queryClient = useQueryClient();

  // ── Server data ────────────────────────────────────────────────
  const {
    data: pipeline,
    isLoading,
    isFetching,
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
  const [codeSaveOk, setCodeSaveOk] = useState(false);
  const [requestedTab, setRequestedTab] = useState<string | null>(null);
  const [requestedRunId, setRequestedRunId] = useState<string | null>(null);

  // Auto-switch to Output tab when selected node completes (done/error)
  const lastDoneNodeId = useExecutionStore((s) => s.lastDoneNodeId);
  const setLastDoneNodeId = useExecutionStore((s) => s.setLastDoneNodeId);
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);

  useEffect(() => {
    if (lastDoneNodeId && lastDoneNodeId === selectedNodeId) {
      setRequestedTab("output");
      setLastDoneNodeId(null);
    } else if (lastDoneNodeId) {
      setLastDoneNodeId(null);
    }
  }, [lastDoneNodeId, selectedNodeId, setLastDoneNodeId]);

  // Pipeline switch transition: show skeleton while fetching new pipeline
  const viewportCenterRef = useRef<(() => { x: number; y: number }) | null>(null);
  const clickAddCountRef = useRef(0);
  const prevPipelineIdRef = useRef(pipelineId);
  const [switched, setSwitched] = useState(false);

  useEffect(() => {
    if (prevPipelineIdRef.current !== pipelineId) {
      setSwitched(true);
      prevPipelineIdRef.current = pipelineId;
    }
  }, [pipelineId]);

  // Clear switched flag once new data arrives
  useEffect(() => {
    if (switched && !isFetching) {
      setSwitched(false);
    }
  }, [switched, isFetching]);

  const showSkeleton = switched && isFetching;

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
      condition?: string;
    }) =>
      api.addEdge(pipelineId, {
        source: conn.source,
        source_port: conn.sourcePort,
        target: conn.target,
        target_port: conn.targetPort,
        ...(conn.condition ? { condition: conn.condition } : {}),
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

  const patchEdgeMutation = useMutation({
    mutationFn: ({
      edgeId,
      body,
    }: {
      edgeId: string;
      body: { condition?: string };
    }) => api.patchEdge(pipelineId, edgeId, body),
    onSuccess: invalidate,
  });

  const addNodeMutation = useMutation({
    mutationFn: (body: AddNodeRequest) => api.addNode(pipelineId, body),
    onSuccess: invalidate,
  });

  const patchNodeMutation = useMutation({
    mutationFn: ({
      nodeId,
      body,
    }: {
      nodeId: string;
      body: { params?: Record<string, unknown>; code?: string; name?: string };
    }) => api.patchNode(pipelineId, nodeId, body),
    onSuccess: invalidate,
  });

  const runFromMutation = useMutation({
    mutationFn: (nodeId: string) => api.runFromNode(pipelineId, nodeId),
  });

  // ── Handlers ──────────────────────────────────────────────────
  const handleNodePositionChange = useCallback(
    (nodeId: string, position: { x: number; y: number }) => {
      patchNodePosition.mutate({ nodeId, position }, { onError: () => toast.error("Failed to save position") });
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
      addEdgeMutation.mutate(conn, {
        onError: (err) => {
          const msg = err instanceof Error ? err.message : "Connection failed";
          toast.error(msg.includes("cycle") ? "Cannot connect: would create cycle" : msg);
        },
      });
    },
    [addEdgeMutation],
  );

  const handleDeleteNode = useCallback(
    (nodeId: string) => deleteNodeMutation.mutate(nodeId, { onError: () => toast.error("Failed to delete node") }),
    [deleteNodeMutation],
  );

  const handleDeleteEdge = useCallback(
    (edgeId: string) => deleteEdgeMutation.mutate(edgeId, { onError: () => toast.error("Failed to delete edge") }),
    [deleteEdgeMutation],
  );

  const handlePatchEdge = useCallback(
    (edgeId: string, condition: string) => {
      patchEdgeMutation.mutate(
        { edgeId, body: { condition: condition || undefined } },
        { onError: () => toast.error("Failed to save condition") },
      );
    },
    [patchEdgeMutation],
  );

  const handleDropNode = useCallback(
    (type: string, position: { x: number; y: number }) => {
      addNodeMutation.mutate({ type, position }, { onError: () => toast.error("Failed to add node") });
    },
    [addNodeMutation],
  );

  const handleRunFrom = useCallback(
    (nodeId: string) => runFromMutation.mutate(nodeId, { onError: () => toast.error("Failed to run") }),
    [runFromMutation],
  );

  const handleAddNodeFromSidebar = useCallback(
    (nodeType: string) => {
      const center = viewportCenterRef.current?.() ?? { x: 250, y: 150 };
      const n = clickAddCountRef.current++;
      const position = { x: center.x + n * 220, y: center.y };
      addNodeMutation.mutate(
        { type: nodeType, position },
        { onError: () => toast.error("Failed to add node") },
      );
    },
    [addNodeMutation],
  );

  const handleNodeSelect = useCallback((nodeId: string | null) => {
    setSelectedNodeId(nodeId);
    // Default to Output tab for nodes with done/error status
    if (nodeId) {
      const status = nodeStatuses[nodeId];
      if (status === "done" || status === "error" || status === "cached") {
        setRequestedTab("output");
      } else {
        setRequestedTab(null);
      }
    } else {
      setRequestedTab(null);
    }
  }, [nodeStatuses]);

  const handleTabClick = useCallback((nodeId: string, tab: string) => {
    setSelectedNodeId(nodeId);
    setRequestedTab(tab);
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
      patchNodeMutation.mutate(
        { nodeId, body: { params: currentValues } },
        {
          onError: () => {
            toast.error("Failed to save parameter");
            invalidate();
          },
        },
      );
    },
    [pipeline, patchNodeMutation, invalidate],
  );

  const handleCodeChange = useCallback(
    (nodeId: string, code: string) => {
      pendingCodeRef.current[nodeId] = code;
    },
    [],
  );

  const handleCodeSave = useCallback(
    (nodeId: string, code: string) => {
      delete pendingCodeRef.current[nodeId];
      setCodeSaveOk(false);
      patchNodeMutation.mutate(
        { nodeId, body: { code } },
        {
          onSuccess: () => setCodeSaveOk(true),
          onError: () => toast.error("Failed to save code"),
        },
      );
    },
    [patchNodeMutation],
  );

  const handleClosePanel = useCallback(() => {
    setSelectedNodeId(null);
  }, []);

  // ── Rename ─────────────────────────────────────────────────────
  const [renameRequested, setRenameRequested] = useState(false);

  const handleRename = useCallback(
    (nodeId: string, name: string) => {
      patchNodeMutation.mutate({ nodeId, body: { name } }, {
        onError: () => toast.error("Failed to rename node"),
      });
    },
    [patchNodeMutation],
  );

  const handleRenameFromContextMenu = useCallback(
    (nodeId: string) => {
      // Select the node to open the right panel, then trigger rename
      setSelectedNodeId(nodeId);
      setRenameRequested(true);
    },
    [],
  );

  const handleRenameHandled = useCallback(() => {
    setRenameRequested(false);
  }, []);

  const handleDuplicateNode = useCallback(
    (nodeId: string) => {
      const node = pipeline?.nodes.find((n) => n.id === nodeId);
      if (!node) return;
      const def = nodeDefinitions[node.type];
      const baseName = node.name || def?.label || node.type;
      addNodeMutation.mutate({
        type: node.type,
        position: { x: node.position.x + 50, y: node.position.y + 50 },
        params: node.params,
        code: node.code,
        name: `${baseName} (copy)`,
      }, {
        onError: () => toast.error("Failed to duplicate node"),
      });
    },
    [pipeline, addNodeMutation],
  );

  const handlePasteNodes = useCallback(
    async (
      pastedNodes: Array<{ type: string; position: { x: number; y: number }; params?: unknown; code?: string }>,
      pastedEdges?: Array<{ sourceIdx: number; targetIdx: number; sourcePort: string; targetPort: string; condition?: string }>,
    ): Promise<string[]> => {
      // Create all nodes in parallel and collect new IDs (order preserved)
      const results = await Promise.allSettled(
        pastedNodes.map((n) =>
          addNodeMutation.mutateAsync({
            type: n.type,
            position: n.position,
            params: n.params as Record<string, unknown> | undefined,
            code: n.code,
          }),
        ),
      );
      const newIds = results.map((r) =>
        r.status === "fulfilled" ? r.value.id : "",
      );
      // Report partial paste
      const succeeded = newIds.filter(Boolean).length;
      const failed = pastedNodes.length - succeeded;
      if (failed > 0) {
        toast.error(`Pasted ${succeeded} of ${pastedNodes.length} nodes (${failed} failed)`);
      }
      // Recreate edges between pasted nodes
      if (pastedEdges && pastedEdges.length > 0) {
        for (const e of pastedEdges) {
          const source = newIds[e.sourceIdx];
          const target = newIds[e.targetIdx];
          if (source && target) {
            addEdgeMutation.mutate({
              source,
              sourcePort: e.sourcePort,
              target,
              targetPort: e.targetPort,
              ...(e.condition ? { condition: e.condition } : {}),
            }, {
              onError: () => toast.error("Failed to create edge during paste"),
            });
          }
        }
      }
      return newIds.filter(Boolean);
    },
    [addNodeMutation, addEdgeMutation],
  );

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
      <Topbar
        pipelineId={pipelineId}
        onViewRun={selectedNodeId ? (runId) => {
          setRequestedTab("output");
          setRequestedRunId(runId);
        } : undefined}
      />
      <DisconnectionBanner />
      <div className="flex flex-1 min-h-0">
        <ErrorBoundary key={pipelineId} variant="compact">
          <Sidebar onAddNode={handleAddNodeFromSidebar} />
        </ErrorBoundary>
        <main
          className="flex-1 min-w-0 overflow-hidden relative"
          style={{ backgroundColor: "var(--canvas-bg)" }}
        >
          {showSkeleton && (
            <div
              className="absolute inset-0"
              style={{ backgroundColor: "var(--canvas-bg)", zIndex: 10 }}
            >
              {/* Skeleton node cards scattered like a real canvas */}
              {[
                { x: "15%", y: "20%", w: 232, h: 100 },
                { x: "45%", y: "15%", w: 232, h: 120 },
                { x: "30%", y: "55%", w: 232, h: 90 },
              ].map((s, i) => (
                <div
                  key={i}
                  className="animate-pulse absolute"
                  style={{
                    left: s.x,
                    top: s.y,
                    width: s.w,
                    height: s.h,
                    borderRadius: 8,
                    border: "1px solid var(--border-default)",
                    background: "var(--node-bg)",
                    opacity: 0.5,
                  }}
                >
                  <div
                    style={{
                      height: 3,
                      margin: "0 8px",
                      borderRadius: "0 0 2px 2px",
                      background: "var(--border-default)",
                      opacity: 0.5,
                    }}
                  />
                </div>
              ))}
              {/* Skeleton edge lines */}
              <svg className="absolute inset-0 w-full h-full" style={{ opacity: 0.2 }}>
                <line x1="calc(15% + 232px)" y1="calc(20% + 50px)" x2="45%" y2="calc(15% + 60px)" stroke="var(--border-default)" strokeWidth="2" />
                <line x1="calc(45% + 116px)" y1="calc(15% + 120px)" x2="calc(30% + 116px)" y2="55%" stroke="var(--border-default)" strokeWidth="2" />
              </svg>
            </div>
          )}
          <div
            style={{
              opacity: showSkeleton ? 0 : 1,
              transition: "opacity 150ms ease-out",
              height: "100%",
            }}
          >
          <ErrorBoundary key={pipelineId} variant="compact">
          <Canvas
            pipelineId={pipelineId}
            pipelineNodes={pipeline.nodes}
            pipelineEdges={pipeline.edges}
            nodeDefinitions={nodeDefinitions}
            onNodePositionChange={handleNodePositionChange}
            onConnect={handleConnect}
            onDeleteNode={handleDeleteNode}
            onDeleteEdge={handleDeleteEdge}
            onPatchEdge={handlePatchEdge}
            onDropNode={handleDropNode}
            onRunFrom={handleRunFrom}
            onNodeSelect={handleNodeSelect}
            onTabClick={handleTabClick}
            onRenameNode={handleRenameFromContextMenu}
            onDuplicateNode={handleDuplicateNode}
            onPasteNodes={handlePasteNodes}
            viewportCenterRef={viewportCenterRef}
          />
          </ErrorBoundary>
          </div>
        </main>
        <ErrorBoundary key={pipelineId} variant="compact">
        <RightPanel
          pipelineId={pipelineId}
          node={selectedNode}
          definition={selectedDefinition}
          onParamChange={handleParamChange}
          paramSaving={patchNodeMutation.isPending}
          onCodeChange={handleCodeChange}
          onCodeSave={handleCodeSave}
          codeSaveOk={codeSaveOk}
          onClose={handleClosePanel}
          requestedTab={requestedTab}
          onRequestedTabHandled={() => setRequestedTab(null)}
          onRename={handleRename}
          onRunFrom={handleRunFrom}
          renameRequested={renameRequested}
          onRenameHandled={handleRenameHandled}
          requestedRunId={requestedRunId}
          onRequestedRunHandled={() => setRequestedRunId(null)}
        />
        </ErrorBoundary>
      </div>
    </div>
  );
}
