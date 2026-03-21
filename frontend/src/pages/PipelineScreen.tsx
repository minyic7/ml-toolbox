import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import * as api from "../lib/api";
import type { AddNodeRequest, NodeDefinition, Pipeline } from "../lib/types";
import { useExecutionSocket } from "../hooks/useExecutionSocket";
import { useExecutionStore } from "../store/executionStore";
import Topbar from "../components/Topbar/Topbar";
import Toolbar from "../components/Toolbar/Toolbar";
import Canvas from "../components/Canvas/Canvas";
import DisconnectionBanner from "../components/Canvas/DisconnectionBanner";
import BottomDrawer from "../components/Drawer/BottomDrawer";
import CodePane from "../components/CodePane/CodePane";
import OutputPanel from "../components/OutputPanel/OutputPanel";
import ErrorBoundary from "../components/ErrorBoundary";
import { toast } from "sonner";

function getDownstreamNodeIds(nodeId: string, pipeline: Pipeline): string[] {
  const result = new Set<string>([nodeId]);
  const queue = [nodeId];
  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const edge of pipeline.edges) {
      if (edge.source === current && !result.has(edge.target)) {
        result.add(edge.target);
        queue.push(edge.target);
      }
    }
  }
  return Array.from(result);
}

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
  const [requestedRunId, setRequestedRunId] = useState<string | null>(null);

  // Auto-open output panel when selected node completes (done/error)
  const lastDoneNodeId = useExecutionStore((s) => s.lastDoneNodeId);
  const setLastDoneNodeId = useExecutionStore((s) => s.setLastDoneNodeId);
  const nodeStatuses = useExecutionStore((s) => s.nodeStatuses);

  useEffect(() => {
    if (!lastDoneNodeId) return;
    const status = nodeStatuses[lastDoneNodeId];

    if (lastDoneNodeId === selectedNodeId) {
      // Selected node completed — open output panel
      setRightPanelMode("output");
      setRightPanelOpen(true);
    } else if (status === "error") {
      // Non-selected node errored — show toast with action to navigate
      const errorNodeId = lastDoneNodeId;
      toast.error("Node failed", {
        duration: 8000,
        action: {
          label: "View Error",
          onClick: () => {
            setSelectedNodeId(errorNodeId);
            setDrawerOpen(true);
            setRightPanelMode("output");
            setRightPanelOpen(true);
          },
        },
      });
    }
    setLastDoneNodeId(null);
  }, [lastDoneNodeId, selectedNodeId, setLastDoneNodeId, nodeStatuses]);

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
    setRightPanelOpen(false);
  }, [pipelineId]);

  // Drawer + right panel state
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [rightPanelOpen, setRightPanelOpen] = useState(false);
  const [rightPanelMode, setRightPanelMode] = useState<"code" | "output">("code");

  // Escape key: close right panel first, then drawer, then clear selection
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        const tag = (e.target as HTMLElement)?.tagName;
        if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
        // Don't handle Escape if focus is inside Monaco editor — it handles its own
        const el = e.target as HTMLElement;
        if (el.closest(".monaco-editor")) return;
        if (rightPanelOpen) {
          setRightPanelOpen(false);
        } else if (drawerOpen) {
          setDrawerOpen(false);
          setSelectedNodeId(null);
        } else {
          setSelectedNodeId(null);
        }
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [drawerOpen, rightPanelOpen]);

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
    onMutate: (nodeId: string) => {
      useExecutionStore.getState().setNodeStatus(nodeId, "pending");
    },
    onSuccess: (data, nodeId) => {
      const store = useExecutionStore.getState();
      const downstream = pipeline
        ? getDownstreamNodeIds(nodeId, pipeline)
        : [nodeId];
      store.setAllPending(downstream);
      store.setRunning(true);
      store.setRunId(data.run_id);
    },
    onError: (err) => {
      const msg = err instanceof Error ? err.message : "";
      toast.error(msg.includes("409") ? "Already running" : "Failed to run");
    },
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
    (nodeId: string) => runFromMutation.mutate(nodeId),
    [runFromMutation],
  );

  const handleAddNodeFromToolbar = useCallback(
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
    if (nodeId) {
      // Clicking the already-selected node is a no-op (no toggle)
      if (nodeId === selectedNodeId) return;
      setSelectedNodeId(nodeId);
      setDrawerOpen(true);
      // Close right panel when selecting a different node
      setRightPanelOpen(false);
      const status = nodeStatuses[nodeId];
      if (status === "done" || status === "error" || status === "cached") {
        setRightPanelMode("output");
        setRightPanelOpen(true);
      }
    }
    // Clicking blank canvas — keep drawer open with last selected node
  }, [nodeStatuses, selectedNodeId]);

  const handleTabClick = useCallback((nodeId: string, tab: string) => {
    setSelectedNodeId(nodeId);
    setDrawerOpen(true);
    if (tab === "code") {
      setRightPanelMode("code");
      setRightPanelOpen(true);
    } else if (tab === "output") {
      setRightPanelMode("output");
      setRightPanelOpen(true);
    }
  }, []);

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
          onSuccess: () => {
            useExecutionStore.getState().markDirty(nodeId);
          },
          onError: () => {
            toast.error("Failed to save parameter");
            invalidate();
          },
        },
      );
    },
    [pipeline, patchNodeMutation, invalidate],
  );

  const handleClosePanel = useCallback(() => {
    setRightPanelOpen(false);
    setDrawerOpen(false);
    setSelectedNodeId(null);
  }, []);

  const handleCodeToggle = useCallback(() => {
    if (rightPanelOpen && rightPanelMode === "code") {
      setRightPanelOpen(false);
    } else {
      setRightPanelMode("code");
      setRightPanelOpen(true);
    }
  }, [rightPanelOpen, rightPanelMode]);

  const handleOutputToggle = useCallback(() => {
    if (rightPanelOpen && rightPanelMode === "output") {
      setRightPanelOpen(false);
    } else {
      setRightPanelMode("output");
      setRightPanelOpen(true);
    }
  }, [rightPanelOpen, rightPanelMode]);

  const handleRightPanelClose = useCallback(() => {
    setRightPanelOpen(false);
  }, []);

  const handleCodeSave = useCallback(
    (nodeId: string, code: string) => {
      patchNodeMutation.mutate(
        { nodeId, body: { code } },
        {
          onSuccess: () => {
            useExecutionStore.getState().markDirty(nodeId);
          },
          onError: () => toast.error("Failed to save code"),
        },
      );
    },
    [patchNodeMutation],
  );

  // ── Rename ─────────────────────────────────────────────────────
  const handleRenameFromContextMenu = useCallback(
    (nodeId: string) => {
      setSelectedNodeId(nodeId);
      setDrawerOpen(true);
    },
    [],
  );

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
            style={{ color: "var(--accent-primary)" }}
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
          setRightPanelMode("output");
          setRightPanelOpen(true);
          setRequestedRunId(runId);
        } : undefined}
      />
      <ErrorBoundary key={pipelineId} variant="compact">
        <Toolbar onAddNode={handleAddNodeFromToolbar} />
      </ErrorBoundary>
      <DisconnectionBanner />
      {/* Horizontal flex: canvas (+ optional right panel) */}
      <div className="flex flex-1 min-h-0">
          <main
            className="flex-1 min-h-0 overflow-hidden relative"
            style={{
              backgroundColor: "var(--canvas-bg)",
              transition: "width 250ms ease",
            }}
          >
            {showSkeleton && (
              <div
                className="absolute inset-0"
                style={{ backgroundColor: "var(--canvas-bg)", zIndex: 10 }}
              >
                {[
                  { x: "15%", y: "20%", w: 210, h: 100 },
                  { x: "45%", y: "15%", w: 210, h: 120 },
                  { x: "30%", y: "55%", w: 210, h: 90 },
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
            <ErrorBoundary key={pipelineId} variant="compact">
            <BottomDrawer
              pipelineId={pipelineId}
              node={drawerOpen ? selectedNode : null}
              definition={selectedDefinition}
              edges={pipeline?.edges ?? []}
              onParamChange={handleParamChange}
              paramSaving={patchNodeMutation.isPending}
              onClose={handleClosePanel}
              onRunFrom={handleRunFrom}
              onCodeClick={handleCodeToggle}
              onOutputClick={handleOutputToggle}
              rightPanelOpen={rightPanelOpen}
              rightPanelMode={rightPanelMode}
            />
            </ErrorBoundary>
          </main>

          {/* Right panel — CodeEditor or OutputPanel */}
          {rightPanelOpen && selectedNode && selectedDefinition && rightPanelMode === "code" && (
            <CodePane
              node={selectedNode}
              definition={selectedDefinition}
              pipelineId={pipelineId}
              onSave={handleCodeSave}
              onClose={handleRightPanelClose}
            />
          )}
          {rightPanelOpen && selectedNode && selectedDefinition && rightPanelMode === "output" && (
            <OutputPanel
              node={selectedNode}
              definition={selectedDefinition}
              pipelineId={pipelineId}
              onClose={handleRightPanelClose}
              onRunFrom={handleRunFrom}
              requestedRunId={requestedRunId}
              onRequestedRunHandled={() => setRequestedRunId(null)}
            />
          )}
      </div>
    </div>
  );
}
