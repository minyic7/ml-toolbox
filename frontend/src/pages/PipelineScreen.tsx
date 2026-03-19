import { useCallback, useMemo } from "react";
import { useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import * as api from "../lib/api";
import type { NodeDefinition } from "../lib/types";
import Topbar from "../components/Topbar/Topbar";
import Canvas from "../components/Canvas/Canvas";

export default function PipelineScreen() {
  const { id } = useParams<{ id: string }>();
  const pipelineId = id!;
  const queryClient = useQueryClient();

  // ── Server data ────────────────────────────────────────────────
  const { data: pipeline } = useQuery({
    queryKey: ["pipeline", pipelineId],
    queryFn: () => api.getPipeline(pipelineId),
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

  // ── Mutations ──────────────────────────────────────────────────
  const invalidate = useCallback(
    () => queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] }),
    [queryClient, pipelineId],
  );

  const patchNodePosition = useMutation({
    mutationFn: ({ nodeId, position }: { nodeId: string; position: { x: number; y: number } }) =>
      api.patchNode(pipelineId, nodeId, { position }),
    onSuccess: invalidate,
  });

  const addEdgeMutation = useMutation({
    mutationFn: (conn: { source: string; sourcePort: string; target: string; targetPort: string }) =>
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
    onSuccess: invalidate,
  });

  const deleteEdgeMutation = useMutation({
    mutationFn: (edgeId: string) => api.deleteEdge(pipelineId, edgeId),
    onSuccess: invalidate,
  });

  const addNodeMutation = useMutation({
    mutationFn: ({ type, position }: { type: string; position: { x: number; y: number } }) =>
      api.addNode(pipelineId, { type, position }),
    onSuccess: invalidate,
  });

  const runFromMutation = useMutation({
    mutationFn: (nodeId: string) => api.runFromNode(pipelineId, nodeId),
  });

  // ── Handlers ───────────────────────────────────────────────────
  const handleNodePositionChange = useCallback(
    (nodeId: string, position: { x: number; y: number }) => {
      patchNodePosition.mutate({ nodeId, position });
    },
    [patchNodePosition],
  );

  const handleConnect = useCallback(
    (conn: { source: string; sourcePort: string; target: string; targetPort: string }) => {
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

  // ── Render ─────────────────────────────────────────────────────
  if (!pipeline) {
    return (
      <div
        style={{
          width: "100vw",
          height: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--text-muted)",
        }}
      >
        Loading pipeline…
      </div>
    );
  }

  if (!id) return null;

  return (
    <div className="flex flex-col h-screen">
      <Topbar pipelineId={id} />
      <main className="flex-1 overflow-hidden" style={{ backgroundColor: "var(--canvas-bg)" }}>
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
        />
      </main>
    </div>
  );
}
