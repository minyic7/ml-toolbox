import { useState, useEffect, useCallback } from "react";
import { Canvas } from "@/components/Canvas";
import { Sidebar } from "@/components/Panel/Sidebar";
import { RightPanel } from "@/components/Panel/RightPanel";
import { usePipeline, type PipelineSummary, type Toast } from "@/hooks/usePipeline";
import type { NodeDefinition } from "@/lib/types";
import type { NodeTab } from "@/components/Canvas";
import * as api from "@/lib/api";

// ── Toast overlay ───────────────────────────────────────────────────

function ToastContainer({
  toasts,
  onDismiss,
}: {
  toasts: Toast[];
  onDismiss: (id: number) => void;
}) {
  if (toasts.length === 0) return null;
  return (
    <div className="fixed right-4 top-4 z-50 flex flex-col gap-2">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`flex items-center gap-2 rounded-lg px-4 py-2 text-sm shadow-lg ${
            t.type === "error"
              ? "bg-red-900/90 text-red-100"
              : "bg-zinc-800/90 text-zinc-100"
          }`}
        >
          <span className="flex-1">{t.message}</span>
          <button
            type="button"
            onClick={() => onDismiss(t.id)}
            className="ml-2 text-xs opacity-60 hover:opacity-100"
          >
            ✕
          </button>
        </div>
      ))}
    </div>
  );
}

// ── Pipeline list header ────────────────────────────────────────────

function PipelineHeader({
  pipelines,
  currentId,
  onSelect,
  onCreate,
  onDelete,
}: {
  pipelines: PipelineSummary[];
  currentId?: string;
  onSelect: (id: string) => void;
  onCreate: () => void;
  onDelete: (id: string) => void;
}) {
  const [showDelete, setShowDelete] = useState<string | null>(null);

  return (
    <div className="flex h-10 shrink-0 items-center gap-2 border-b border-border bg-secondary px-3">
      <label className="text-xs font-medium text-muted-foreground">
        Pipeline:
      </label>
      <select
        className="rounded border border-border bg-background px-2 py-1 text-xs text-foreground"
        value={currentId ?? ""}
        onChange={(e) => {
          if (e.target.value) onSelect(e.target.value);
        }}
      >
        <option value="">— select —</option>
        {pipelines.map((p) => (
          <option key={p.id} value={p.id}>
            {p.name}
          </option>
        ))}
      </select>

      <button
        type="button"
        onClick={onCreate}
        className="rounded bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
      >
        + New
      </button>

      {currentId && (
        <>
          {showDelete === currentId ? (
            <div className="flex items-center gap-1">
              <span className="text-xs text-red-400">Delete?</span>
              <button
                type="button"
                onClick={() => {
                  onDelete(currentId);
                  setShowDelete(null);
                }}
                className="rounded bg-red-600 px-1.5 py-0.5 text-xs text-white hover:bg-red-700"
              >
                Yes
              </button>
              <button
                type="button"
                onClick={() => setShowDelete(null)}
                className="rounded bg-zinc-600 px-1.5 py-0.5 text-xs text-white hover:bg-zinc-700"
              >
                No
              </button>
            </div>
          ) : (
            <button
              type="button"
              onClick={() => setShowDelete(currentId)}
              className="rounded px-1.5 py-0.5 text-xs text-red-400 hover:bg-red-900/30"
            >
              Delete
            </button>
          )}
        </>
      )}
    </div>
  );
}

// ── Main App ────────────────────────────────────────────────────────

export default function App() {
  const [definitions, setDefinitions] = useState<NodeDefinition[]>([]);

  // Fetch node definitions from API on mount
  useEffect(() => {
    api.fetchNodes().then(setDefinitions).catch(() => {
      // Fallback: definitions stay empty, sidebar shows "No nodes found"
    });
  }, []);

  const pipeline = usePipeline(definitions);

  const handleNodeSelect = useCallback(
    (nodeId?: string, tab?: string) => {
      pipeline.selectNode(nodeId, tab as NodeTab | undefined);
    },
    [pipeline.selectNode],
  );

  const handleParamsChange = useCallback(
    (params: Record<string, string | number | boolean>) => {
      if (pipeline.selectedNodeId) {
        pipeline.updateNodeData(pipeline.selectedNodeId, { params });
      }
    },
    [pipeline.selectedNodeId, pipeline.updateNodeData],
  );

  const handleCodeChange = useCallback(
    (code: string) => {
      if (pipeline.selectedNodeId) {
        pipeline.updateNodeData(pipeline.selectedNodeId, { code });
      }
    },
    [pipeline.selectedNodeId, pipeline.updateNodeData],
  );

  const handleCreate = useCallback(() => {
    const name = prompt("Pipeline name:");
    if (name?.trim()) {
      pipeline.createPipeline(name.trim());
    }
  }, [pipeline.createPipeline]);

  const rightPanelOpen = pipeline.selectedNode !== null;

  return (
    <div className="flex h-screen w-screen flex-col overflow-hidden bg-background text-foreground">
      {/* Top bar with pipeline selector */}
      <PipelineHeader
        pipelines={pipeline.pipelines}
        currentId={pipeline.currentPipeline?.id}
        onSelect={pipeline.loadPipeline}
        onCreate={handleCreate}
        onDelete={pipeline.deletePipeline}
      />

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar - left */}
        <div className="w-[250px] shrink-0 border-r border-border">
          <Sidebar definitions={definitions} />
        </div>

        {/* Canvas - center */}
        <div className="flex-1">
          {pipeline.currentPipeline ? (
            <Canvas
              nodes={pipeline.nodes}
              edges={pipeline.edges}
              onNodesChange={pipeline.onNodesChange}
              onEdgesChange={pipeline.onEdgesChange}
              onConnect={pipeline.onConnect}
              onNodeSelect={handleNodeSelect}
              onDropNode={pipeline.addNode}
            />
          ) : (
            <div className="flex h-full items-center justify-center">
              <div className="text-center text-muted-foreground">
                {pipeline.loading ? (
                  <p className="text-sm">Loading...</p>
                ) : (
                  <>
                    <p className="mb-2 text-sm">
                      Select a pipeline or create a new one.
                    </p>
                    <button
                      type="button"
                      onClick={handleCreate}
                      className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700"
                    >
                      + New Pipeline
                    </button>
                  </>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Right panel - conditionally shown */}
        {rightPanelOpen && (
          <div className="w-[350px] shrink-0 border-l border-border">
            <RightPanel
              node={pipeline.selectedNode}
              activeTab={pipeline.selectedTab}
              onTabChange={(tab) => pipeline.selectNode(pipeline.selectedNodeId, tab)}
              onParamsChange={handleParamsChange}
              onCodeChange={handleCodeChange}
              onClose={() => pipeline.selectNode(undefined)}
            />
          </div>
        )}
      </div>

      {/* Toast notifications */}
      <ToastContainer toasts={pipeline.toasts} onDismiss={pipeline.dismissToast} />
    </div>
  );
}
