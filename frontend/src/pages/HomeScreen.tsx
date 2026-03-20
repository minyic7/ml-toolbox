import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  usePipelines,
  useCreatePipeline,
  useDeletePipeline,
  useDuplicatePipeline,
} from "../hooks/usePipeline";
import {
  createPipeline,
  getPipeline,
  updatePipeline,
} from "../lib/api";
import type { Pipeline, PipelineListItem, RunFilterParams } from "../lib/types";
import { RUN_STATUSES } from "../lib/runConstants";
import { useAllRuns } from "../hooks/useAllRuns";
import FilterRow from "../components/Home/FilterRow";
import RunList from "../components/Home/RunList";
import RunDetail from "../components/Home/RunDetail";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Plus, MoreHorizontal, AlertCircle, RefreshCw, Pencil } from "lucide-react";

type View = "runs" | "pipelines";

const ALL_STATUSES = [...RUN_STATUSES];

export default function HomeScreen() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // View state
  const [view, setView] = useState<View>("runs");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedPipelineId, setSelectedPipelineId] = useState<string | null>(null);
  const [selectedStatuses, setSelectedStatuses] = useState<string[]>(ALL_STATUSES);
  const [searchQuery, setSearchQuery] = useState("");

  // Data fetching
  const runFilterParams = useMemo<RunFilterParams>(() => {
    const params: RunFilterParams = {};
    if (selectedPipelineId) params.pipeline_id = selectedPipelineId;
    if (searchQuery) params.search = searchQuery;
    // Only send status param if exactly one status selected
    if (selectedStatuses.length === 1) {
      params.status = selectedStatuses[0];
    }
    // If all 3 selected or 0 selected, don't send status param
    return params;
  }, [selectedPipelineId, selectedStatuses, searchQuery]);

  const {
    data: runs,
    isLoading: runsLoading,
    isError: runsError,
    refetch: refetchRuns,
  } = useAllRuns(runFilterParams);

  const {
    data: pipelines,
    isLoading: pipelinesLoading,
    isError: pipelinesError,
    refetch: refetchPipelines,
  } = usePipelines();

  // Client-side status filtering when multiple (but not all) statuses selected
  const filteredRuns = useMemo(() => {
    if (!runs) return undefined;
    if (selectedStatuses.length === 0) return [];
    if (selectedStatuses.length === ALL_STATUSES.length || selectedStatuses.length === 1) {
      return runs;
    }
    // Multiple but not all — filter client-side
    return runs.filter((r) => selectedStatuses.includes(r.status));
  }, [runs, selectedStatuses]);

  // Auto-select most recent run on load
  useEffect(() => {
    if (filteredRuns && filteredRuns.length > 0 && selectedRunId === null) {
      setSelectedRunId(filteredRuns[0].id);
    }
  }, [filteredRuns, selectedRunId]);

  const selectedRun = useMemo(
    () => filteredRuns?.find((r) => r.id === selectedRunId) ?? null,
    [filteredRuns, selectedRunId],
  );

  // Pipeline mutations (for Pipelines view)
  const createPipelineMut = useCreatePipeline();
  const deletePipeline = useDeletePipeline();
  const duplicatePipeline = useDuplicatePipeline();
  const renamePipeline = useMutation({
    mutationFn: async ({ id, name }: { id: string; name: string }) => {
      const res = await fetch(`/api/pipelines/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      if (!res.ok) throw new Error("Rename failed");
      return res.json();
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
  });

  const handleCreate = useCallback(() => {
    createPipelineMut.mutate("Untitled Pipeline", {
      onSuccess: (data) => navigate(`/pipeline/${data.id}`),
    });
  }, [createPipelineMut, navigate]);

  const handleStatusToggle = useCallback((status: string) => {
    setSelectedStatuses((prev) => {
      if (prev.includes(status)) {
        // Don't allow deselecting all
        if (prev.length === 1) return prev;
        return prev.filter((s) => s !== status);
      }
      return [...prev, status];
    });
  }, []);

  const handleOpenPipeline = useCallback(
    (pipelineId: string) => navigate(`/pipeline/${pipelineId}`),
    [navigate],
  );

  // Pipeline list for filter dropdown
  const pipelineOptions = useMemo(
    () => (pipelines ?? []).map((p) => ({ id: p.id, name: p.name })),
    [pipelines],
  );

  return (
    <div style={styles.page}>
      {/* Topbar */}
      <div style={styles.topbar}>
        <span style={styles.title}>ML Toolbox</span>
        <div style={styles.viewToggle}>
          <button
            style={view === "runs" ? styles.viewBtnActive : styles.viewBtn}
            onClick={() => setView("runs")}
          >
            Runs
          </button>
          <button
            style={view === "pipelines" ? styles.viewBtnActive : styles.viewBtn}
            onClick={() => setView("pipelines")}
          >
            Pipelines
          </button>
        </div>
        <div style={styles.topbarRight}>
          {view === "pipelines" && (
            <Button
              onClick={handleCreate}
              disabled={createPipelineMut.isPending}
              size="sm"
            >
              <Plus className="h-4 w-4" />
              New Pipeline
            </Button>
          )}
        </div>
      </div>

      {/* Runs view */}
      {view === "runs" && (
        <div style={styles.runsContainer}>
          <FilterRow
            pipelines={pipelineOptions}
            selectedPipelineId={selectedPipelineId}
            onPipelineChange={setSelectedPipelineId}
            selectedStatuses={selectedStatuses}
            onStatusToggle={handleStatusToggle}
            searchQuery={searchQuery}
            onSearchChange={setSearchQuery}
            runCount={filteredRuns?.length ?? 0}
          />
          {runsError ? (
            <div style={styles.emptyState}>
              <AlertCircle
                className="h-8 w-8 mx-auto mb-3"
                style={{ color: "var(--error-red)" }}
              />
              <p style={styles.emptyTitle}>Failed to load runs</p>
              <Button variant="outline" onClick={() => refetchRuns()}>
                <RefreshCw className="h-4 w-4" />
                Retry
              </Button>
            </div>
          ) : (
            <div style={styles.splitLayout}>
              <div style={styles.runListPane}>
                <RunList
                  runs={filteredRuns ?? []}
                  selectedRunId={selectedRunId}
                  onSelectRun={setSelectedRunId}
                  isLoading={runsLoading}
                />
              </div>
              <div style={styles.runDetailPane}>
                <RunDetail
                  run={selectedRun}
                  onOpenPipeline={handleOpenPipeline}
                />
              </div>
            </div>
          )}
        </div>
      )}

      {/* Pipelines view */}
      {view === "pipelines" && (
        <div style={styles.pipelinesContainer}>
          {pipelinesLoading ? (
            <p style={{ color: "var(--text-secondary)" }}>Loading...</p>
          ) : pipelinesError ? (
            <div style={styles.emptyState}>
              <AlertCircle
                className="h-8 w-8 mx-auto mb-3"
                style={{ color: "var(--error-red)" }}
              />
              <p style={styles.emptyTitle}>Failed to load pipelines</p>
              <Button variant="outline" onClick={() => refetchPipelines()}>
                <RefreshCw className="h-4 w-4" />
                Retry
              </Button>
            </div>
          ) : pipelines && pipelines.length > 0 ? (
            <div style={styles.grid}>
              {pipelines.map((p) => (
                <PipelineCard
                  key={p.id}
                  pipeline={p}
                  onOpen={() => navigate(`/pipeline/${p.id}`)}
                  onRename={(name) => renamePipeline.mutate({ id: p.id, name })}
                  onDuplicate={() =>
                    duplicatePipeline.mutate(p.id, {
                      onSuccess: (data) => navigate(`/pipeline/${data.id}`),
                    })
                  }
                  onDelete={() => deletePipeline.mutate(p.id)}
                />
              ))}
            </div>
          ) : (
            <div style={styles.emptyState}>
              <p style={styles.emptyTitle}>No pipelines yet</p>
              <Button onClick={handleCreate} disabled={createPipelineMut.isPending}>
                Create your first pipeline
              </Button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* -- Pipeline Card -------------------------------------------------------- */

function PipelineCard({
  pipeline,
  onOpen,
  onRename,
  onDuplicate,
  onDelete,
}: {
  pipeline: PipelineListItem;
  onOpen: () => void;
  onRename: (name: string) => void;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  const [isRenaming, setIsRenaming] = useState(false);
  const [draft, setDraft] = useState(pipeline.name);
  const inputRef = useRef<HTMLInputElement>(null);
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!isRenaming) setDraft(pipeline.name);
  }, [pipeline.name, isRenaming]);

  useEffect(() => {
    if (isRenaming) inputRef.current?.select();
  }, [isRenaming]);

  const commitRename = useCallback(() => {
    const trimmed = draft.trim();
    if (!trimmed) {
      inputRef.current?.classList.add("shake");
      setTimeout(() => {
        inputRef.current?.classList.remove("shake");
        setDraft(pipeline.name);
        setIsRenaming(false);
      }, 300);
      return;
    }
    setIsRenaming(false);
    if (trimmed !== pipeline.name) {
      onRename(trimmed);
    } else {
      setDraft(pipeline.name);
    }
  }, [draft, pipeline.name, onRename]);

  const handleDelete = useCallback(async () => {
    let snapshot: Pipeline | undefined;
    try {
      snapshot = await getPipeline(pipeline.id);
    } catch {
      // If we can't fetch the full data, fall back to name-only restore
    }
    onDelete();
    toast(`Deleted '${pipeline.name}'`, {
      action: {
        label: "Undo",
        onClick: () => {
          createPipeline({ name: snapshot?.name ?? pipeline.name })
            .then((created) => {
              if (snapshot) {
                return updatePipeline(created.id, {
                  ...snapshot,
                  id: created.id,
                });
              }
            })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ["pipelines"] });
            });
        },
      },
      duration: 4000,
    });
  }, [pipeline.id, pipeline.name, onDelete, queryClient]);

  return (
    <div
      style={styles.card}
      onClick={isRenaming ? undefined : onOpen}
    >
      <div style={styles.cardHeader}>
        {isRenaming ? (
          <Input
            ref={inputRef}
            type="text"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onBlur={commitRename}
            onKeyDown={(e) => {
              if (e.key === "Enter") commitRename();
              if (e.key === "Escape") {
                setDraft(pipeline.name);
                setIsRenaming(false);
              }
            }}
            onClick={(e) => e.stopPropagation()}
            className="max-w-48 h-7 px-1.5 py-0.5"
            style={{
              fontSize: 16,
              fontWeight: 500,
              color: "var(--text-primary)",
              borderColor: "var(--accent-primary)",
            }}
          />
        ) : (
          <span style={styles.cardName}>{pipeline.name}</span>
        )}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-[var(--text-secondary)]"
              onClick={(e) => e.stopPropagation()}
              aria-label="Pipeline actions"
            >
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            align="end"
            onClick={(e) => e.stopPropagation()}
          >
            <DropdownMenuItem
              onClick={() => setIsRenaming(true)}
            >
              <Pencil className="h-4 w-4 mr-2" />
              Rename
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onDuplicate}>
              Duplicate
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              className="text-[var(--error-red)] focus:text-[var(--error-red)]"
              onClick={handleDelete}
            >
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
      <p style={styles.cardMeta}>
        {pipeline.node_count} {pipeline.node_count === 1 ? "node" : "nodes"}
      </p>
    </div>
  );
}

/* -- Styles --------------------------------------------------------------- */

const styles: Record<string, React.CSSProperties> = {
  page: {
    minHeight: "100vh",
    backgroundColor: "var(--canvas-bg)",
    display: "flex",
    flexDirection: "column",
  },
  topbar: {
    height: 44,
    display: "flex",
    alignItems: "center",
    padding: "0 20px",
    borderBottom: "1px solid var(--border-default)",
    backgroundColor: "var(--node-bg)",
    flexShrink: 0,
    gap: 24,
  },
  title: {
    fontSize: 16,
    fontWeight: 800,
    fontFamily: "'Manrope', sans-serif",
    color: "#4A4558",
    letterSpacing: "-0.02em",
    whiteSpace: "nowrap",
  },
  viewToggle: {
    display: "flex",
    gap: 4,
    flex: 1,
  },
  viewBtn: {
    background: "none",
    border: "none",
    borderBottom: "2px solid transparent",
    padding: "10px 12px",
    fontFamily: "'Inter', sans-serif",
    fontWeight: 700,
    fontSize: 11,
    textTransform: "uppercase" as const,
    letterSpacing: "0.04em",
    color: "var(--text-muted)",
    cursor: "pointer",
  },
  viewBtnActive: {
    background: "none",
    border: "none",
    borderBottom: "2px solid var(--accent-primary, #4A4558)",
    padding: "10px 12px",
    fontFamily: "'Inter', sans-serif",
    fontWeight: 700,
    fontSize: 11,
    textTransform: "uppercase" as const,
    letterSpacing: "0.04em",
    color: "var(--text-primary)",
    cursor: "pointer",
  },
  topbarRight: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    marginLeft: "auto",
  },
  runsContainer: {
    display: "flex",
    flexDirection: "column",
    flex: 1,
    overflow: "hidden",
  },
  splitLayout: {
    display: "flex",
    flex: 1,
    overflow: "hidden",
  },
  runListPane: {
    width: 240,
    flexShrink: 0,
    borderRight: "1px solid var(--border-default)",
    overflow: "auto",
  },
  runDetailPane: {
    flex: 1,
    overflow: "auto",
  },
  pipelinesContainer: {
    maxWidth: 960,
    margin: "0 auto",
    padding: "32px 24px",
    width: "100%",
  },
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
    gap: 16,
  },
  card: {
    position: "relative" as const,
    backgroundColor: "var(--node-bg)",
    border: "1px solid var(--border-default)",
    borderRadius: 8,
    padding: 20,
    cursor: "pointer",
    transition: "border-color 0.15s",
  },
  cardHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  cardName: {
    fontSize: 16,
    fontWeight: 500,
    color: "var(--text-primary)",
  },
  cardMeta: {
    fontSize: 13,
    color: "var(--text-secondary)",
    margin: "8px 0 0",
  },
  emptyState: {
    textAlign: "center" as const,
    paddingTop: 120,
  },
  emptyTitle: {
    fontSize: 18,
    color: "var(--text-secondary)",
    marginBottom: 16,
  },
};
