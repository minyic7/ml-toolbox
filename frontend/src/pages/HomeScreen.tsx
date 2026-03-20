import { useState, useCallback, useRef, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  usePipelines,
  useCreatePipeline,
  useDeletePipeline,
  useDuplicatePipeline,
} from "../hooks/usePipeline";
import type { PipelineListItem } from "../lib/types";
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

export default function HomeScreen() {
  const navigate = useNavigate();
  const { data: pipelines, isLoading, isError, refetch } = usePipelines();
  const queryClient = useQueryClient();
  const createPipeline = useCreatePipeline();
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
    createPipeline.mutate("Untitled Pipeline", {
      onSuccess: (data) => navigate(`/pipeline/${data.id}`),
    });
  }, [createPipeline, navigate]);

  if (isLoading) {
    return (
      <div style={styles.page}>
        <div style={styles.container}>
          <p style={{ color: "var(--text-secondary)" }}>Loading...</p>
        </div>
      </div>
    );
  }

  if (isError) {
    return (
      <div style={styles.page}>
        <div style={styles.container}>
          <div style={styles.emptyState}>
            <AlertCircle
              className="h-8 w-8 mx-auto mb-3"
              style={{ color: "var(--error-red)" }}
            />
            <p style={styles.emptyTitle}>Failed to load pipelines</p>
            <Button variant="outline" onClick={() => refetch()}>
              <RefreshCw className="h-4 w-4" />
              Retry
            </Button>
          </div>
        </div>
      </div>
    );
  }

  const hasPipelines = pipelines && pipelines.length > 0;

  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <header style={styles.header}>
          <h1 style={styles.title}>ML Toolbox</h1>
          {hasPipelines && (
            <Button onClick={handleCreate} disabled={createPipeline.isPending}>
              <Plus className="h-4 w-4" />
              New Pipeline
            </Button>
          )}
        </header>

        {hasPipelines ? (
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
            <Button onClick={handleCreate} disabled={createPipeline.isPending}>
              Create your first pipeline
            </Button>
          </div>
        )}
      </div>
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
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [isRenaming, setIsRenaming] = useState(false);
  const [draft, setDraft] = useState(pipeline.name);
  const inputRef = useRef<HTMLInputElement>(null);

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
      setTimeout(() => inputRef.current?.classList.remove("shake"), 300);
      return;
    }
    setIsRenaming(false);
    if (trimmed !== pipeline.name) {
      onRename(trimmed);
    } else {
      setDraft(pipeline.name);
    }
  }, [draft, pipeline.name, onRename]);

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
              borderColor: "var(--accent-primary, #4A4558)",
            }}
          />
        ) : (
          <span style={styles.cardName}>{pipeline.name}</span>
        )}
        <DropdownMenu onOpenChange={() => setConfirmDelete(false)}>
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
            {confirmDelete ? (
              <DropdownMenuItem
                className="text-[var(--error-red)] focus:text-[var(--error-red)]"
                onClick={onDelete}
              >
                Confirm Delete
              </DropdownMenuItem>
            ) : (
              <DropdownMenuItem
                className="text-[var(--error-red)] focus:text-[var(--error-red)]"
                onClick={(e) => {
                  e.preventDefault();
                  setConfirmDelete(true);
                }}
              >
                Delete
              </DropdownMenuItem>
            )}
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
    padding: "48px 24px",
  },
  container: {
    maxWidth: 960,
    margin: "0 auto",
  },
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 32,
  },
  title: {
    fontSize: 24,
    fontWeight: 800,
    fontFamily: "'Manrope', sans-serif",
    color: "var(--text-primary)",
    margin: 0,
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
