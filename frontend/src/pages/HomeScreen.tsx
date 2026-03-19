import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  usePipelines,
  useCreatePipeline,
  useDeletePipeline,
  useDuplicatePipeline,
} from "../hooks/usePipeline";
import type { PipelineListItem } from "../lib/types";

export default function HomeScreen() {
  const navigate = useNavigate();
  const { data: pipelines, isLoading } = usePipelines();
  const createPipeline = useCreatePipeline();
  const deletePipeline = useDeletePipeline();
  const duplicatePipeline = useDuplicatePipeline();

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

  const hasPipelines = pipelines && pipelines.length > 0;

  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <header style={styles.header}>
          <h1 style={styles.title}>ML Toolbox</h1>
          {hasPipelines && (
            <button
              style={styles.createButton}
              onClick={handleCreate}
              disabled={createPipeline.isPending}
            >
              + New Pipeline
            </button>
          )}
        </header>

        {hasPipelines ? (
          <div style={styles.grid}>
            {pipelines.map((p) => (
              <PipelineCard
                key={p.id}
                pipeline={p}
                onOpen={() => navigate(`/pipeline/${p.id}`)}
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
            <button
              style={styles.createButton}
              onClick={handleCreate}
              disabled={createPipeline.isPending}
            >
              Create your first pipeline
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Pipeline Card ────────────────────────────────────────────────── */

function PipelineCard({
  pipeline,
  onOpen,
  onDuplicate,
  onDelete,
}: {
  pipeline: PipelineListItem;
  onOpen: () => void;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  return (
    <div
      style={styles.card}
      onClick={onOpen}
      onMouseLeave={() => {
        setMenuOpen(false);
        setConfirmDelete(false);
      }}
    >
      <div style={styles.cardHeader}>
        <span style={styles.cardName}>{pipeline.name}</span>
        <button
          style={styles.menuButton}
          onClick={(e) => {
            e.stopPropagation();
            setMenuOpen((v) => !v);
            setConfirmDelete(false);
          }}
          aria-label="Pipeline actions"
        >
          &#x2026;
        </button>
      </div>
      <p style={styles.cardMeta}>
        {pipeline.node_count} {pipeline.node_count === 1 ? "node" : "nodes"}
      </p>

      {menuOpen && (
        <div style={styles.menu} onClick={(e) => e.stopPropagation()}>
          <button
            style={styles.menuItem}
            onClick={() => {
              setMenuOpen(false);
              onDuplicate();
            }}
          >
            Duplicate
          </button>
          {confirmDelete ? (
            <button
              style={{ ...styles.menuItem, color: "var(--error-red)" }}
              onClick={() => {
                setMenuOpen(false);
                setConfirmDelete(false);
                onDelete();
              }}
            >
              Confirm Delete
            </button>
          ) : (
            <button
              style={{ ...styles.menuItem, color: "var(--error-red)" }}
              onClick={() => setConfirmDelete(true)}
            >
              Delete
            </button>
          )}
        </div>
      )}
    </div>
  );
}

/* ── Styles ───────────────────────────────────────────────────────── */

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
    fontWeight: 600,
    color: "var(--text-primary)",
    margin: 0,
  },
  createButton: {
    padding: "10px 20px",
    backgroundColor: "var(--accent-blue)",
    color: "#fff",
    border: "none",
    borderRadius: 6,
    fontSize: 14,
    fontWeight: 500,
    cursor: "pointer",
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
  menuButton: {
    background: "none",
    border: "none",
    fontSize: 20,
    lineHeight: 1,
    color: "var(--text-secondary)",
    cursor: "pointer",
    padding: "0 4px",
    borderRadius: 4,
  },
  menu: {
    position: "absolute" as const,
    top: 44,
    right: 12,
    backgroundColor: "var(--node-bg)",
    border: "1px solid var(--border-default)",
    borderRadius: 6,
    boxShadow: "0 4px 12px rgba(0,0,0,0.08)",
    zIndex: 10,
    overflow: "hidden",
    minWidth: 140,
  },
  menuItem: {
    display: "block",
    width: "100%",
    padding: "8px 16px",
    background: "none",
    border: "none",
    textAlign: "left" as const,
    fontSize: 14,
    color: "var(--text-primary)",
    cursor: "pointer",
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
