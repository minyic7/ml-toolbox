import { useState, useCallback } from "react";
import { CheckCircle2, XCircle, MinusCircle, ExternalLink } from "lucide-react";
import type { GlobalRunRecord } from "../../lib/types";
import {
  formatDuration,
  relativeTime,
  pipelineDotColor,
  STATUS_BADGE_COLORS,
  STATUS_LABELS,
} from "../../lib/runConstants";
import DagThumbnail from "./DagThumbnail";
import ArtifactsGrid from "./ArtifactsGrid";

export interface RunDetailProps {
  run: GlobalRunRecord | null;
  onOpenPipeline: (pipelineId: string) => void;
}

const STATUS_ICON: Record<string, typeof CheckCircle2> = {
  done: CheckCircle2,
  error: XCircle,
  cancelled: MinusCircle,
};

const STATUS_BADGE: Record<string, { bg: string; color: string; icon: typeof CheckCircle2; label: string }> = {
  done:      { bg: STATUS_BADGE_COLORS.done.bg, color: STATUS_BADGE_COLORS.done.color, icon: STATUS_ICON.done, label: STATUS_LABELS.done },
  error:     { bg: STATUS_BADGE_COLORS.error.bg, color: STATUS_BADGE_COLORS.error.color, icon: STATUS_ICON.error, label: STATUS_LABELS.error },
  cancelled: { bg: STATUS_BADGE_COLORS.cancelled.bg, color: STATUS_BADGE_COLORS.cancelled.color, icon: STATUS_ICON.cancelled, label: STATUS_LABELS.cancelled },
};

function getStatusBadge(status: string) {
  return STATUS_BADGE[status] ?? STATUS_BADGE.cancelled;
}

/* -- Header Sub-component -------------------------------------------------- */

function RunDetailHeader({
  run,
  onOpenPipeline,
}: {
  run: GlobalRunRecord;
  onOpenPipeline: (pipelineId: string) => void;
}) {
  const badge = getStatusBadge(run.status);
  const Icon = badge.icon;

  const totalNodes = run.dag_snapshot.length;
  const doneNodes = run.dag_snapshot.filter((n) => n.status === "done").length;
  const failedNode = run.dag_snapshot.find((n) => n.status === "error");
  const allDone = totalNodes > 0 && doneNodes === totalNodes;

  return (
    <div style={headerStyles.wrapper}>
      {/* Top row: badge + run ID + open pipeline */}
      <div style={headerStyles.topRow}>
        <div style={headerStyles.leftGroup}>
          <span
            style={{
              ...headerStyles.badge,
              backgroundColor: badge.bg,
              color: badge.color,
            }}
          >
            <Icon size={12} style={{ marginRight: 4 }} />
            {badge.label}
          </span>
          <span style={headerStyles.runId}>{run.id}</span>
        </div>
        <button
          style={headerStyles.openBtn}
          onClick={() => onOpenPipeline(run.pipeline_id)}
          title="Open pipeline"
        >
          Open pipeline
          <ExternalLink size={12} style={{ marginLeft: 4 }} />
        </button>
      </div>

      {/* Pipeline name + time */}
      <div style={headerStyles.metaRow}>
        <span
          style={{
            ...headerStyles.dot,
            backgroundColor: pipelineDotColor(run.pipeline_id),
          }}
        />
        <span style={headerStyles.pipelineName}>{run.pipeline_name}</span>
        <span style={headerStyles.metaSep}>&middot;</span>
        <span style={headerStyles.metaText}>{relativeTime(run.started_at)}</span>
        {run.duration != null && (
          <>
            <span style={headerStyles.metaSep}>&middot;</span>
            <span style={headerStyles.metaText}>{formatDuration(run.duration)}</span>
          </>
        )}
      </div>

      {/* Stat cards */}
      <div style={headerStyles.statsRow}>
        <StatCard label="Duration" value={formatDuration(run.duration)} />
        <StatCard
          label="Nodes"
          value={allDone ? `${totalNodes}/${totalNodes} \u2713` : `${doneNodes}/${totalNodes}`}
        />
        <StatCard label="Failed at" value={failedNode ? failedNode.node_name : "\u2014"} />
        <StatCard label="Artifacts" value={String(run.artifacts.length)} />
      </div>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div style={headerStyles.statCard}>
      <div style={headerStyles.statLabel}>{label}</div>
      <div style={headerStyles.statValue}>{value}</div>
    </div>
  );
}

/* -- Main RunDetail Component ---------------------------------------------- */

export default function RunDetail({ run, onOpenPipeline }: RunDetailProps) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  const handleNodeClick = useCallback((nodeId: string) => {
    setSelectedNodeId((prev) => (prev === nodeId ? null : nodeId));
  }, []);

  if (!run) {
    return (
      <div style={styles.emptyState}>
        Select a run to view details
      </div>
    );
  }

  return (
    <div style={styles.container}>
      <RunDetailHeader run={run} onOpenPipeline={onOpenPipeline} />

      {/* DAG Thumbnail */}
      {run.dag_snapshot.length > 0 && (
        <section style={styles.section}>
          <h3 style={styles.sectionTitle}>Pipeline DAG</h3>
          <DagThumbnail
            dagSnapshot={run.dag_snapshot}
            selectedNodeId={selectedNodeId}
            onNodeClick={handleNodeClick}
          />
        </section>
      )}

      {/* Artifacts Grid */}
      <section style={styles.section}>
        <h3 style={styles.sectionTitle}>Artifacts</h3>
        <ArtifactsGrid
          artifacts={run.artifacts}
          pipelineId={run.pipeline_id}
          runId={run.id}
        />
      </section>
    </div>
  );
}

/* -- Styles --------------------------------------------------------------- */

const styles: Record<string, React.CSSProperties> = {
  container: {
    flex: 1,
    overflowY: "auto",
    padding: "16px 20px",
    minWidth: 0,
  },
  emptyState: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: 13,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-muted, #94A3B8)",
  },
  section: {
    marginTop: 20,
  },
  sectionTitle: {
    fontSize: 9,
    fontWeight: 800,
    fontFamily: "'Inter', sans-serif",
    textTransform: "uppercase" as const,
    letterSpacing: "0.08em",
    color: "var(--text-muted, #94A3B8)",
    marginBottom: 8,
    margin: "0 0 8px 0",
  },
};

const headerStyles: Record<string, React.CSSProperties> = {
  wrapper: {
    paddingBottom: 16,
    borderBottom: "1px solid var(--border-default, #E2E8F0)",
  },
  topRow: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
    marginBottom: 8,
  },
  leftGroup: {
    display: "flex",
    alignItems: "center",
    gap: 8,
  },
  badge: {
    display: "inline-flex",
    alignItems: "center",
    fontSize: 11,
    fontWeight: 600,
    fontFamily: "'Inter', sans-serif",
    padding: "3px 10px",
    borderRadius: 9999,
  },
  runId: {
    fontSize: 11,
    fontFamily: "'JetBrains Mono', monospace",
    color: "var(--text-secondary, #475569)",
  },
  openBtn: {
    display: "inline-flex",
    alignItems: "center",
    fontSize: 11,
    fontWeight: 600,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-secondary, #475569)",
    background: "none",
    border: "1px solid var(--border-default, #E2E8F0)",
    borderRadius: 6,
    padding: "4px 10px",
    cursor: "pointer",
  },
  metaRow: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    marginBottom: 12,
  },
  dot: {
    width: 8,
    height: 8,
    borderRadius: "50%",
    flexShrink: 0,
  },
  pipelineName: {
    fontSize: 10,
    fontWeight: 500,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-muted, #94A3B8)",
  },
  metaSep: {
    fontSize: 10,
    color: "var(--text-muted, #94A3B8)",
  },
  metaText: {
    fontSize: 10,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-muted, #94A3B8)",
  },
  statsRow: {
    display: "grid",
    gridTemplateColumns: "repeat(4, 1fr)",
    gap: 8,
  },
  statCard: {
    backgroundColor: "var(--canvas-bg, #F9F9FB)",
    borderRadius: 6,
    padding: "8px 10px",
  },
  statLabel: {
    fontSize: 9,
    fontWeight: 700,
    fontFamily: "'Inter', sans-serif",
    textTransform: "uppercase" as const,
    letterSpacing: "0.06em",
    color: "var(--text-muted, #94A3B8)",
    marginBottom: 2,
  },
  statValue: {
    fontSize: 13,
    fontWeight: 600,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-primary, #1E293B)",
  },
};
