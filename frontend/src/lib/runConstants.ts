/* ── Shared run status constants, color maps, and utility functions ── */

/* 1. Status keys */
export const RUN_STATUSES = ["done", "error", "cancelled"] as const;
export type RunStatus = (typeof RUN_STATUSES)[number];

export const DAG_NODE_STATUSES = ["done", "error", "pending", "cancelled", "skipped"] as const;
export type DagNodeStatus = (typeof DAG_NODE_STATUSES)[number];

/* 2. Status badge colors (FilterRow, RunDetail) */
export const STATUS_BADGE_COLORS: Record<string, { bg: string; color: string }> = {
  done: { bg: "#DCFCE7", color: "#166534" },
  error: { bg: "#FFF7F7", color: "#9E3F4E" },
  cancelled: { bg: "#F1F5F9", color: "#64748B" },
};

/* 3. Status labels */
export const STATUS_LABELS: Record<string, string> = {
  done: "Success",
  error: "Failed",
  cancelled: "Cancelled",
};

/* 4. DAG node colors (DagThumbnail, TinyDag) */
export const DAG_NODE_COLORS: Record<string, { fill: string; dot: string; labelColor: string; opacity: number }> = {
  done: { fill: "#10B981", dot: "#10B981", labelColor: "#1E293B", opacity: 0.12 },
  error: { fill: "#9E3F4E", dot: "#9E3F4E", labelColor: "#1E293B", opacity: 0.12 },
  cancelled: { fill: "#CBD5E1", dot: "#CBD5E1", labelColor: "#94A3B8", opacity: 0.30 },
  pending: { fill: "#CBD5E1", dot: "#CBD5E1", labelColor: "#94A3B8", opacity: 0.30 },
  skipped: { fill: "#F59E0B", dot: "#F59E0B", labelColor: "#1E293B", opacity: 0.12 },
};

/* 5. Artifact type badge colors (ArtifactsGrid) */
export const ARTIFACT_TYPE_COLORS: Record<string, { bg: string; color: string }> = {
  parquet: { bg: "#DCFCE7", color: "#166634" },
  pkl: { bg: "#EDE9FE", color: "#5B21B6" },
  json: { bg: "#FEF3C7", color: "#92400E" },
  npy: { bg: "#DBEAFE", color: "#1D4ED8" },
  pt: { bg: "#DBEAFE", color: "#1D4ED8" },
  png: { bg: "#FCE7F3", color: "#9D174D" },
  svg: { bg: "#FCE7F3", color: "#9D174D" },
};

/* 6. Formatting helpers */
export function formatDuration(seconds: number | null): string {
  if (seconds == null) return "\u2014";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return `${m}m ${s}s`;
}

export function relativeTime(iso: string): string {
  const now = Date.now();
  const then = new Date(iso).getTime();
  const diffSec = Math.round((now - then) / 1000);
  if (diffSec < 60) return "just now";
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  if (diffDay === 1) return "yesterday";
  return `${diffDay}d ago`;
}

/* 7. Pipeline dot color helper */
const PIPELINE_DOT_COLORS = ["#1D9E75", "#7F77DD", "#378ADD", "#EF9F27", "#D85A30", "#888780"];
export function pipelineDotColor(pipelineId: string): string {
  let hash = 0;
  for (let i = 0; i < pipelineId.length; i++) {
    hash = (hash * 31 + pipelineId.charCodeAt(i)) | 0;
  }
  return PIPELINE_DOT_COLORS[Math.abs(hash) % PIPELINE_DOT_COLORS.length];
}
