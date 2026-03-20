/* ── Shared run status constants, color maps, and utility functions ── */

import type { GlobalRunRecord } from "./types";

/* 1. Status keys */
export const RUN_STATUSES = ["done", "error", "cancelled"] as const;
export type RunStatus = (typeof RUN_STATUSES)[number];

export const DAG_NODE_STATUSES = ["done", "error", "pending", "cancelled", "skipped"] as const;
export type DagNodeStatus = (typeof DAG_NODE_STATUSES)[number];

/* 2. Status badge colors (FilterRow, RunDetail) */
export const STATUS_BADGE_COLORS: Record<string, { bg: string; color: string }> = {
  done: { bg: "var(--status-done-bg)", color: "var(--status-done-text)" },
  error: { bg: "var(--error-bg-light)", color: "var(--error-red)" },
  cancelled: { bg: "var(--status-cancelled-bg)", color: "var(--status-cancelled-text)" },
};

/* 3. Status labels */
export const STATUS_LABELS: Record<string, string> = {
  done: "Success",
  error: "Failed",
  cancelled: "Cancelled",
};

/* 4. DAG node colors (DagThumbnail, TinyDag) */
export const DAG_NODE_COLORS: Record<string, { fill: string; dot: string; labelColor: string; opacity: number }> = {
  done: { fill: "var(--success-green)", dot: "var(--success-green)", labelColor: "var(--text-primary)", opacity: 0.12 },
  error: { fill: "var(--error-red)", dot: "var(--error-red)", labelColor: "var(--text-primary)", opacity: 0.12 },
  cancelled: { fill: "var(--status-idle)", dot: "var(--status-idle)", labelColor: "var(--text-muted)", opacity: 0.30 },
  pending: { fill: "var(--status-idle)", dot: "var(--status-idle)", labelColor: "var(--text-muted)", opacity: 0.30 },
  skipped: { fill: "var(--warning-amber)", dot: "var(--warning-amber)", labelColor: "var(--text-primary)", opacity: 0.12 },
};

/* 5. Artifact type badge colors (ArtifactsGrid) */
export const ARTIFACT_TYPE_COLORS: Record<string, { bg: string; color: string }> = {
  parquet: { bg: "var(--status-done-bg)", color: "var(--output-healthy-text)" },
  pkl: { bg: "var(--artifact-pkl-bg)", color: "var(--artifact-pkl-text)" },
  json: { bg: "var(--artifact-json-bg)", color: "var(--artifact-json-text)" },
  npy: { bg: "var(--artifact-npy-bg)", color: "var(--artifact-npy-text)" },
  pt: { bg: "var(--artifact-npy-bg)", color: "var(--artifact-npy-text)" },
  png: { bg: "var(--artifact-png-bg)", color: "var(--artifact-png-text)" },
  svg: { bg: "var(--artifact-png-bg)", color: "var(--artifact-png-text)" },
};

/* 6. Date grouping helpers */
export function dateLabel(iso: string): string {
  const d = new Date(iso);
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const target = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const diff = (today.getTime() - target.getTime()) / 86_400_000;
  if (diff === 0) return "Today";
  if (diff === 1) return "Yesterday";
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

export function groupByDate(runs: GlobalRunRecord[]): [string, GlobalRunRecord[]][] {
  const groups = new Map<string, GlobalRunRecord[]>();
  for (const run of runs) {
    const label = dateLabel(run.started_at);
    const list = groups.get(label);
    if (list) list.push(run);
    else groups.set(label, [run]);
  }
  return Array.from(groups.entries());
}

/* 7. Formatting helpers */

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

/* 8. Pipeline dot color helper */
const PIPELINE_DOT_COLORS = [
  "var(--category-ingest)",
  "var(--category-transform)",
  "var(--category-train)",
  "var(--category-evaluate)",
  "var(--category-export)",
  "var(--category-demo)",
];
export function pipelineDotColor(pipelineId: string): string {
  let hash = 0;
  for (let i = 0; i < pipelineId.length; i++) {
    hash = (hash * 31 + pipelineId.charCodeAt(i)) | 0;
  }
  return PIPELINE_DOT_COLORS[Math.abs(hash) % PIPELINE_DOT_COLORS.length];
}
