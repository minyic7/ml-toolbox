import { CheckCircle2, XCircle, MinusCircle } from "lucide-react";
import type { GlobalRunRecord } from "../../lib/types";
import { formatDuration, relativeTime, pipelineDotColor } from "../../lib/runConstants";
import TinyDag from "./TinyDag";

interface RunListProps {
  runs: GlobalRunRecord[];
  selectedRunId: string | null;
  onSelectRun: (runId: string) => void;
  isLoading: boolean;
}

function dateLabel(iso: string): string {
  const d = new Date(iso);
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const target = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const diff = (today.getTime() - target.getTime()) / 86_400_000;
  if (diff === 0) return "Today";
  if (diff === 1) return "Yesterday";
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function groupByDate(runs: GlobalRunRecord[]): [string, GlobalRunRecord[]][] {
  const groups = new Map<string, GlobalRunRecord[]>();
  for (const run of runs) {
    const label = dateLabel(run.started_at);
    const list = groups.get(label);
    if (list) list.push(run);
    else groups.set(label, [run]);
  }
  return Array.from(groups.entries());
}

const STATUS_ICON_COLOR: Record<string, string> = {
  done: "var(--success-green)",
  error: "var(--error-red)",
  cancelled: "#94A3B8",
};

function StatusIcon({ status }: { status: string }) {
  const color = STATUS_ICON_COLOR[status] ?? "#CBD5E1";
  const size = 18;
  if (status === "done") return <CheckCircle2 size={size} color={color} style={{ flexShrink: 0 }} />;
  if (status === "error") return <XCircle size={size} color={color} style={{ flexShrink: 0 }} />;
  return <MinusCircle size={size} color={color} style={{ flexShrink: 0 }} />;
}

/* ── Component ───────────────────────────────────────────────────── */

export default function RunList({ runs, selectedRunId, onSelectRun, isLoading }: RunListProps) {
  if (isLoading) {
    return (
      <div style={styles.container}>
        {[1, 2, 3, 4].map((i) => (
          <div key={i} style={styles.skeletonRow}>
            <div style={{ ...styles.skeletonRect, width: 18, height: 18, borderRadius: "50%" }} />
            <div style={{ flex: 1 }}>
              <div style={{ ...styles.skeletonRect, width: "70%", height: 10, marginBottom: 6 }} />
              <div style={{ ...styles.skeletonRect, width: "50%", height: 8 }} />
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (runs.length === 0) {
    return (
      <div style={styles.container}>
        <p style={styles.emptyText}>No runs yet</p>
      </div>
    );
  }

  const groups = groupByDate(runs);

  return (
    <div style={styles.container}>
      {groups.map(([label, groupRuns]) => (
        <div key={label}>
          <div style={styles.dateHeader}>{label}</div>
          {groupRuns.map((run) => {
            const isSelected = run.id === selectedRunId;
            return (
              <div
                key={run.id}
                style={{
                  ...styles.row,
                  ...(isSelected ? styles.rowSelected : {}),
                }}
                onClick={() => onSelectRun(run.id)}
              >
                {/* Row 1: status icon + run ID + duration */}
                <div style={styles.rowTop}>
                  <StatusIcon status={run.status} />
                  <span style={styles.runId}>{run.id.slice(0, 8)}</span>
                  <span style={styles.duration}>{formatDuration(run.duration)}</span>
                </div>
                {/* Row 2: pipeline dot + name + relative time */}
                <div style={styles.rowMid}>
                  <span
                    style={{
                      ...styles.pipelineDot,
                      backgroundColor: pipelineDotColor(run.pipeline_id),
                    }}
                  />
                  <span style={styles.pipelineName}>{run.pipeline_name}</span>
                  <span style={styles.separator}>&middot;</span>
                  <span style={styles.relTime}>{relativeTime(run.started_at)}</span>
                </div>
                {/* Row 3: tiny DAG dots */}
                {run.dag_snapshot.length > 0 && (
                  <div style={styles.rowBottom}>
                    <TinyDag nodes={run.dag_snapshot} />
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

/* ── Styles ───────────────────────────────────────────────────────── */

const styles: Record<string, React.CSSProperties> = {
  container: {
    width: 240,
    height: "100%",
    overflowY: "auto",
    borderRight: "1px solid var(--border-default)",
    backgroundColor: "var(--node-bg)",
  },
  dateHeader: {
    position: "sticky",
    top: 0,
    zIndex: 1,
    padding: "8px 12px 4px",
    fontSize: 8,
    fontFamily: "'Inter', sans-serif",
    fontWeight: 800,
    textTransform: "uppercase",
    letterSpacing: "0.08em",
    color: "var(--text-muted)",
    backgroundColor: "var(--node-bg)",
  },
  row: {
    padding: "8px 12px",
    cursor: "pointer",
    borderLeft: "2px solid transparent",
    transition: "background-color 0.1s",
  },
  rowSelected: {
    backgroundColor: "#F1F5F9",
    borderLeft: "2px solid var(--accent-primary)",
  },
  rowTop: {
    display: "flex",
    alignItems: "center",
    gap: 6,
  },
  runId: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 9,
    color: "var(--text-primary)",
    flex: 1,
  },
  duration: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 8,
    color: "var(--text-muted)",
    textAlign: "right",
  },
  rowMid: {
    display: "flex",
    alignItems: "center",
    gap: 4,
    marginTop: 3,
    paddingLeft: 24,
  },
  pipelineDot: {
    display: "inline-block",
    width: 6,
    height: 6,
    borderRadius: "50%",
    flexShrink: 0,
  },
  pipelineName: {
    fontFamily: "'Inter', sans-serif",
    fontSize: 8,
    color: "var(--text-muted)",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  separator: {
    fontSize: 8,
    color: "var(--text-muted)",
  },
  relTime: {
    fontFamily: "'Inter', sans-serif",
    fontSize: 8,
    color: "var(--text-muted)",
    whiteSpace: "nowrap",
  },
  rowBottom: {
    marginTop: 4,
    paddingLeft: 24,
  },
  emptyText: {
    fontFamily: "'Inter', sans-serif",
    fontSize: 12,
    color: "var(--text-muted)",
    textAlign: "center",
    padding: "48px 12px",
  },
  skeletonRow: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "10px 12px",
  },
  skeletonRect: {
    backgroundColor: "#E2E8F0",
    borderRadius: 4,
    animation: "pulse 1.5s ease-in-out infinite",
  },
};
