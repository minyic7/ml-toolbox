import { Download, FileText } from "lucide-react";
import { getRunOutputDownloadUrl } from "../../lib/api";
import type { RunArtifact } from "../../lib/types";

export interface ArtifactsGridProps {
  artifacts: RunArtifact[];
  pipelineId: string;
  runId: string;
}

const TYPE_BADGE_COLORS: Record<string, { bg: string; color: string }> = {
  parquet: { bg: "#DCFCE7", color: "#166634" },
  pkl:     { bg: "#EDE9FE", color: "#5B21B6" },
  json:    { bg: "#FEF3C7", color: "#92400E" },
  npy:     { bg: "#DBEAFE", color: "#1D4ED8" },
  pt:      { bg: "#DBEAFE", color: "#1D4ED8" },
  png:     { bg: "#FCE7F3", color: "#9D174D" },
  svg:     { bg: "#FCE7F3", color: "#9D174D" },
};

function getBadgeStyle(type: string) {
  return TYPE_BADGE_COLORS[type] ?? { bg: "#F1F5F9", color: "#64748B" };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
}

function isChartType(type: string): boolean {
  return type === "png" || type === "svg";
}

function BarChart({ bars }: { bars: number[] }) {
  const max = Math.max(...bars, 1);
  const barWidth = Math.max(4, Math.floor(80 / bars.length) - 1);
  const chartWidth = bars.length * (barWidth + 1);
  const chartHeight = 50;

  return (
    <svg width={chartWidth} height={chartHeight} style={{ display: "block", margin: "0 auto" }}>
      {bars.map((val, i) => {
        const h = (val / max) * chartHeight;
        return (
          <rect
            key={i}
            x={i * (barWidth + 1)}
            y={chartHeight - h}
            width={barWidth}
            height={h}
            rx={1}
            fill="#7F77DD"
            opacity={0.7}
          />
        );
      })}
    </svg>
  );
}

function ChartCard({
  artifact,
  pipelineId,
  runId,
}: {
  artifact: RunArtifact;
  pipelineId: string;
  runId: string;
}) {
  const badge = getBadgeStyle(artifact.type);

  return (
    <div style={styles.card}>
      <div style={styles.previewArea}>
        {artifact.bars && artifact.bars.length > 0 ? (
          <BarChart bars={artifact.bars} />
        ) : (
          <FileText size={24} style={{ color: "#94A3B8" }} />
        )}
      </div>
      <div style={styles.cardFooter}>
        <div style={styles.nodeName}>{artifact.node_name}</div>
        <div style={styles.fileRow}>
          <span style={styles.filename}>
            {artifact.filename}
          </span>
          <span style={{ ...styles.badge, backgroundColor: badge.bg, color: badge.color }}>
            {artifact.type}
          </span>
          <span style={styles.fileSize}>{formatBytes(artifact.size)}</span>
          <a
            href={getRunOutputDownloadUrl(pipelineId, runId, artifact.node_id)}
            download
            onClick={(e) => e.stopPropagation()}
            style={styles.downloadBtn}
            title="Download"
          >
            <Download size={12} />
          </a>
        </div>
      </div>
    </div>
  );
}

function FileCard({
  artifact,
  pipelineId,
  runId,
}: {
  artifact: RunArtifact;
  pipelineId: string;
  runId: string;
}) {
  const badge = getBadgeStyle(artifact.type);

  return (
    <div style={styles.card}>
      <div style={styles.previewArea}>
        <FileText size={24} style={{ color: "#94A3B8" }} />
        <span
          style={{
            ...styles.typeBadge,
            backgroundColor: badge.bg,
            color: badge.color,
          }}
        >
          {artifact.type}
        </span>
      </div>
      <div style={styles.cardFooter}>
        <div style={styles.nodeName}>{artifact.node_name}</div>
        <div style={styles.fileRow}>
          <span style={styles.filename}>
            {artifact.filename}
          </span>
          <span style={styles.fileSize}>{formatBytes(artifact.size)}</span>
          <a
            href={getRunOutputDownloadUrl(pipelineId, runId, artifact.node_id)}
            download
            onClick={(e) => e.stopPropagation()}
            style={styles.downloadBtn}
            title="Download"
          >
            <Download size={12} />
          </a>
        </div>
      </div>
    </div>
  );
}

export default function ArtifactsGrid({ artifacts, pipelineId, runId }: ArtifactsGridProps) {
  if (!artifacts || artifacts.length === 0) {
    return (
      <div style={styles.emptyState}>
        No artifacts produced
      </div>
    );
  }

  return (
    <div style={styles.grid}>
      {artifacts.map((artifact, i) =>
        isChartType(artifact.type) ? (
          <ChartCard
            key={`${artifact.node_id}-${artifact.filename}-${i}`}
            artifact={artifact}
            pipelineId={pipelineId}
            runId={runId}
          />
        ) : (
          <FileCard
            key={`${artifact.node_id}-${artifact.filename}-${i}`}
            artifact={artifact}
            pipelineId={pipelineId}
            runId={runId}
          />
        )
      )}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
    gap: 6,
  },
  card: {
    backgroundColor: "var(--node-bg, #FFFFFF)",
    border: "1px solid var(--border-default, #E2E8F0)",
    borderRadius: 8,
    overflow: "hidden",
    cursor: "default",
    transition: "border-color 0.15s",
  },
  previewArea: {
    position: "relative" as const,
    height: 65,
    backgroundColor: "var(--canvas-bg, #F9F9FB)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  typeBadge: {
    position: "absolute" as const,
    top: 4,
    right: 4,
    fontSize: 8,
    fontWeight: 600,
    fontFamily: "'Inter', sans-serif",
    padding: "1px 4px",
    borderRadius: 3,
    textTransform: "uppercase" as const,
  },
  cardFooter: {
    padding: "4px 6px 6px",
    borderTop: "1px solid var(--border-default, #E2E8F0)",
  },
  nodeName: {
    fontSize: 7,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-muted, #94A3B8)",
    marginBottom: 2,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap" as const,
  },
  fileRow: {
    display: "flex",
    alignItems: "center",
    gap: 3,
  },
  filename: {
    fontSize: 9,
    fontFamily: "'JetBrains Mono', monospace",
    color: "var(--text-primary, #1E293B)",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap" as const,
    flex: 1,
    minWidth: 0,
  },
  badge: {
    fontSize: 7,
    fontWeight: 600,
    fontFamily: "'Inter', sans-serif",
    padding: "0px 3px",
    borderRadius: 3,
    textTransform: "uppercase" as const,
    flexShrink: 0,
  },
  fileSize: {
    fontSize: 9,
    fontFamily: "'JetBrains Mono', monospace",
    color: "var(--text-muted, #94A3B8)",
    flexShrink: 0,
  },
  downloadBtn: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    width: 16,
    height: 16,
    color: "var(--text-muted, #94A3B8)",
    flexShrink: 0,
    borderRadius: 3,
    textDecoration: "none",
  },
  emptyState: {
    textAlign: "center" as const,
    padding: "24px 0",
    fontSize: 12,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-muted, #94A3B8)",
  },
};
