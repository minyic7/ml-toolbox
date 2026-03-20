import { useState } from "react";
import type { DagNodeSnapshot } from "../../lib/types";

export interface DagThumbnailProps {
  dagSnapshot: DagNodeSnapshot[];
  selectedNodeId: string | null;
  onNodeClick: (nodeId: string) => void;
}

const NODE_W = 44;
const NODE_H = 22;
const GAP = 12;
const LABEL_H = 14;
const DOT_R = 3;
const RX = 4;
const PADDING_X = 8;
const PADDING_Y = 8;

const STATUS_COLORS: Record<string, { fill: string; dot: string; labelColor: string; opacity: number }> = {
  done:      { fill: "#10B981", dot: "#10B981", labelColor: "#1E293B", opacity: 0.12 },
  error:     { fill: "#E24B4A", dot: "#E24B4A", labelColor: "#1E293B", opacity: 0.12 },
  cancelled: { fill: "#CBD5E1", dot: "#CBD5E1", labelColor: "#94A3B8", opacity: 0.30 },
  pending:   { fill: "#CBD5E1", dot: "#CBD5E1", labelColor: "#94A3B8", opacity: 0.30 },
  skipped:   { fill: "#F59E0B", dot: "#F59E0B", labelColor: "#1E293B", opacity: 0.12 },
};

function getStatusStyle(status: string) {
  return STATUS_COLORS[status] ?? STATUS_COLORS.pending;
}

function hexToRgba(hex: string, alpha: number): string {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function truncateLabel(name: string, maxChars: number = 7): string {
  return name.length > maxChars ? name.slice(0, maxChars - 1) + "\u2026" : name;
}

export default function DagThumbnail({ dagSnapshot, selectedNodeId, onNodeClick }: DagThumbnailProps) {
  const [hoveredId, setHoveredId] = useState<string | null>(null);

  if (!dagSnapshot || dagSnapshot.length === 0) {
    return null;
  }

  const svgWidth = PADDING_X * 2 + dagSnapshot.length * NODE_W + (dagSnapshot.length - 1) * GAP;
  const svgHeight = PADDING_Y * 2 + NODE_H + LABEL_H;

  return (
    <div>
      <svg
        width={svgWidth}
        height={svgHeight}
        style={{ display: "block" }}
        role="img"
        aria-label="Pipeline DAG thumbnail"
      >
        {dagSnapshot.map((node, i) => {
          const x = PADDING_X + i * (NODE_W + GAP);
          const y = PADDING_Y;
          const style = getStatusStyle(node.status);
          const isSelected = selectedNodeId === node.node_id;
          const isHovered = hoveredId === node.node_id;

          return (
            <g
              key={node.node_id}
              onClick={() => onNodeClick(node.node_id)}
              onMouseEnter={() => setHoveredId(node.node_id)}
              onMouseLeave={() => setHoveredId(null)}
              style={{ cursor: "pointer" }}
            >
              {/* Connection line to next node */}
              {i < dagSnapshot.length - 1 && (
                <line
                  x1={x + NODE_W}
                  y1={y + NODE_H / 2}
                  x2={x + NODE_W + GAP}
                  y2={y + NODE_H / 2}
                  stroke="#E2E8F0"
                  strokeWidth={1}
                />
              )}

              {/* Node rectangle */}
              <rect
                x={x}
                y={y}
                width={NODE_W}
                height={NODE_H}
                rx={RX}
                fill={hexToRgba(style.fill, style.opacity)}
                stroke={isSelected ? "#4A4558" : isHovered ? "#94A3B8" : "none"}
                strokeWidth={isSelected ? 2 : 1}
              />

              {/* Status dot top-right */}
              <circle
                cx={x + NODE_W - DOT_R - 2}
                cy={y + DOT_R + 2}
                r={DOT_R}
                fill={style.dot}
              />

              {/* Node label */}
              <text
                x={x + NODE_W / 2}
                y={y + NODE_H / 2 + 3}
                textAnchor="middle"
                style={{
                  fontSize: 7,
                  fontFamily: "'Inter', sans-serif",
                  fill: style.labelColor,
                  pointerEvents: "none",
                  userSelect: "none",
                }}
              >
                {truncateLabel(node.node_name)}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Inline detail panel for selected node */}
      {selectedNodeId && (() => {
        const node = dagSnapshot.find((n) => n.node_id === selectedNodeId);
        if (!node) return null;

        if (node.status === "done") {
          return (
            <div style={detailStyles.done}>
              Completed — see artifacts below
            </div>
          );
        }
        if (node.status === "error") {
          return (
            <div style={detailStyles.error}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9 }}>
                Error
              </span>
            </div>
          );
        }
        return (
          <div style={detailStyles.pending}>
            Not executed
          </div>
        );
      })()}
    </div>
  );
}

const detailStyles: Record<string, React.CSSProperties> = {
  done: {
    fontSize: 11,
    fontFamily: "'Inter', sans-serif",
    color: "#166534",
    backgroundColor: "#DCFCE7",
    borderRadius: 6,
    padding: "6px 10px",
    marginTop: 6,
  },
  error: {
    fontSize: 11,
    fontFamily: "'Inter', sans-serif",
    color: "#FECDD3",
    backgroundColor: "#1A1625",
    borderRadius: 6,
    padding: "6px 10px",
    marginTop: 6,
  },
  pending: {
    fontSize: 11,
    fontFamily: "'Inter', sans-serif",
    color: "var(--text-muted, #94A3B8)",
    backgroundColor: "var(--canvas-bg, #F9F9FB)",
    borderRadius: 6,
    padding: "6px 10px",
    marginTop: 6,
  },
};
