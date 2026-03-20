import { useState } from "react";
import type { DagNodeSnapshot } from "../../lib/types";
import { DAG_NODE_COLORS } from "../../lib/runConstants";

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

function getStatusStyle(status: string) {
  return DAG_NODE_COLORS[status] ?? DAG_NODE_COLORS.pending;
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
                  stroke="var(--border-default)"
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
                fill={style.fill}
                fillOpacity={style.opacity}
                stroke={isSelected ? "var(--accent-primary)" : isHovered ? "var(--text-muted)" : "none"}
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
    color: "var(--status-done-text)",
    backgroundColor: "var(--status-done-bg)",
    borderRadius: 6,
    padding: "6px 10px",
    marginTop: 6,
  },
  error: {
    fontSize: 11,
    fontFamily: "'Inter', sans-serif",
    color: "var(--error-border-light)",
    backgroundColor: "var(--codepane-bg)",
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
