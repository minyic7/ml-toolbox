import type { DagNodeSnapshot } from "../../lib/types";

interface TinyDagProps {
  nodes: DagNodeSnapshot[];
}

const STATUS_COLORS: Record<string, string> = {
  done: "#10B981",
  error: "#9E3F4E",
  pending: "#CBD5E1",
  skipped: "#F59E0B",
};

const CIRCLE_R = 3;
const SPACING = 14;
const LINE_COLOR = "#E2E8F0";

export default function TinyDag({ nodes }: TinyDagProps) {
  if (nodes.length === 0) return null;

  const width = (nodes.length - 1) * SPACING + CIRCLE_R * 2;
  const height = CIRCLE_R * 2;

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      style={{ display: "block", flexShrink: 0 }}
    >
      {/* Connecting lines */}
      {nodes.map((_, i) => {
        if (i === 0) return null;
        return (
          <line
            key={`line-${i}`}
            x1={CIRCLE_R + (i - 1) * SPACING}
            y1={CIRCLE_R}
            x2={CIRCLE_R + i * SPACING}
            y2={CIRCLE_R}
            stroke={LINE_COLOR}
            strokeWidth={1}
          />
        );
      })}
      {/* Node circles */}
      {nodes.map((node, i) => (
        <circle
          key={node.node_id}
          cx={CIRCLE_R + i * SPACING}
          cy={CIRCLE_R}
          r={CIRCLE_R}
          fill={STATUS_COLORS[node.status] ?? STATUS_COLORS.pending}
        />
      ))}
    </svg>
  );
}
