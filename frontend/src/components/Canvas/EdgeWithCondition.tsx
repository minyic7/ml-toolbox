import { useState, useCallback } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from "@xyflow/react";

interface EdgeData extends Record<string, unknown> {
  condition?: string;
  onDeleteEdge?: (edgeId: string) => void;
}

export default function EdgeWithCondition({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style,
  markerEnd,
  data,
  selected,
}: EdgeProps & { data?: EdgeData }) {
  const [hovered, setHovered] = useState(false);

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const condition = data?.condition;
  const onDeleteEdge = data?.onDeleteEdge;

  const handleMouseEnter = useCallback(() => setHovered(true), []);
  const handleMouseLeave = useCallback(() => setHovered(false), []);
  const handleDelete = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      onDeleteEdge?.(id);
    },
    [id, onDeleteEdge],
  );

  const isHighlighted = selected || hovered;

  return (
    <g onMouseEnter={handleMouseEnter} onMouseLeave={handleMouseLeave}>
      {/* Invisible wider path for easier hover targeting */}
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={20}
      />
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          stroke: isHighlighted
            ? "var(--border-selected)"
            : "var(--border-default)",
          strokeWidth: isHighlighted ? 3 : 1.5,
          transition: "stroke-width 0.15s, stroke 0.15s",
          ...style,
        }}
      />
      {/* Delete button on hover */}
      {hovered && onDeleteEdge && (
        <EdgeLabelRenderer>
          <button
            type="button"
            onClick={handleDelete}
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: "all",
              width: 16,
              height: 16,
              borderRadius: "50%",
              border: "1px solid var(--border-default)",
              background: "var(--node-bg)",
              color: "var(--text-muted)",
              fontSize: 10,
              lineHeight: "14px",
              textAlign: "center",
              padding: 0,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              zIndex: 10,
            }}
            className="nodrag nopan"
            onMouseEnter={(e) => {
              e.currentTarget.style.color = "var(--error-red)";
              e.currentTarget.style.borderColor = "var(--error-red)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.color = "var(--text-muted)";
              e.currentTarget.style.borderColor = "var(--border-default)";
            }}
          >
            ×
          </button>
        </EdgeLabelRenderer>
      )}
      {/* Condition label */}
      {condition && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px,${condition ? labelY - 16 : labelY}px)`,
              pointerEvents: "all",
              fontSize: 11,
              padding: "2px 6px",
              borderRadius: 4,
              background: "var(--node-bg)",
              border: "1px solid var(--border-default)",
              color: "var(--text-secondary)",
            }}
            className="nodrag nopan"
          >
            {condition}
          </div>
        </EdgeLabelRenderer>
      )}
    </g>
  );
}
