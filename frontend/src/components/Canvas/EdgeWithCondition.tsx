import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from "@xyflow/react";

interface EdgeData extends Record<string, unknown> {
  condition?: string;
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
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const condition = data?.condition;

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          stroke: selected ? "var(--border-selected)" : "var(--border-default)",
          strokeWidth: selected ? 2 : 1.5,
          ...style,
        }}
      />
      {condition && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
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
    </>
  );
}
