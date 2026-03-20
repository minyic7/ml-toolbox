import { useCallback, useRef, useState } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from "@xyflow/react";

interface EdgeData extends Record<string, unknown> {
  condition?: string;
  onDeleteEdge?: (edgeId: string) => void;
  onPatchEdge?: (edgeId: string, condition: string) => void;
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
  const onPatchEdge = data?.onPatchEdge;

  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const committedRef = useRef(false);

  const handleMouseEnter = useCallback(() => setHovered(true), []);
  const handleMouseLeave = useCallback(() => setHovered(false), []);
  const handleDelete = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      onDeleteEdge?.(id);
    },
    [id, onDeleteEdge],
  );

  const startEditing = useCallback(() => {
    setDraft(condition ?? "");
    committedRef.current = false;
    setEditing(true);
    setTimeout(() => inputRef.current?.focus(), 0);
  }, [condition]);

  const commitEdit = useCallback(() => {
    if (committedRef.current) return;
    committedRef.current = true;
    setEditing(false);
    const trimmed = draft.trim();
    if (trimmed !== (condition ?? "")) {
      onPatchEdge?.(id, trimmed);
    }
  }, [draft, condition, onPatchEdge, id]);

  const cancelEdit = useCallback(() => {
    committedRef.current = true;
    setEditing(false);
    setDraft(condition ?? "");
  }, [condition]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter") {
        e.preventDefault();
        commitEdit();
      } else if (e.key === "Escape") {
        e.preventDefault();
        cancelEdit();
      }
    },
    [commitEdit, cancelEdit],
  );

  const isHighlighted = selected || hovered;

  const labelStyle: React.CSSProperties = {
    position: "absolute",
    transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
    pointerEvents: "all",
    fontSize: 11,
    padding: "2px 6px",
    borderRadius: 4,
    background: "var(--node-bg)",
    border: "1px solid var(--border-default)",
    color: "var(--text-secondary)",
  };

  // Offset label above delete button when both are present
  const conditionLabelStyle: React.CSSProperties = {
    ...labelStyle,
    transform: `translate(-50%, -50%) translate(${labelX}px,${labelY - 16}px)`,
  };

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
            : "var(--dot-grid)",
          strokeWidth: isHighlighted ? 2.5 : 1.5,
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
      {/* "+ Condition" button on hover when no condition exists */}
      {hovered && !condition && !editing && onPatchEdge && (
        <EdgeLabelRenderer>
          <button
            type="button"
            onClick={startEditing}
            style={{
              ...labelStyle,
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY - 16}px)`,
              cursor: "pointer",
              color: "var(--text-muted)",
              fontSize: 11,
              fontFamily: "inherit",
              lineHeight: "normal",
            }}
            className="nodrag nopan"
          >
            + Condition
          </button>
        </EdgeLabelRenderer>
      )}
      {/* Condition label — click to edit */}
      {(condition || editing) && (
        <EdgeLabelRenderer>
          {editing ? (
            <div style={conditionLabelStyle} className="nodrag nopan">
              <input
                ref={inputRef}
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onBlur={commitEdit}
                onKeyDown={handleKeyDown}
                style={{
                  border: "none",
                  outline: "none",
                  background: "transparent",
                  fontSize: 11,
                  color: "var(--text-primary)",
                  width: Math.max(40, draft.length * 7),
                  padding: 0,
                  margin: 0,
                  fontFamily: "inherit",
                }}
              />
            </div>
          ) : (
            <div
              style={{ ...conditionLabelStyle, cursor: "pointer" }}
              className="nodrag nopan"
              onClick={startEditing}
            >
              {condition}
            </div>
          )}
        </EdgeLabelRenderer>
      )}
    </g>
  );
}
