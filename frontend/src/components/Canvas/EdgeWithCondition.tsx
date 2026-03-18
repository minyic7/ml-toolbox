import { useState, useCallback, useRef, useEffect } from "react";
import {
  getBezierPath,
  BaseEdge,
  type EdgeProps,
  type Edge,
} from "@xyflow/react";

export interface EdgeConditionData {
  condition?: string;
  onConditionChange?: (edgeId: string, condition: string) => void;
  [key: string]: unknown;
}

type EdgeWithConditionProps = EdgeProps<Edge<EdgeConditionData>>;

function abbreviate(text: string, max = 20): string {
  if (text.length <= max) return text;
  return text.slice(0, max) + "…";
}

export function EdgeWithCondition({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  selected,
  data,
  markerEnd,
}: EdgeWithConditionProps) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });

  const condition = data?.condition ?? "";
  const onConditionChange = data?.onConditionChange;

  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(condition);
  const inputRef = useRef<HTMLInputElement>(null);

  // Sync draft when external condition changes
  useEffect(() => {
    if (!editing) {
      setDraft(condition);
    }
  }, [condition, editing]);

  // Focus input when entering edit mode
  useEffect(() => {
    if (editing) {
      inputRef.current?.focus();
      inputRef.current?.select();
    }
  }, [editing]);

  const handleLabelClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      setEditing(true);
    },
    [],
  );

  const commitEdit = useCallback(() => {
    setEditing(false);
    const trimmed = draft.trim();
    if (trimmed !== condition) {
      onConditionChange?.(id, trimmed);
    }
  }, [draft, condition, onConditionChange, id]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter") {
        commitEdit();
      } else if (e.key === "Escape") {
        setDraft(condition);
        setEditing(false);
      }
    },
    [commitEdit, condition],
  );

  const edgeColor = selected ? "#3B82F6" : "#6b7280";

  return (
    <>
      <BaseEdge
        path={edgePath}
        markerEnd={markerEnd}
        style={{ stroke: edgeColor, strokeWidth: 2 }}
      />

      <foreignObject
        x={labelX - 100}
        y={labelY - 14}
        width={200}
        height={28}
        requiredExtensions="http://www.w3.org/1999/xhtml"
        style={{ overflow: "visible", pointerEvents: "none" }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            width: "100%",
            height: "100%",
            pointerEvents: "all",
          }}
        >
          {editing ? (
            <input
              ref={inputRef}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onBlur={commitEdit}
              onKeyDown={handleKeyDown}
              placeholder='outputs["accuracy"] > 0.8'
              style={{
                fontFamily: "monospace",
                fontSize: "12px",
                padding: "2px 6px",
                background: "rgba(30, 30, 34, 0.9)",
                color: "#e4e4e7",
                border: "1px solid #3B82F6",
                borderRadius: "4px",
                outline: "none",
                width: "200px",
                textAlign: "center",
              }}
            />
          ) : condition ? (
            <span
              onClick={handleLabelClick}
              style={{
                fontFamily: "monospace",
                fontSize: "11px",
                color: "#9ca3af",
                background: "rgba(30, 30, 34, 0.8)",
                padding: "1px 6px",
                borderRadius: "3px",
                cursor: "pointer",
                whiteSpace: "nowrap",
                userSelect: "none",
              }}
            >
              {abbreviate(condition)}
            </span>
          ) : selected ? (
            <span
              onClick={handleLabelClick}
              style={{
                fontFamily: "monospace",
                fontSize: "11px",
                color: "#6b7280",
                background: "rgba(30, 30, 34, 0.8)",
                padding: "1px 6px",
                borderRadius: "3px",
                cursor: "pointer",
                whiteSpace: "nowrap",
                userSelect: "none",
              }}
            >
              + condition
            </span>
          ) : null}
        </div>
      </foreignObject>
    </>
  );
}
