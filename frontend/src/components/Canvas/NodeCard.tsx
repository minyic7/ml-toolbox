import { memo, useState } from "react";
import type { NodeProps } from "@xyflow/react";
import type { NodeCardData } from "../../lib/rfAdapters";
import type { NodeStatus } from "../../lib/types";
import { CATEGORY_ACCENT_COLORS, PORT_COLORS } from "../../lib/portColors";
import type { PortType } from "../../lib/types";
import { useExecutionStore } from "../../store/executionStore";
import PortDot from "./PortDot";
import NodeActionBar from "./NodeActionBar";
import { AlertTriangle } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "../ui/dialog";

// ── Status config ──────────────────────────────────────────────────

const STATUS_BORDERS: Record<
  NodeStatus,
  { width: string; style: string; color: string; shadow?: string }
> = {
  idle: { width: "1px", style: "solid", color: "var(--border-default)" },
  dirty: { width: "1px", style: "dashed", color: "var(--border-default)" },
  pending: { width: "1.5px", style: "solid", color: "var(--status-pending)" },
  running: { width: "2px", style: "solid", color: "var(--accent-primary)", shadow: "0 0 8px rgba(59,130,246,0.35)" },
  done: { width: "2px", style: "solid", color: "var(--success-green)", shadow: "0 0 8px rgba(34,197,94,0.3)" },
  error: { width: "2px", style: "solid", color: "var(--error-red)", shadow: "0 0 8px rgba(239,68,68,0.35)" },
  skipped: { width: "1.5px", style: "solid", color: "var(--warning-amber)" },
  cached: { width: "1.5px", style: "solid", color: "var(--warning-amber)" },
};

const STATUS_DOT_COLORS: Record<NodeStatus, string> = {
  idle: "var(--status-idle)",
  dirty: "var(--status-idle)",
  pending: "var(--status-pending)",
  running: "var(--accent-primary)",
  done: "var(--success-green)",
  error: "var(--error-red)",
  skipped: "var(--warning-amber)",
  cached: "var(--success-green)",
};

const STATUS_LABELS: Partial<Record<NodeStatus, string>> = {
  pending: "queued",
  running: "running",
  done: "done",
  error: "error",
  cached: "cached",
};

// ── TypeBadge ──────────────────────────────────────────────────────

function TypeBadge({ type }: { type: PortType }) {
  const color = PORT_COLORS[type];
  return (
    <span
      title={`Port type: ${type}`}
      style={{
        fontSize: 8,
        fontFamily: "'Inter', sans-serif",
        fontWeight: 700,
        textTransform: "uppercase",
        color,
        background: `color-mix(in srgb, ${color} 15%, transparent)`,
        padding: "1px 4px",
        borderRadius: 6,
        lineHeight: "normal",
        flexShrink: 0,
      }}
    >
      {type}
    </span>
  );
}

// ── NodeCard ───────────────────────────────────────────────────────

function NodeCard({ id, data, selected }: NodeProps & { data: NodeCardData }) {
  const {
    label,
    type: nodeType,
    category,
    status,
    inputs,
    outputs,
    isKnownType,
    onTabClick,
    onRunFrom,
    onDeleteNode,
  } = data;
  const isDeprecated = !isKnownType;
  const isError = status === "error";
  const isCached = status === "cached";
  const isRunning = status === "running";
  const [hovered, setHovered] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const traceback = useExecutionStore((s) => s.nodeTracebacks[id]);
  const errorSummary = traceback
    ? traceback.split("\n").filter((l) => l.trim()).pop() ?? "Error"
    : "Error";

  // Category-based left accent border color
  const accentColor =
    CATEGORY_ACCENT_COLORS[category.toLowerCase()] ?? "var(--border-default)";

  // Right/top/bottom border: per-status
  const borderDef = STATUS_BORDERS[status];

  // Status label text (only shown for certain states)
  const statusLabel = STATUS_LABELS[status] ?? null;

  return (
    <div
      className="node-card"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        width: 210,
        background: "var(--node-bg)",
        borderRadius: "0 8px 8px 0",
        borderLeft: `4px solid ${accentColor}`,
        borderRight: isDeprecated ? "2px solid var(--warning-amber)" : `${borderDef.width} ${borderDef.style} ${borderDef.color}`,
        borderTop: isDeprecated ? "2px solid var(--warning-amber)" : `${borderDef.width} ${borderDef.style} ${borderDef.color}`,
        borderBottom: isDeprecated ? "2px solid var(--warning-amber)" : `${borderDef.width} ${borderDef.style} ${borderDef.color}`,
        boxShadow: borderDef.shadow
          ? borderDef.shadow
          : hovered
            ? "0 4px 16px rgba(0,0,0,0.08)"
            : "0 1px 3px rgba(0,0,0,0.06)",
        outline: selected ? "2px solid var(--border-selected)" : "none",
        outlineOffset: selected ? 2 : 0,
        position: "relative",
        overflow: "visible",
        fontSize: 13,
        transition: "box-shadow 150ms ease",
      }}
    >
      {/* Progress bar — indeterminate shimmer when running */}
      {isRunning && (
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            height: 2,
            overflow: "hidden",
            borderRadius: "0 8px 0 0",
          }}
        >
          <div
            style={{
              width: "100%",
              height: "100%",
              background: `linear-gradient(90deg, transparent 0%, var(--accent-primary) 50%, transparent 100%)`,
              animation: "shimmer 1.5s ease-in-out infinite",
            }}
          />
        </div>
      )}

      {/* Header */}
      <div
        style={{
          padding: "10px 10px 3px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 6,
        }}
      >
        <span
          style={{
            fontFamily: "'Inter', sans-serif",
            fontWeight: 700,
            fontSize: 10,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
            color: "var(--text-primary)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            flex: 1,
          }}
          title={label}
        >
          {label}
        </span>
        <span
          style={{
            display: "flex",
            alignItems: "center",
            gap: 4,
            flexShrink: 0,
          }}
        >
          {statusLabel && (
            <span
              style={{
                fontFamily: "'Inter', sans-serif",
                fontWeight: 600,
                fontSize: 9,
                color: STATUS_DOT_COLORS[status],
              }}
            >
              {statusLabel}
            </span>
          )}
          <span
            className={
              isRunning
                ? "status-dot-pulse"
                : status === "pending"
                  ? "status-dot-fade"
                  : undefined
            }
            style={{
              display: "inline-block",
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: STATUS_DOT_COLORS[status],
              flexShrink: 0,
              boxShadow: isCached
                ? `0 0 0 2px var(--node-bg), 0 0 0 3px var(--success-green)`
                : status === "done"
                  ? `0 0 4px ${STATUS_DOT_COLORS[status]}`
                  : undefined,
            }}
          />
        </span>
      </div>

      {/* Node type subline */}
      <div
        style={{
          padding: "0 10px 6px",
          fontFamily: "'Inter', sans-serif",
          fontSize: 9,
          fontWeight: 500,
          color: "var(--text-muted)",
        }}
      >
        {category} &middot; {nodeType.includes("/") ? nodeType.split("/").pop() : nodeType}
      </div>

      {/* Port rows — each row aligns a port label with its PortDot */}
      <div
        style={{
          paddingTop: 2,
          paddingBottom: 8,
          display: "flex",
          flexDirection: "column",
          gap: 4,
        }}
      >
        {Array.from({ length: Math.max(inputs.length, outputs.length) }).map(
          (_, i) => {
            const inp = inputs[i];
            const out = outputs[i];
            return (
              <div
                key={i}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  minHeight: 20,
                  position: "relative",
                }}
              >
                {/* Input label */}
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 4,
                    paddingLeft: 10,
                  }}
                >
                  {inp && (
                    <>
                      <span
                        title={inp.name}
                        style={{
                          fontFamily: "'JetBrains Mono', monospace",
                          fontSize: 9,
                          color: "var(--text-muted)",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                          maxWidth: "7ch",
                        }}
                      >
                        {inp.name}
                      </span>
                      <TypeBadge type={inp.type} />
                    </>
                  )}
                </div>

                {/* Output label */}
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 4,
                    paddingRight: 10,
                  }}
                >
                  {out && (
                    <>
                      <TypeBadge type={out.type} />
                      <span
                        title={out.name}
                        style={{
                          fontFamily: "'JetBrains Mono', monospace",
                          fontSize: 9,
                          color: "var(--text-muted)",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                          maxWidth: "7ch",
                        }}
                      >
                        {out.name}
                      </span>
                    </>
                  )}
                </div>

                {/* Port dots — inside the row so they align vertically */}
                {inp && <PortDot port={inp} side="input" />}
                {out && <PortDot port={out} side="output" />}
              </div>
            );
          },
        )}
      </div>

      {/* Error strip */}
      {isError && (
        <div
          className="nodrag nopan"
          style={{
            padding: "4px 10px",
            fontSize: 10,
            fontFamily: "monospace",
            color: "var(--error-red)",
            background: "var(--error-red-bg)",
            borderTop: "1px solid var(--error-red-border)",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 4,
            cursor: "pointer",
          }}
          onClick={(e) => {
            e.stopPropagation();
            onTabClick?.(id, "output");
          }}
        >
          <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {errorSummary}
          </span>
          <span
            style={{ flexShrink: 0, cursor: "pointer", textDecoration: "underline" }}
            onClick={(e) => {
              e.stopPropagation();
              onTabClick?.(id, "code");
            }}
          >
            Open Code
          </span>
        </div>
      )}

      {/* Cached strip */}
      {isCached && (
        <div
          style={{
            padding: "4px 10px",
            fontSize: 10,
            fontFamily: "'Inter', sans-serif",
            fontWeight: 500,
            color: "var(--success-green)",
            background: "var(--success-green-bg)",
            borderTop: "1px solid var(--success-green-border)",
          }}
        >
          using cached output
        </div>
      )}

      {/* Deprecated node warning strip */}
      {isDeprecated && (
        <div
          title="This node type is no longer available. Delete it and replace with an updated node."
          style={{
            padding: "4px 10px",
            fontSize: 10,
            fontFamily: "'Inter', sans-serif",
            fontWeight: 500,
            color: "var(--warning-amber)",
            background: "color-mix(in srgb, var(--warning-amber) 10%, transparent)",
            borderTop: "1px solid color-mix(in srgb, var(--warning-amber) 30%, transparent)",
            display: "flex",
            alignItems: "center",
            gap: 4,
          }}
        >
          <AlertTriangle size={12} style={{ flexShrink: 0 }} />
          <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            Unknown node type
          </span>
        </div>
      )}

      {/* Delete confirmation dialog */}
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent className="max-w-xs" onClick={(e) => e.stopPropagation()}>
          <DialogHeader>
            <DialogTitle>Delete node</DialogTitle>
            <DialogDescription>
              Delete &ldquo;{label}&rdquo;? Connected edges will also be removed.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <button
              style={{
                padding: "6px 14px",
                fontSize: 13,
                fontWeight: 600,
                borderRadius: 6,
                border: "1px solid var(--border-default)",
                background: "transparent",
                cursor: "pointer",
                color: "var(--text-primary)",
              }}
              onClick={(e) => {
                e.stopPropagation();
                setConfirmOpen(false);
              }}
            >
              Cancel
            </button>
            <button
              style={{
                padding: "6px 14px",
                fontSize: 13,
                fontWeight: 600,
                borderRadius: 6,
                border: "none",
                background: "var(--error-red)",
                color: "#fff",
                cursor: "pointer",
              }}
              onClick={(e) => {
                e.stopPropagation();
                setConfirmOpen(false);
                onDeleteNode?.(id);
              }}
            >
              Delete
            </button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Action bar — visible on hover or selected */}
      <NodeActionBar
        visible={hovered || !!selected}
        nodeId={id}
        onRun={() => onRunFrom?.(id)}
        onDelete={() => setConfirmOpen(true)}
      />

    </div>
  );
}

export default memo(NodeCard);
