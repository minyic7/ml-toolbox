import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import type { NodeCardData } from "../../lib/rfAdapters";
import type { NodeStatus } from "../../lib/types";
import { CATEGORY_ACCENT_COLORS } from "../../lib/portColors";
import PortDot from "./PortDot";

// ── Status config ──────────────────────────────────────────────────

const STATUS_LABELS: Record<NodeStatus, string> = {
  idle: "Idle",
  dirty: "Dirty",
  pending: "Pending",
  running: "Running…",
  done: "Done",
  error: "Error",
  skipped: "Skipped",
  cached: "Cached",
};

const STATUS_BORDERS: Record<
  NodeStatus,
  { width: string; style: string; color: string }
> = {
  idle: { width: "1px", style: "solid", color: "#D3D1C7" },
  dirty: { width: "1px", style: "dashed", color: "#D3D1C7" },
  pending: { width: "1px", style: "solid", color: "#B5D4F4" },
  running: { width: "1.5px", style: "solid", color: "#378ADD" },
  done: { width: "1.5px", style: "solid", color: "#639922" },
  error: { width: "1.5px", style: "solid", color: "#E24B4A" },
  skipped: { width: "1.5px", style: "solid", color: "#BA7517" },
  cached: { width: "1.5px", style: "solid", color: "#BA7517" },
};

const STATUS_DOT_COLORS: Record<NodeStatus, string> = {
  idle: "#D3D1C7",
  dirty: "#D3D1C7",
  pending: "#B5D4F4",
  running: "#378ADD",
  done: "#639922",
  error: "#E24B4A",
  skipped: "#BA7517",
  cached: "#BA7517",
};

// ── Tab bar types ──────────────────────────────────────────────────

type TabKey = "params" | "code" | "output";

const TABS: { key: TabKey; label: string }[] = [
  { key: "params", label: "Params" },
  { key: "code", label: "Code" },
  { key: "output", label: "Output" },
];

// ── NodeCard ───────────────────────────────────────────────────────

function NodeCard({ id, data, selected }: NodeProps & { data: NodeCardData }) {
  const { label, category, status, inputs, outputs, onTabClick } = data;
  const isError = status === "error";
  const isCached = status === "cached";
  const isRunning = status === "running";
  const isDone = status === "done";

  // Category-based accent bar color
  const accentColor =
    CATEGORY_ACCENT_COLORS[category.toLowerCase()] ?? "var(--border-default)";

  // Border: per-status width/style/color from spec, override color when selected
  const borderDef = STATUS_BORDERS[status];
  const borderColor = selected ? "var(--border-selected)" : borderDef.color;

  // Output badge: green dot if done/cached, red dot if error
  const outputBadgeColor = isError
    ? "var(--error-red)"
    : isDone || isCached
      ? "var(--success-green)"
      : null;

  return (
    <div
      className="node-card"
      style={{
        width: 232,
        background: "var(--node-bg)",
        borderRadius: 8,
        border: `${borderDef.width} ${borderDef.style} ${borderColor}`,
        boxShadow: selected
          ? "0 0 0 2px var(--border-selected)"
          : "0 1px 3px rgba(0,0,0,0.06)",
        position: "relative",
        overflow: "visible",
        fontSize: 13,
      }}
    >
      {/* Accent bar (3px top) — category-based */}
      <div
        style={{
          position: "absolute",
          top: 0,
          left: 8,
          right: 8,
          height: 3,
          borderRadius: "0 0 2px 2px",
          background: accentColor,
          transition: "background 0.2s",
        }}
      />

      {/* Progress bar — indeterminate shimmer when running */}
      {isRunning && (
        <div
          style={{
            position: "absolute",
            top: 3,
            left: 0,
            right: 0,
            height: 2,
            overflow: "hidden",
            borderRadius: "0 0 1px 1px",
          }}
        >
          <div
            style={{
              width: "100%",
              height: "100%",
              background: `linear-gradient(90deg, transparent 0%, var(--accent-blue) 50%, transparent 100%)`,
              animation: "shimmer 1.5s ease-in-out infinite",
            }}
          />
        </div>
      )}

      {/* Header */}
      <div
        style={{
          padding: "12px 12px 8px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        <span
          style={{
            fontWeight: 600,
            color: "var(--text-primary)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            flex: 1,
          }}
        >
          {label}
        </span>
        <span
          style={{
            fontSize: 11,
            color: "var(--text-secondary)",
            whiteSpace: "nowrap",
            display: "flex",
            alignItems: "center",
            gap: 4,
          }}
        >
          {STATUS_LABELS[status]}
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
              width: 6,
              height: 6,
              borderRadius: "50%",
              background: STATUS_DOT_COLORS[status],
              flexShrink: 0,
            }}
          />
        </span>
      </div>

      {/* Port labels */}
      <div
        style={{
          padding: "0 12px 10px",
          display: "flex",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        {/* Input labels */}
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {inputs.map((p) => (
            <span
              key={p.name}
              style={{ fontSize: 11, color: "var(--text-muted)" }}
            >
              {p.name}
            </span>
          ))}
        </div>
        {/* Output labels */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 2,
            textAlign: "right",
          }}
        >
          {outputs.map((p) => (
            <span
              key={p.name}
              style={{ fontSize: 11, color: "var(--text-muted)" }}
            >
              {p.name}
            </span>
          ))}
        </div>
      </div>

      {/* Error strip */}
      {isError && (
        <div
          style={{
            padding: "4px 12px",
            fontSize: 11,
            color: "var(--error-red)",
            background: "rgba(226,75,74,0.08)",
            borderTop: "1px solid rgba(226,75,74,0.15)",
          }}
        >
          Error — click to view
        </div>
      )}

      {/* Cached strip */}
      {isCached && (
        <div
          style={{
            padding: "4px 12px",
            fontSize: 11,
            color: "var(--success-green)",
            background: "rgba(99,153,34,0.08)",
            borderTop: "1px solid rgba(99,153,34,0.15)",
          }}
        >
          Cached
        </div>
      )}

      {/* Tab bar */}
      <div
        style={{
          display: "flex",
          borderTop: "1px solid var(--border-default)",
          borderRadius: "0 0 8px 8px",
          overflow: "hidden",
        }}
      >
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={(e) => {
              e.stopPropagation();
              onTabClick?.(id, tab.key);
            }}
            style={{
              flex: 1,
              padding: "5px 0",
              fontSize: 10,
              fontWeight: 500,
              color: "var(--text-secondary)",
              background: "transparent",
              border: "none",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              gap: 4,
            }}
          >
            {tab.label}
            {tab.key === "output" && outputBadgeColor && (
              <span
                style={{
                  display: "inline-block",
                  width: 6,
                  height: 6,
                  borderRadius: "50%",
                  background: outputBadgeColor,
                }}
              />
            )}
          </button>
        ))}
      </div>

      {/* Port dots */}
      {inputs.map((port, i) => (
        <PortDot
          key={`in-${port.name}`}
          port={port}
          side="input"
          index={i}
          total={inputs.length}
        />
      ))}
      {outputs.map((port, i) => (
        <PortDot
          key={`out-${port.name}`}
          port={port}
          side="output"
          index={i}
          total={outputs.length}
        />
      ))}
    </div>
  );
}

export default memo(NodeCard);
