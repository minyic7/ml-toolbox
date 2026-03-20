import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import type { NodeCardData } from "../../lib/rfAdapters";
import type { NodeStatus } from "../../lib/types";
import { CATEGORY_ACCENT_COLORS, PORT_COLORS } from "../../lib/portColors";
import type { PortType } from "../../lib/types";
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
  idle: { width: "1px", style: "solid", color: "var(--border-default)" },
  dirty: { width: "1px", style: "dashed", color: "var(--border-default)" },
  pending: { width: "1px", style: "solid", color: "#B5D4F4" },
  running: { width: "1.5px", style: "solid", color: "var(--accent-primary)" },
  done: { width: "1.5px", style: "solid", color: "var(--success-green)" },
  error: { width: "1.5px", style: "solid", color: "var(--error-red)" },
  skipped: { width: "1.5px", style: "solid", color: "var(--warning-amber)" },
  cached: { width: "1.5px", style: "solid", color: "var(--warning-amber)" },
};

const STATUS_DOT_COLORS: Record<NodeStatus, string> = {
  idle: "var(--border-default)",
  dirty: "var(--border-default)",
  pending: "#B5D4F4",
  running: "var(--accent-primary)",
  done: "var(--success-green)",
  error: "var(--error-red)",
  skipped: "var(--warning-amber)",
  cached: "var(--success-green)",
};

// ── Tab bar types ──────────────────────────────────────────────────

type TabKey = "params" | "code" | "output";

const TABS: { key: TabKey; label: string }[] = [
  { key: "params", label: "Params" },
  { key: "code", label: "Code" },
  { key: "output", label: "Output" },
];

// ── TypeBadge ──────────────────────────────────────────────────────

function TypeBadge({ type }: { type: PortType }) {
  const color = PORT_COLORS[type];
  return (
    <span
      title={`Port type: ${type}`}
      style={{
        fontSize: 9,
        fontWeight: 500,
        textTransform: "uppercase",
        color,
        background: `${color}26`,
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
  const { label, type: nodeType, category, status, inputs, outputs, onTabClick } = data;
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
              background: `linear-gradient(90deg, transparent 0%, var(--accent-primary) 50%, transparent 100%)`,
              animation: "shimmer 1.5s ease-in-out infinite",
            }}
          />
        </div>
      )}

      {/* Header */}
      <div
        style={{
          padding: "12px 12px 4px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        <span
          style={{
            fontFamily: "'Inter', sans-serif",
            fontWeight: 700,
            textTransform: "uppercase" as const,
            fontSize: 11,
            letterSpacing: "0.04em",
            color: "var(--text-primary)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            flex: 1,
            maxWidth: "22ch",
          }}
          title={label}
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
              boxShadow: isCached ? `0 0 0 2px var(--node-bg), 0 0 0 3px var(--success-green)` : undefined,
            }}
          />
        </span>
      </div>

      {/* Node type subline */}
      <div
        style={{
          padding: "0 12px 6px",
          fontSize: 10,
          color: "var(--text-muted)",
        }}
      >
        {category} &middot; {nodeType.includes("/") ? nodeType.split("/").pop() : nodeType}
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
        {/* Input labels: [Name] [TypeBadge] */}
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {inputs.map((p) => (
            <div
              key={p.name}
              style={{ display: "flex", alignItems: "center", gap: 4 }}
            >
              <span
                title={p.name}
                style={{
                  fontSize: 11,
                  color: "var(--text-muted)",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  maxWidth: "8ch",
                }}
              >
                {p.name}
              </span>
              <TypeBadge type={p.type} />
            </div>
          ))}
        </div>
        {/* Output labels: [TypeBadge] [Name] */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 3,
            alignItems: "flex-end",
          }}
        >
          {outputs.map((p) => (
            <div
              key={p.name}
              style={{ display: "flex", alignItems: "center", gap: 4 }}
            >
              <TypeBadge type={p.type} />
              <span
                title={p.name}
                style={{
                  fontSize: 11,
                  color: "var(--text-muted)",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  maxWidth: "8ch",
                }}
              >
                {p.name}
              </span>
            </div>
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
            background: "rgba(158,63,78,0.08)",
            borderTop: "1px solid rgba(158,63,78,0.15)",
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
            Error
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
            padding: "4px 12px",
            fontSize: 11,
            color: "var(--success-green)",
            background: "rgba(16,185,129,0.08)",
            borderTop: "1px solid rgba(16,185,129,0.15)",
          }}
        >
          ↩ using cached output
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
            className="node-card-tab"
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
              borderBottom: "2px solid transparent",
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
