import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import type { NodeCardData } from "../../lib/rfAdapters";
import type { NodeStatus } from "../../lib/types";
import PortDot from "./PortDot";

// ── Status config ──────────────────────────────────────────────────

const STATUS_STYLES: Record<NodeStatus, { accent: string; label: string }> = {
  idle: { accent: "var(--border-default)", label: "Idle" },
  dirty: { accent: "var(--warning-amber)", label: "Dirty" },
  pending: { accent: "var(--text-muted)", label: "Pending" },
  running: { accent: "var(--accent-blue)", label: "Running…" },
  done: { accent: "var(--success-green)", label: "Done" },
  error: { accent: "var(--error-red)", label: "Error" },
  skipped: { accent: "var(--text-secondary)", label: "Skipped" },
  cached: { accent: "var(--success-green)", label: "Cached" },
};

// ── NodeCard ───────────────────────────────────────────────────────

function NodeCard({ data, selected }: NodeProps & { data: NodeCardData }) {
  const { label, status, inputs, outputs } = data;
  const statusStyle = STATUS_STYLES[status];
  const isError = status === "error";
  const isCached = status === "cached";
  const isRunning = status === "running";

  return (
    <div
      className="node-card"
      style={{
        width: 232,
        background: "var(--node-bg)",
        borderRadius: 8,
        border: `1px solid ${selected ? "var(--border-selected)" : "var(--border-default)"}`,
        boxShadow: selected
          ? "0 0 0 2px var(--border-selected)"
          : "0 1px 3px rgba(0,0,0,0.06)",
        position: "relative",
        overflow: "visible",
        fontSize: 13,
      }}
    >
      {/* Accent bar (3px top) */}
      <div
        style={{
          position: "absolute",
          top: 0,
          left: 8,
          right: 8,
          height: 3,
          borderRadius: "0 0 2px 2px",
          background: statusStyle.accent,
          transition: "background 0.2s",
        }}
      />

      {/* Running pulse animation */}
      {isRunning && (
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 8,
            right: 8,
            height: 3,
            borderRadius: "0 0 2px 2px",
            background: statusStyle.accent,
            animation: "pulse 1.5s ease-in-out infinite",
          }}
        />
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
          }}
        >
          {statusStyle.label}
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
            borderRadius: "0 0 8px 8px",
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
            borderRadius: "0 0 8px 8px",
          }}
        >
          Cached
        </div>
      )}

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
