import type { NodeInstance, NodeDefinition, NodeStatus } from "../../lib/types";
import { useExecutionStore } from "../../store/executionStore";
import { X } from "lucide-react";

type DrawerTab = "params" | "output";

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

const TABS: { key: DrawerTab; label: string; icon: string }[] = [
  { key: "params", label: "Params", icon: "▤" },
  { key: "output", label: "Output", icon: "▦" },
];

interface DrawerHeaderProps {
  node: NodeInstance;
  definition: NodeDefinition;
  activeTab: DrawerTab;
  onTabChange: (tab: DrawerTab) => void;
  onClose: () => void;
}

export default function DrawerHeader({
  node,
  definition,
  activeTab,
  onTabChange,
  onClose,
}: DrawerHeaderProps) {
  const status = useExecutionStore(
    (s) => s.nodeStatuses[node.id] ?? "idle",
  );
  const dotColor = STATUS_DOT_COLORS[status];
  const displayName = node.name || definition.label || node.type;
  const nodeType = `${definition.category} · ${node.type}`;

  return (
    <div
      className="flex items-center gap-3 px-4 shrink-0"
      style={{
        height: 38,
        borderBottom: "1px solid var(--border-default)",
        background: "var(--node-bg)",
      }}
    >
      {/* Status dot */}
      <span
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: dotColor,
          flexShrink: 0,
        }}
      />

      {/* Node name — Manrope 700 12px uppercase */}
      <span
        style={{
          fontFamily: "'Manrope', sans-serif",
          fontWeight: 700,
          fontSize: 12,
          textTransform: "uppercase",
          color: "var(--text-primary)",
          letterSpacing: "0.04em",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          maxWidth: 180,
        }}
      >
        {displayName}
      </span>

      {/* Node type — Inter 500 10px */}
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 500,
          fontSize: 10,
          color: "var(--text-muted)",
          whiteSpace: "nowrap",
        }}
      >
        {nodeType}
      </span>

      {/* Spacer */}
      <div style={{ flex: 1 }} />

      {/* Tab buttons — Inter 700 10px uppercase */}
      <div className="flex items-center gap-1">
        {TABS.map((tab) => {
          const isActive = activeTab === tab.key;
          return (
            <button
              key={tab.key}
              onClick={() => onTabChange(tab.key)}
              style={{
                fontFamily: "'Inter', sans-serif",
                fontWeight: 700,
                fontSize: 10,
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                color: isActive ? "var(--accent-primary)" : "var(--text-muted)",
                borderBottom: isActive
                  ? "2px solid var(--accent-primary)"
                  : "2px solid transparent",
                background: "none",
                border: "none",
                borderBottomStyle: "solid",
                borderBottomWidth: 2,
                borderBottomColor: isActive
                  ? "var(--accent-primary)"
                  : "transparent",
                padding: "4px 8px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 4,
              }}
            >
              <span style={{ fontSize: 11 }}>{tab.icon}</span>
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Close button — 24x24px */}
      <button
        onClick={onClose}
        aria-label="Close drawer"
        style={{
          width: 24,
          height: 24,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          border: "1px solid var(--border-default)",
          borderRadius: 4,
          background: "transparent",
          cursor: "pointer",
          flexShrink: 0,
          color: "var(--text-muted)",
        }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLElement).style.background = "var(--ghost-hover-bg)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLElement).style.background = "transparent";
        }}
      >
        <X size={14} />
      </button>
    </div>
  );
}
