import { useEffect, useRef, useState } from "react";
import type { NodeInstance, NodeDefinition, NodeStatus } from "../../lib/types";
import { X, Play, SlidersHorizontal, Code2, BarChart3, type LucideIcon } from "lucide-react";

type Tab = "params" | "code" | "output";

interface DrawerHeaderProps {
  node: NodeInstance;
  definition: NodeDefinition;
  status?: NodeStatus;
  activeTab: Tab;
  onTabChange: (tab: Tab) => void;
  onClose: () => void;
  onRunFrom: (nodeId: string) => void;
  onRename: (nodeId: string, name: string) => void;
  renameRequested?: boolean;
  onRenameHandled?: () => void;
}

const TABS: { key: Tab; label: string; icon: LucideIcon }[] = [
  { key: "params", label: "Params", icon: SlidersHorizontal },
  { key: "code", label: "Code", icon: Code2 },
  { key: "output", label: "Output", icon: BarChart3 },
];

const STATUS_COLORS: Record<string, string> = {
  idle: "var(--text-muted)",
  dirty: "var(--warning-amber)",
  pending: "var(--warning-amber)",
  running: "var(--accent-blue)",
  done: "var(--success-green)",
  error: "var(--error-red)",
  skipped: "var(--text-muted)",
  cached: "var(--success-green)",
};

const STATUS_ANIMATION: Record<string, string> = {
  running: "status-dot-pulse",
  pending: "status-dot-fade",
};

export function DrawerHeader({
  node,
  definition,
  status,
  activeTab,
  onTabChange,
  onClose,
  onRunFrom,
  onRename,
  renameRequested,
  onRenameHandled,
}: DrawerHeaderProps) {
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const displayName = node.name || definition.label || "";

  // Handle rename request from context menu
  useEffect(() => {
    if (renameRequested && node) {
      setEditValue(displayName);
      setIsEditing(true);
      onRenameHandled?.();
    }
  }, [renameRequested, node, displayName, onRenameHandled]);

  // Focus input when editing starts
  useEffect(() => {
    if (isEditing) {
      requestAnimationFrame(() => {
        inputRef.current?.focus();
        inputRef.current?.select();
      });
    }
  }, [isEditing]);

  const startEditing = () => {
    setEditValue(displayName);
    setIsEditing(true);
  };

  const saveEdit = () => {
    if (!node) return;
    setIsEditing(false);
    const trimmed = editValue.trim();
    if (!trimmed || trimmed === definition.label) {
      onRename(node.id, "");
    } else {
      onRename(node.id, trimmed);
    }
  };

  const cancelEdit = () => {
    setIsEditing(false);
    setEditValue(displayName);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") { e.preventDefault(); saveEdit(); }
    else if (e.key === "Escape") { e.preventDefault(); cancelEdit(); }
  };

  const statusColor = STATUS_COLORS[status ?? "idle"] ?? "var(--text-muted)";
  const statusAnim = STATUS_ANIMATION[status ?? ""] ?? "";

  return (
    <div
      className="flex items-center gap-3 border-b px-4"
      style={{
        borderColor: "var(--border-default)",
        backgroundColor: "var(--node-bg)",
        height: 40,
        minHeight: 40,
      }}
    >
      {/* Status dot */}
      <span
        className={statusAnim}
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          backgroundColor: statusColor,
          flexShrink: 0,
        }}
      />

      {/* Node name */}
      {isEditing ? (
        <input
          ref={inputRef}
          type="text"
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          onBlur={saveEdit}
          onKeyDown={handleKeyDown}
          maxLength={50}
          className="text-xs font-bold uppercase tracking-wider bg-transparent border rounded px-1 py-0.5 outline-none"
          style={{
            fontFamily: "var(--drawer-header-font)",
            fontWeight: 700,
            fontSize: 12,
            color: "var(--text-primary)",
            borderColor: "var(--border-selected)",
            minWidth: 80,
            maxWidth: 180,
          }}
        />
      ) : (
        <span
          className="cursor-pointer truncate"
          style={{
            fontFamily: "var(--drawer-header-font)",
            fontWeight: 700,
            fontSize: 12,
            textTransform: "uppercase",
            letterSpacing: "0.05em",
            color: "var(--text-primary)",
            maxWidth: 180,
          }}
          title={displayName}
          onClick={startEditing}
        >
          {displayName}
        </span>
      )}

      {/* Category type */}
      <span
        className="text-xs shrink-0"
        style={{ color: "var(--text-muted)" }}
      >
        {definition.category}
      </span>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Tab buttons */}
      <div className="flex items-center gap-0.5">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeTab === tab.key;
          return (
            <button
              key={tab.key}
              data-testid={`drawer-${tab.key}-tab`}
              onClick={() => onTabChange(tab.key)}
              className="flex items-center gap-1 px-2.5 py-1.5 transition-colors"
              style={{
                fontFamily: "var(--drawer-label-font)",
                fontWeight: 700,
                fontSize: 10,
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                color: isActive ? "var(--accent-blue)" : "var(--text-muted)",
                borderBottom: isActive ? "2px solid var(--accent-blue)" : "2px solid transparent",
              }}
            >
              <Icon className="h-3 w-3" />
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Run + Close buttons */}
      <div className="flex items-center gap-1 ml-2 shrink-0">
        <button
          onClick={() => onRunFrom(node.id)}
          aria-label="Run from this node"
          title="Run from this node"
          className="flex items-center justify-center transition-colors"
          style={{
            width: 24,
            height: 24,
            borderRadius: 4,
            border: "1px solid var(--border-default)",
            color: "var(--text-muted)",
            backgroundColor: "transparent",
            cursor: "pointer",
          }}
        >
          <Play className="h-3 w-3" />
        </button>
        <button
          onClick={onClose}
          aria-label="Close drawer"
          className="flex items-center justify-center transition-colors"
          style={{
            width: 24,
            height: 24,
            borderRadius: 4,
            border: "1px solid var(--border-default)",
            color: "var(--text-muted)",
            backgroundColor: "transparent",
            cursor: "pointer",
          }}
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );
}
