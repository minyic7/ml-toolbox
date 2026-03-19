import { useEffect, useRef, useState } from "react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import { cn } from "../../lib/utils";
import { Button } from "@/components/ui/button";
import { Play, X } from "lucide-react";
import { ParamsTab } from "./ParamsTab";
import { CodeTab } from "./CodeTab";
import { OutputTab } from "./OutputTab";

type Tab = "params" | "code" | "output";

interface RightPanelProps {
  pipelineId: string;
  node: NodeInstance | null;
  definition: NodeDefinition | null;
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  paramSaving?: boolean;
  onCodeChange: (nodeId: string, code: string) => void;
  onCodeSave: (nodeId: string, code: string) => void;
  codeSaveOk?: boolean;
  onClose: () => void;
  requestedTab?: string | null;
  onRequestedTabHandled?: () => void;
  onRename: (nodeId: string, name: string) => void;
  onRunFrom: (nodeId: string) => void;
  renameRequested?: boolean;
  onRenameHandled?: () => void;
}

const TABS: { key: Tab; label: string }[] = [
  { key: "params", label: "Params" },
  { key: "code", label: "Code" },
  { key: "output", label: "Output" },
];

export function RightPanel({
  pipelineId,
  node,
  definition,
  onParamChange,
  paramSaving,
  onCodeChange,
  onCodeSave,
  codeSaveOk,
  onClose,
  requestedTab,
  onRequestedTabHandled,
  onRename,
  onRunFrom,
  renameRequested,
  onRenameHandled,
}: RightPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>("params");
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (requestedTab && (requestedTab === "params" || requestedTab === "code" || requestedTab === "output")) {
      setActiveTab(requestedTab);
      onRequestedTabHandled?.();
    }
  }, [requestedTab, onRequestedTabHandled]);

  const isOpen = node !== null;
  const displayName = node?.name || definition?.label || "";

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
      // Use rAF to ensure DOM is updated before focusing
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
    // If empty or same as definition label, clear custom name
    if (!trimmed || trimmed === definition?.label) {
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
    if (e.key === "Enter") {
      e.preventDefault();
      saveEdit();
    } else if (e.key === "Escape") {
      e.preventDefault();
      cancelEdit();
    }
  };

  return (
    <div
      className={cn(
        "flex flex-col border-l border-border bg-background overflow-hidden transition-all",
        isOpen ? "w-[360px] min-w-[360px]" : "w-0 min-w-0",
      )}
      style={{ transitionDuration: "250ms" }}
    >
      {node && definition && (
        <>
          {/* Header */}
          <div
            className="flex items-center justify-between border-b border-border px-4 py-3"
          >
            <div className="flex flex-col gap-0.5 min-w-0 flex-1">
              {isEditing ? (
                <input
                  ref={inputRef}
                  type="text"
                  value={editValue}
                  onChange={(e) => setEditValue(e.target.value)}
                  onBlur={saveEdit}
                  onKeyDown={handleKeyDown}
                  maxLength={50}
                  className="text-sm font-semibold bg-transparent border border-[var(--border-selected)] rounded px-1 py-0.5 outline-none w-full"
                  style={{ color: "var(--text-primary)" }}
                />
              ) : (
                <span
                  className="text-sm font-semibold cursor-pointer truncate block"
                  style={{
                    color: "var(--text-primary)",
                    maxWidth: "22ch",
                  }}
                  title={displayName}
                  onClick={startEditing}
                >
                  {displayName}
                </span>
              )}
              <span
                className="text-xs"
                style={{ color: "var(--text-muted)" }}
              >
                {definition.category}
              </span>
            </div>
            <div className="flex items-center gap-1 ml-2 shrink-0">
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-[var(--text-muted)] hover:text-[var(--accent-blue)]"
                onClick={() => onRunFrom(node.id)}
                aria-label="Run from this node"
                title="Run from this node"
              >
                <Play className="h-3.5 w-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-[var(--text-muted)]"
                onClick={onClose}
                aria-label="Close panel"
              >
                <X className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>

          {/* Tabs */}
          <div className="flex border-b border-border">
            {TABS.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className="flex-1 py-2 text-xs font-medium transition-colors"
                style={{
                  color:
                    activeTab === tab.key
                      ? "var(--accent-blue)"
                      : "var(--text-muted)",
                  borderBottom:
                    activeTab === tab.key
                      ? "2px solid var(--accent-blue)"
                      : "2px solid transparent",
                }}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="min-h-0 flex-1 overflow-y-auto">
            {activeTab === "params" && (
              <ParamsTab
                params={definition.params}
                values={buildParamValues(node)}
                onChange={(name, value) => onParamChange(node.id, name, value)}
                disabled={paramSaving}
              />
            )}
            {activeTab === "code" && (
              <CodeTab
                code={node.code}
                defaultCode={definition.default_code}
                onChange={(code) => onCodeChange(node.id, code)}
                lastSaveOk={codeSaveOk}
                onSave={(code) => onCodeSave(node.id, code)}
              />
            )}
            {activeTab === "output" && (
              <OutputTab pipelineId={pipelineId} nodeId={node.id} />
            )}
          </div>
        </>
      )}
    </div>
  );
}

function buildParamValues(node: NodeInstance): Record<string, unknown> {
  const values: Record<string, unknown> = {};
  for (const p of node.params) {
    values[p.name] = p.default;
  }
  return values;
}
