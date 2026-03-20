import type { NodeInstance, NodeDefinition, NodeStatus } from "../../lib/types";
import { DrawerHeader } from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";
import { OutputTab } from "./OutputTab";
import { useEffect, useState } from "react";

type Tab = "params" | "code" | "output";

interface BottomDrawerProps {
  pipelineId: string;
  node: NodeInstance | null;
  definition: NodeDefinition | null;
  status?: NodeStatus;
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  paramSaving?: boolean;
  onClose: () => void;
  onRename: (nodeId: string, name: string) => void;
  onRunFrom: (nodeId: string) => void;
  onCodePaneOpen: () => void;
  requestedTab?: string | null;
  onRequestedTabHandled?: () => void;
  renameRequested?: boolean;
  onRenameHandled?: () => void;
  requestedRunId?: string | null;
  onRequestedRunHandled?: () => void;
  codePaneOpen?: boolean;
}

export function BottomDrawer({
  pipelineId,
  node,
  definition,
  status,
  onParamChange,
  paramSaving,
  onClose,
  onRename,
  onRunFrom,
  onCodePaneOpen,
  requestedTab,
  onRequestedTabHandled,
  renameRequested,
  onRenameHandled,
  requestedRunId,
  onRequestedRunHandled,
  codePaneOpen,
}: BottomDrawerProps) {
  const [activeTab, setActiveTab] = useState<Tab>("params");
  const isOpen = node !== null;

  // Handle requested tab changes
  useEffect(() => {
    if (
      requestedTab &&
      (requestedTab === "params" ||
        requestedTab === "code" ||
        requestedTab === "output")
    ) {
      if (requestedTab === "code") {
        onCodePaneOpen();
        setActiveTab("code");
      } else {
        setActiveTab(requestedTab);
      }
      onRequestedTabHandled?.();
    }
  }, [requestedTab, onRequestedTabHandled, onCodePaneOpen]);

  const handleTabChange = (tab: Tab) => {
    if (tab === "code") {
      onCodePaneOpen();
      setActiveTab("code");
    } else {
      setActiveTab(tab);
    }
  };

  // When code pane is open, show a context strip instead of full content
  const showContextStrip = codePaneOpen && activeTab === "code";

  return (
    <div
      className="border-t overflow-hidden transition-all"
      style={{
        borderColor: "var(--border-default)",
        backgroundColor: "var(--node-bg)",
        height: isOpen ? "var(--drawer-height)" : 0,
        minHeight: isOpen ? "var(--drawer-height)" : 0,
        transitionDuration: "250ms",
        transitionTimingFunction: "ease",
      }}
    >
      {node && definition && (
        <div className="flex flex-col h-full">
          <DrawerHeader
            node={node}
            definition={definition}
            status={status}
            activeTab={activeTab}
            onTabChange={handleTabChange}
            onClose={onClose}
            onRunFrom={onRunFrom}
            onRename={onRename}
            renameRequested={renameRequested}
            onRenameHandled={onRenameHandled}
          />

          {/* Tab content */}
          <div className="min-h-0 flex-1 overflow-y-auto">
            {showContextStrip ? (
              <div
                className="flex items-center justify-center h-full text-xs"
                style={{ color: "var(--text-muted)" }}
              >
                Editing code for {node.name || definition.label}
              </div>
            ) : activeTab === "params" ? (
              <ParamsTab
                params={definition.params}
                values={buildParamValues(node)}
                onChange={(name, value) => onParamChange(node.id, name, value)}
                disabled={paramSaving}
                onCodePaneOpen={onCodePaneOpen}
              />
            ) : activeTab === "output" ? (
              <OutputTab
                pipelineId={pipelineId}
                nodeId={node.id}
                requestedRunId={requestedRunId}
                onRequestedRunHandled={onRequestedRunHandled}
                onRunFrom={() => onRunFrom(node.id)}
              />
            ) : activeTab === "code" ? (
              <div
                className="flex items-center justify-center h-full text-xs"
                style={{ color: "var(--text-muted)" }}
              >
                Editing code for {node.name || definition.label}
              </div>
            ) : null}
          </div>
        </div>
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
