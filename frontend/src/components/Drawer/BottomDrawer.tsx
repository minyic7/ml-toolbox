import { useEffect, useState } from "react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import DrawerHeader from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";
import { OutputTab } from "./OutputTab";

type DrawerTab = "params" | "code" | "output";

interface BottomDrawerProps {
  pipelineId: string;
  node: NodeInstance | null;
  definition: NodeDefinition | null;
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  paramSaving?: boolean;
  onClose: () => void;
  requestedTab?: string | null;
  onRequestedTabHandled?: () => void;
  requestedRunId?: string | null;
  onRequestedRunHandled?: () => void;
  onRunFrom: (nodeId: string) => void;
  codePaneOpen?: boolean;
  onCodeTabClick?: () => void;
  onCodePaneClose?: () => void;
}

const DRAWER_HEIGHT = 172;
const DRAWER_HEADER_HEIGHT = 38;

export default function BottomDrawer({
  pipelineId,
  node,
  definition,
  onParamChange,
  paramSaving,
  onClose,
  requestedTab,
  onRequestedTabHandled,
  requestedRunId,
  onRequestedRunHandled,
  onRunFrom,
  codePaneOpen,
  onCodeTabClick,
  onCodePaneClose,
}: BottomDrawerProps) {
  const [activeTab, setActiveTab] = useState<DrawerTab>("params");

  // Handle external tab requests
  useEffect(() => {
    if (requestedTab === "params" || requestedTab === "output") {
      setActiveTab(requestedTab);
      onRequestedTabHandled?.();
    } else if (requestedTab === "code") {
      setActiveTab("code");
      onCodeTabClick?.();
      onRequestedTabHandled?.();
    }
  }, [requestedTab, onRequestedTabHandled, onCodeTabClick]);

  const handleTabChange = (tab: DrawerTab) => {
    setActiveTab(tab);
    if (tab === "code") {
      onCodeTabClick?.();
    } else if (codePaneOpen) {
      // Clicking Params or Output closes the code pane
      onCodePaneClose?.();
    }
  };

  // When code pane closes externally, switch back to params
  useEffect(() => {
    if (!codePaneOpen && activeTab === "code") {
      setActiveTab("params");
    }
  }, [codePaneOpen, activeTab]);

  const isOpen = node !== null;
  // When code pane is open, drawer shrinks to header-only
  const drawerHeight = isOpen
    ? codePaneOpen
      ? DRAWER_HEADER_HEIGHT
      : DRAWER_HEIGHT
    : 0;

  return (
    <div
      style={{
        height: isOpen ? drawerHeight : 0,
        minHeight: isOpen ? drawerHeight : 0,
        overflow: "hidden",
        transition: "height 220ms ease, min-height 220ms ease",
        borderTop: isOpen ? "1px solid var(--border-default)" : "none",
        background: "var(--node-bg)",
        flexShrink: 0,
      }}
    >
      {node && definition && (
        <>
          <DrawerHeader
            node={node}
            definition={definition}
            activeTab={activeTab}
            onTabChange={handleTabChange}
            onClose={onClose}
          />
          {!codePaneOpen && (
            <div
              style={{
                height: DRAWER_HEIGHT - DRAWER_HEADER_HEIGHT,
                overflowY: "auto",
              }}
            >
              {activeTab === "params" && (
                <ParamsTab
                  params={definition.params}
                  values={buildParamValues(node)}
                  onChange={(name, value) => onParamChange(node.id, name, value)}
                  disabled={paramSaving}
                />
              )}
              {activeTab === "output" && (
                <OutputTab
                  pipelineId={pipelineId}
                  nodeId={node.id}
                  requestedRunId={requestedRunId}
                  onRequestedRunHandled={onRequestedRunHandled}
                  onRunFrom={() => onRunFrom(node.id)}
                />
              )}
            </div>
          )}
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
