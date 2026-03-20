import { useEffect, useState } from "react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import DrawerHeader from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";
import { DrawerOutputTab } from "./DrawerOutputTab";

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
  rightPanelOpen?: boolean;
  onCodeTabClick?: () => void;
  onOutputTabClick?: () => void;
}

const DRAWER_HEIGHT = 220;
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
  rightPanelOpen,
  onCodeTabClick,
  onOutputTabClick,
}: BottomDrawerProps) {
  const [activeTab, setActiveTab] = useState<DrawerTab>("params");

  // Handle external tab requests
  useEffect(() => {
    if (requestedTab === "params") {
      setActiveTab(requestedTab);
      onRequestedTabHandled?.();
    } else if (requestedTab === "code") {
      setActiveTab("code");
      onCodeTabClick?.();
      onRequestedTabHandled?.();
    } else if (requestedTab === "output") {
      setActiveTab("output");
      onOutputTabClick?.();
      onRequestedTabHandled?.();
    }
  }, [requestedTab, onRequestedTabHandled, onCodeTabClick, onOutputTabClick]);

  const handleTabChange = (tab: DrawerTab) => {
    setActiveTab(tab);
    if (tab === "code") {
      onCodeTabClick?.();
    } else if (tab === "output") {
      onOutputTabClick?.();
    }
    // Switching tabs never closes panels — only explicit close buttons do
  };

  // When right panel closes externally, switch back to params if on code or output
  useEffect(() => {
    if (!rightPanelOpen && (activeTab === "code" || activeTab === "output")) {
      setActiveTab("params");
    }
  }, [rightPanelOpen, activeTab]);

  const isOpen = node !== null;
  // When right panel is open, drawer shrinks to header-only
  const drawerHeight = isOpen
    ? rightPanelOpen
      ? DRAWER_HEADER_HEIGHT
      : DRAWER_HEIGHT
    : 0;

  return (
    <div
      style={{
        position: "absolute",
        bottom: 8,
        left: 12,
        right: 12,
        zIndex: 20,
        height: isOpen ? drawerHeight : 0,
        overflow: "hidden",
        transition: "height 220ms ease",
        borderRadius: 12,
        boxShadow: isOpen ? "0 4px 24px rgba(0,0,0,0.08)" : "none",
        background: "rgba(255,255,255,0.95)",
        backdropFilter: "blur(8px)",
        WebkitBackdropFilter: "blur(8px)",
        border: isOpen ? "1px solid var(--border-default)" : "none",
        pointerEvents: isOpen ? "auto" : "none",
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
          {!rightPanelOpen && (
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
                <DrawerOutputTab
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
  for (const p of node.params) values[p.name] = p.default;
  return values;
}
