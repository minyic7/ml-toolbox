import { useEffect, useState } from "react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import DrawerHeader from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";
import { OutputTab } from "./OutputTab";

type DrawerTab = "params" | "output";

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
}

const DRAWER_HEIGHT = 172;

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
}: BottomDrawerProps) {
  const [activeTab, setActiveTab] = useState<DrawerTab>("params");

  // Handle external tab requests
  useEffect(() => {
    if (requestedTab === "params" || requestedTab === "output") {
      setActiveTab(requestedTab);
      onRequestedTabHandled?.();
    } else if (requestedTab === "code") {
      // Code tab not in drawer — ignore, just acknowledge
      onRequestedTabHandled?.();
    }
  }, [requestedTab, onRequestedTabHandled]);

  const isOpen = node !== null;

  return (
    <div
      style={{
        height: isOpen ? DRAWER_HEIGHT : 0,
        minHeight: isOpen ? DRAWER_HEIGHT : 0,
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
            onTabChange={setActiveTab}
            onClose={onClose}
          />
          <div
            style={{
              height: DRAWER_HEIGHT - 38,
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
