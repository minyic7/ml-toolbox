import type { NodeInstance, NodeDefinition } from "../../lib/types";
import DrawerHeader from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";

interface BottomDrawerProps {
  pipelineId: string;
  node: NodeInstance | null;
  definition: NodeDefinition | null;
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  paramSaving?: boolean;
  onClose: () => void;
  onRunFrom: (nodeId: string) => void;
  onCodeClick: () => void;
  onOutputClick: () => void;
  rightPanelOpen: boolean;
  rightPanelMode: "code" | "output";
}

const DRAWER_HEIGHT = 220;
const DRAWER_HEADER_HEIGHT = 38;

export default function BottomDrawer({
  node,
  definition,
  onParamChange,
  paramSaving,
  onClose,
  onCodeClick,
  onOutputClick,
  rightPanelOpen,
  rightPanelMode,
}: BottomDrawerProps) {
  const isOpen = node !== null;
  const drawerHeight = isOpen ? DRAWER_HEIGHT : 0;

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
            onClose={onClose}
            onCodeClick={onCodeClick}
            onOutputClick={onOutputClick}
            rightPanelOpen={rightPanelOpen}
            rightPanelMode={rightPanelMode}
          />
          <div
            style={{
              height: DRAWER_HEIGHT - DRAWER_HEADER_HEIGHT,
              overflowY: "auto",
            }}
          >
            <ParamsTab
              params={definition.params}
              values={buildParamValues(node)}
              onChange={(name, value) => onParamChange(node.id, name, value)}
              disabled={paramSaving}
            />
          </div>
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
