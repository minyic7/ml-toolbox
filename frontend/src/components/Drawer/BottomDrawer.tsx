import { useExecutionStore } from "../../store/executionStore";
import type { NodeInstance, NodeDefinition, Edge } from "../../lib/types";
import DrawerHeader from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";
import SchemaEditor from "./SchemaEditor";

interface BottomDrawerProps {
  pipelineId: string;
  node: NodeInstance | null;
  definition: NodeDefinition | null;
  edges: Edge[];
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  paramSaving?: boolean;
  onClose: () => void;
  onRunFrom: (nodeId: string) => void;
  onCodeClick: () => void;
  onOutputClick: () => void;
  onInfoClick: () => void;
  onOpenTerminal?: (nodeId: string) => void;
  rightPanelOpen: boolean;
  rightPanelMode: "code" | "output" | "info" | "terminal";
}

const DRAWER_HEADER_HEIGHT = 38;

export default function BottomDrawer({
  pipelineId,
  node,
  definition,
  edges,
  onParamChange,
  paramSaving,
  onClose,
  onRunFrom,
  onCodeClick,
  onOutputClick,
  onInfoClick,
  onOpenTerminal,
  rightPanelOpen,
  rightPanelMode,
}: BottomDrawerProps) {
  const isOpen = node !== null;
  const isRunning = useExecutionStore((s) => s.isRunning);

  return (
    <div
      style={{
        position: "absolute",
        bottom: 8,
        left: 12,
        right: 12,
        zIndex: 20,
        height: isOpen ? "auto" : 0,
        maxHeight: isOpen ? "50vh" : 0,
        minHeight: isOpen ? 120 : 0,
        overflow: "hidden",
        transition: "max-height 220ms ease, min-height 220ms ease",
        borderRadius: 12,
        boxShadow: isOpen ? "0 4px 24px rgba(0,0,0,0.08)" : "none",
        background: "rgba(255,255,255,0.95)",
        backdropFilter: "blur(8px)",
        WebkitBackdropFilter: "blur(8px)",
        border: isOpen ? "1px solid var(--border-default)" : "none",
        pointerEvents: isOpen ? "auto" : "none",
        display: "flex",
        flexDirection: "column",
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
            onInfoClick={onInfoClick}
            rightPanelOpen={rightPanelOpen}
            rightPanelMode={rightPanelMode}
          />
          <div
            style={{
              flex: 1,
              overflowY: "auto",
              minHeight: 120 - DRAWER_HEADER_HEIGHT,
            }}
          >
            <ParamsTab
              params={definition.params}
              values={buildParamValues(node)}
              onChange={(name, value) => onParamChange(node.id, name, value)}
              disabled={paramSaving}
              pipelineId={pipelineId}
              edges={edges}
              nodeId={node.id}
              nodeInputs={definition.inputs}
            />
            {definition.category === "Ingest" && (
              <div
                style={{
                  borderTop: "1px solid var(--border-default)",
                  marginTop: 4,
                }}
              >
                <div
                  style={{
                    padding: "8px 16px 4px",
                    fontFamily: "'Inter', sans-serif",
                    fontWeight: 700,
                    fontSize: 10,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "var(--text-muted)",
                  }}
                >
                  Schema
                </div>
                <SchemaEditor
                  pipelineId={pipelineId}
                  nodeId={node.id}
                  onOpenTerminal={onOpenTerminal}
                />
              </div>
            )}
          </div>
          <div
            style={{
              padding: "8px 16px 12px",
              borderTop: "1px solid var(--border-default)",
              display: "flex",
              justifyContent: "flex-end",
            }}
          >
            <button
              onClick={() => onRunFrom(node.id)}
              disabled={isRunning}
              style={{
                padding: "6px 18px",
                fontSize: 12,
                fontWeight: 600,
                fontFamily: "'Inter', sans-serif",
                borderRadius: 6,
                border: "none",
                background: "var(--accent-primary)",
                color: "#fff",
                cursor: isRunning ? "not-allowed" : "pointer",
                opacity: isRunning ? 0.5 : 1,
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              {isRunning ? (
                <svg className="topbar-spinner" width="12" height="12" viewBox="0 0 12 12" fill="none">
                  <circle cx="6" cy="6" r="5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeDasharray="20 10" opacity="0.8" />
                </svg>
              ) : (
                <svg width="10" height="11" viewBox="0 0 8 9" fill="currentColor">
                  <path d="M1 1.5v6l6-3-6-3z" />
                </svg>
              )}
              {isRunning ? "Running..." : "Run"}
            </button>
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
