import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Table2, BarChart3 } from "lucide-react";
import { useExecutionStore } from "../../store/executionStore";
import { getMetadata, getEdaContext } from "../../lib/api";
import type { NodeInstance, NodeDefinition, Edge } from "../../lib/types";
import DrawerHeader from "./DrawerHeader";
import { ParamsTab } from "./ParamsTab";
import SchemaModal from "./SchemaModal";
import EdaContextModal from "./EdaContextModal";

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
  rightPanelOpen: boolean;
  rightPanelMode: "code" | "output" | "info" | "terminal";
}

const DRAWER_HEADER_HEIGHT = 38;

const contextBtnStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 5,
  fontSize: 11,
  fontWeight: 600,
  textTransform: "uppercase",
  letterSpacing: "0.05em",
  color: "var(--text-muted)",
  background: "transparent",
  border: "1px solid var(--border-default)",
  borderRadius: 6,
  padding: "5px 10px",
  cursor: "pointer",
  fontFamily: "'Inter', sans-serif",
};

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
  rightPanelOpen,
  rightPanelMode,
}: BottomDrawerProps) {
  const isOpen = node !== null;
  const isRunning = useExecutionStore((s) => s.isRunning);
  const [schemaModalOpen, setSchemaModalOpen] = useState(false);
  const [edaModalOpen, setEdaModalOpen] = useState(false);

  const isIngestNode = definition?.category === "Ingest";

  // Fetch metadata for schema button (all nodes)
  const { data: metadata } = useQuery({
    queryKey: ["metadata", pipelineId, node?.id],
    queryFn: async () => {
      const res = await getMetadata(pipelineId, node!.id);
      if (res.metadata && typeof res.metadata === "object" && "columns" in res.metadata) {
        return res.metadata as { columns: Record<string, unknown> };
      }
      return null;
    },
    enabled: !!pipelineId && !!node?.id,
  });
  const hasMetadata = !!metadata?.columns;
  const columnCount = hasMetadata ? Object.keys(metadata.columns).length : 0;

  // Fetch EDA context availability
  const { data: edaData } = useQuery({
    queryKey: ["eda-context", pipelineId, node?.id],
    queryFn: () => getEdaContext(pipelineId, node!.id),
    enabled: !!pipelineId && !!node?.id,
  });
  const hasEdaContext = !!edaData?.eda_context;

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
            <div
              style={{
                display: "flex",
                gap: 4,
                padding: "8px 16px",
                borderTop: "1px solid var(--border-default)",
              }}
            >
              <button
                onClick={() => setSchemaModalOpen(true)}
                style={{
                  ...contextBtnStyle,
                  ...(hasMetadata
                    ? { color: "var(--accent-primary)" }
                    : {}),
                }}
              >
                <Table2 size={12} />
                Schema{hasMetadata ? ` (${columnCount})` : ""}
              </button>
              <button
                onClick={() => setEdaModalOpen(true)}
                style={{
                  ...contextBtnStyle,
                  ...(hasEdaContext
                    ? { color: "var(--accent-primary)" }
                    : {}),
                }}
              >
                <BarChart3 size={12} />
                EDA Context
              </button>
            </div>
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
      {node && (
        <>
          <SchemaModal
            open={schemaModalOpen}
            onClose={() => setSchemaModalOpen(false)}
            pipelineId={pipelineId}
            nodeId={node.id}
            readOnly={!isIngestNode}
          />
          <EdaContextModal
            open={edaModalOpen}
            onClose={() => setEdaModalOpen(false)}
            pipelineId={pipelineId}
            nodeId={node.id}
          />
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
