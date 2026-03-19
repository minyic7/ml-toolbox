import { useState } from "react";
import type { NodeInstance, NodeDefinition, OutputPreview } from "../../lib/types";
import { cn } from "../../lib/utils";
import { ParamsTab } from "./ParamsTab";
import { CodeTab } from "./CodeTab";
import { OutputTab } from "./OutputTab";

type Tab = "params" | "code" | "output";

interface RightPanelProps {
  node: NodeInstance | null;
  definition: NodeDefinition | null;
  output: OutputPreview | null;
  downloadUrl: string | null;
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  onCodeChange: (nodeId: string, code: string) => void;
  onCodeBlur: (nodeId: string) => void;
  onClose: () => void;
}

const TABS: { key: Tab; label: string }[] = [
  { key: "params", label: "Params" },
  { key: "code", label: "Code" },
  { key: "output", label: "Output" },
];

export function RightPanel({
  node,
  definition,
  output,
  downloadUrl,
  onParamChange,
  onCodeChange,
  onCodeBlur,
  onClose,
}: RightPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>("params");

  const isOpen = node !== null;

  return (
    <div
      className={cn(
        "flex flex-col border-l overflow-hidden transition-all duration-250",
        isOpen ? "w-[360px] min-w-[360px]" : "w-0 min-w-0",
      )}
      style={{
        borderColor: "var(--border-default)",
        backgroundColor: "var(--node-bg)",
        transitionDuration: "250ms",
      }}
    >
      {node && definition && (
        <>
          {/* Header */}
          <div
            className="flex items-center justify-between border-b px-4 py-3"
            style={{ borderColor: "var(--border-default)" }}
          >
            <div className="flex flex-col gap-0.5">
              <span
                className="text-sm font-semibold"
                style={{ color: "var(--text-primary)" }}
              >
                {definition.label}
              </span>
              <span
                className="text-xs"
                style={{ color: "var(--text-muted)" }}
              >
                {definition.category}
              </span>
            </div>
            <button
              onClick={onClose}
              className="flex h-6 w-6 items-center justify-center rounded-md transition-colors hover:opacity-70"
              style={{ color: "var(--text-muted)" }}
              aria-label="Close panel"
            >
              <svg
                width="14"
                height="14"
                viewBox="0 0 14 14"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
              >
                <path d="M1 1l12 12M13 1L1 13" />
              </svg>
            </button>
          </div>

          {/* Tabs */}
          <div
            className="flex border-b"
            style={{ borderColor: "var(--border-default)" }}
          >
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
              />
            )}
            {activeTab === "code" && (
              <CodeTab
                code={node.code}
                defaultCode={definition.default_code}
                onChange={(code) => onCodeChange(node.id, code)}
                onBlur={() => onCodeBlur(node.id)}
              />
            )}
            {activeTab === "output" && (
              <OutputTab output={output} downloadUrl={downloadUrl} />
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
