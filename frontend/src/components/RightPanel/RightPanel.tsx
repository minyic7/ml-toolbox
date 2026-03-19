import { useState } from "react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import { cn } from "../../lib/utils";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import { ParamsTab } from "./ParamsTab";
import { CodeTab } from "./CodeTab";
import { OutputTab } from "./OutputTab";

type Tab = "params" | "code" | "output";

interface RightPanelProps {
  pipelineId: string;
  node: NodeInstance | null;
  definition: NodeDefinition | null;
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
  pipelineId,
  node,
  definition,
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
