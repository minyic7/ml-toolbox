import { useState } from "react";
import type { NodeDefinition, PipelineNode, NodeOutputState } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import { ParamsTab } from "./ParamsTab";
import { CodeTab } from "./CodeTab";
import { OutputTab } from "./OutputTab";

type Tab = "params" | "code" | "output";

export interface RightPanelProps {
  node?: PipelineNode;
  definition?: NodeDefinition;
  outputState?: NodeOutputState;
  downloadUrl?: string;
  onParamChange?: (nodeId: string, paramName: string, value: string | number | boolean) => void;
  onCodeChange?: (nodeId: string, code: string) => void;
  onCodeBlur?: (nodeId: string) => void;
  onClose?: () => void;
}

const TABS: { key: Tab; label: string }[] = [
  { key: "params", label: "Params" },
  { key: "code", label: "Code" },
  { key: "output", label: "Output" },
];

export function RightPanel({
  node,
  definition,
  outputState,
  downloadUrl,
  onParamChange,
  onCodeChange,
  onCodeBlur,
  onClose,
}: RightPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>("params");

  if (!node || !definition) {
    return (
      <div className="flex h-full flex-col p-4 text-muted-foreground">
        <h2 className="mb-4 text-sm font-semibold text-foreground">
          Properties
        </h2>
        <p className="text-xs">Select a node to view its properties.</p>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-3 py-2">
        <div className="flex items-center gap-2 overflow-hidden">
          <span className="truncate text-sm font-semibold text-foreground">
            {definition.label}
          </span>
          <span className="shrink-0 rounded bg-secondary px-1.5 py-0.5 text-[10px] text-muted-foreground">
            {definition.type}
          </span>
        </div>
        <Button variant="ghost" size="icon-xs" onClick={onClose}>
          <X className="size-3.5" />
        </Button>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-border">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            className={`flex-1 py-1.5 text-xs font-medium transition-colors ${
              activeTab === tab.key
                ? "border-b-2 border-primary text-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="flex flex-1 flex-col overflow-y-auto p-3">
        {activeTab === "params" && (
          <ParamsTab
            params={definition.params}
            values={node.params}
            onParamChange={(paramName, value) =>
              onParamChange?.(node.id, paramName, value)
            }
          />
        )}
        {activeTab === "code" && (
          <CodeTab
            code={node.code ?? definition.default_code ?? ""}
            readOnly={!definition.default_code && !node.code}
            onChange={
              onCodeChange
                ? (value) => onCodeChange(node.id, value)
                : undefined
            }
            onBlur={
              onCodeBlur ? () => onCodeBlur(node.id) : undefined
            }
          />
        )}
        {activeTab === "output" && (
          <OutputTab
            outputState={outputState}
            downloadUrl={downloadUrl}
          />
        )}
      </div>
    </div>
  );
}
