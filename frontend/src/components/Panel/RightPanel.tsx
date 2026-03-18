import { useState } from "react";
import type { Node } from "@xyflow/react";
import type { NodeCardData, NodeTab } from "@/components/Canvas";
import type { NodeOutputState } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import { ParamsTab } from "./ParamsTab";
import { CodeTab } from "./CodeTab";
import { OutputTab } from "./OutputTab";

const TABS: { key: NodeTab; label: string }[] = [
  { key: "params", label: "Params" },
  { key: "code", label: "Code" },
  { key: "output", label: "Output" },
];

export interface RightPanelProps {
  node: Node<NodeCardData> | null;
  activeTab?: NodeTab;
  onTabChange?: (tab: NodeTab) => void;
  onParamsChange?: (params: Record<string, string | number | boolean>) => void;
  onCodeChange?: (code: string) => void;
  onClose?: () => void;
  outputState?: NodeOutputState;
  downloadUrl?: string;
}

export function RightPanel({
  node,
  activeTab: controlledTab,
  onTabChange,
  onParamsChange,
  onCodeChange,
  onClose,
  outputState,
  downloadUrl,
}: RightPanelProps) {
  const [internalTab, setInternalTab] = useState<NodeTab>("params");
  const activeTab = controlledTab ?? internalTab;

  const handleTabChange = (tab: NodeTab) => {
    setInternalTab(tab);
    onTabChange?.(tab);
  };

  if (!node) {
    return (
      <div className="flex h-full flex-col p-4 text-muted-foreground">
        <h2 className="mb-4 text-sm font-semibold text-foreground">
          Properties
        </h2>
        <p className="text-xs">Select a node to view its properties.</p>
      </div>
    );
  }

  const { definition, params, code } = node.data;

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
            type="button"
            className={`flex-1 py-1.5 text-xs font-medium transition-colors ${
              activeTab === tab.key
                ? "border-b-2 border-primary text-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
            onClick={() => handleTabChange(tab.key)}
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
            values={params}
            onParamChange={(paramName, value) => {
              onParamsChange?.({ ...params, [paramName]: value });
            }}
          />
        )}
        {activeTab === "code" && (
          <CodeTab
            code={code ?? definition.default_code ?? ""}
            readOnly={!definition.default_code && !code}
            onChange={(value) => onCodeChange?.(value)}
          />
        )}
        {activeTab === "output" && (
          <OutputTab outputState={outputState} downloadUrl={downloadUrl} />
        )}
      </div>
    </div>
  );
}
