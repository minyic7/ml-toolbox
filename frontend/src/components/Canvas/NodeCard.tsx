import { memo } from "react";
import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import { Button } from "@/components/ui/button";
import {
  type NodeDefinition,
  type NodeStatus,
  type PortType,
  PORT_COLORS,
} from "@/lib/types";

const STATUS_COLORS: Record<string, string> = {
  idle: "#D1D5DB",
  pending: "#D1D5DB",
  running: "#3B82F6",
  done: "#22C55E",
  error: "#EF4444",
  skipped: "#F59E0B",
};

export type NodeTab = "params" | "code" | "output";

export interface NodeCardData {
  label: string;
  definition: NodeDefinition;
  status: NodeStatus | "idle";
  params: Record<string, string | number | boolean>;
  onTabClick?: (nodeId: string, tab: NodeTab) => void;
  [key: string]: unknown;
}

type NodeCardNode = Node<NodeCardData, "nodeCard">;

function PortBadge({ type }: { type: PortType }) {
  return (
    <span
      className="rounded px-1 py-0.5 text-[10px] font-medium leading-none text-white"
      style={{ backgroundColor: PORT_COLORS[type] }}
    >
      {type}
    </span>
  );
}

function NodeCardComponent({
  id,
  data,
  selected,
}: NodeProps<NodeCardNode>) {
  const { label, definition, status, onTabClick } = data;
  const statusColor = STATUS_COLORS[status] ?? STATUS_COLORS.idle;
  const isRunning = status === "running";

  return (
    <div
      className="min-w-[200px] rounded-lg bg-white shadow"
      style={{
        border: selected ? "2px solid #3B82F6" : "1px solid #E5E7EB",
      }}
      onClick={() => onTabClick?.(id, "params")}
    >
      {/* Header */}
      <div className="flex items-center justify-between border-b border-[#E5E7EB] px-3 py-2">
        <span className="text-sm font-medium text-gray-900">{label}</span>
        <span
          className={`inline-block h-2 w-2 rounded-full${isRunning ? " animate-pulse" : ""}`}
          style={{ backgroundColor: statusColor }}
        />
      </div>

      {/* Ports */}
      <div className="relative px-3 py-2">
        {/* Input ports (left side) */}
        {definition.inputs.map((port) => (
          <div
            key={`in-${port.name}`}
            className="relative flex items-center gap-1.5 py-0.5"
          >
            <Handle
              type="target"
              position={Position.Left}
              id={port.name}
              className="!-left-[9px] !h-3 !w-3 !rounded-full !border-2 !border-white"
              style={{ backgroundColor: PORT_COLORS[port.type], top: "auto" }}
            />
            <span className="text-xs text-gray-600">{port.name}</span>
            <PortBadge type={port.type} />
          </div>
        ))}

        {/* Output ports (right side) */}
        {definition.outputs.map((port) => (
          <div
            key={`out-${port.name}`}
            className="relative flex items-center justify-end gap-1.5 py-0.5"
          >
            <PortBadge type={port.type} />
            <span className="text-xs text-gray-600">{port.name}</span>
            <Handle
              type="source"
              position={Position.Right}
              id={port.name}
              className="!-right-[9px] !h-3 !w-3 !rounded-full !border-2 !border-white"
              style={{ backgroundColor: PORT_COLORS[port.type], top: "auto" }}
            />
          </div>
        ))}

        {definition.inputs.length === 0 && definition.outputs.length === 0 && (
          <div className="py-1 text-xs text-gray-400">No ports</div>
        )}
      </div>

      {/* Footer - tab buttons */}
      <div className="flex border-t border-[#E5E7EB]">
        {(["params", "code", "output"] as NodeTab[]).map((tab) => (
          <Button
            key={tab}
            variant="ghost"
            size="xs"
            className="flex-1 rounded-none text-[11px] capitalize text-gray-500 hover:bg-gray-50 hover:text-gray-900"
            onClick={(e) => {
              e.stopPropagation();
              onTabClick?.(id, tab);
            }}
          >
            {tab === "params" ? "Params" : tab === "code" ? "Code" : "Output"}
          </Button>
        ))}
      </div>
    </div>
  );
}

export const NodeCard = memo(NodeCardComponent);
