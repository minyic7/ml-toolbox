import type { NodeDefinition, PortDefinition } from "../../lib/types";
import { PORT_COLORS } from "../../lib/portColors";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface NodeLibraryItemProps {
  node: NodeDefinition;
  onAdd: (nodeType: string) => void;
}

function PortDots({ ports }: { ports: PortDefinition[] }) {
  return (
    <span className="flex items-center gap-0.5">
      {ports.map((p) => (
        <span
          key={p.name}
          title={`${p.name} (${p.type})`}
          className="inline-block h-2 w-2 rounded-full"
          style={{ backgroundColor: PORT_COLORS[p.type] }}
        />
      ))}
    </span>
  );
}

function formatPortList(ports: PortDefinition[]): string {
  return ports.map((p) => `${p.name} (${p.type})`).join(", ");
}

export default function NodeLibraryItem({ node, onAdd }: NodeLibraryItemProps) {
  function handleDragStart(e: React.DragEvent) {
    e.dataTransfer.setData("application/ml-toolbox-node", node.type);
    e.dataTransfer.effectAllowed = "copy";
  }

  const tooltipLines = [node.description];
  if (node.inputs.length > 0) tooltipLines.push(`Inputs: ${formatPortList(node.inputs)}`);
  if (node.outputs.length > 0) tooltipLines.push(`Outputs: ${formatPortList(node.outputs)}`);

  return (
    <Tooltip delayDuration={600}>
      <TooltipTrigger asChild>
        <div
          draggable
          onDragStart={handleDragStart}
          onClick={() => onAdd(node.type)}
          className="flex cursor-grab items-center justify-between rounded px-2 py-1.5 text-sm transition-colors hover:bg-[rgba(55,138,221,0.08)] active:cursor-grabbing"
        >
          <span className="truncate" style={{ color: "var(--text-primary)" }}>
            {node.label}
          </span>
          <span className="ml-2 flex items-center gap-1.5">
            {node.inputs.length > 0 && <PortDots ports={node.inputs} />}
            {node.outputs.length > 0 && (
              <>
                <span className="text-[10px]" style={{ color: "var(--text-muted)" }}>→</span>
                <PortDots ports={node.outputs} />
              </>
            )}
          </span>
        </div>
      </TooltipTrigger>
      <TooltipContent side="right" className="max-w-xs whitespace-pre-line text-xs">
        {tooltipLines.join("\n")}
      </TooltipContent>
    </Tooltip>
  );
}
