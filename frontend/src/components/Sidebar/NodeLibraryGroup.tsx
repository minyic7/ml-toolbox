import { useState } from "react";
import type { NodeDefinition } from "../../lib/types";
import { CATEGORY_COLORS } from "../../lib/portColors";
import NodeLibraryItem from "./NodeLibraryItem";

interface NodeLibraryGroupProps {
  category: string;
  nodes: NodeDefinition[];
  onAddNode: (nodeType: string) => void;
}

export default function NodeLibraryGroup({
  category,
  nodes,
  onAddNode,
}: NodeLibraryGroupProps) {
  const [open, setOpen] = useState(true);
  const color = CATEGORY_COLORS[category] ?? "var(--text-muted)";

  return (
    <div className="mb-1">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs font-semibold uppercase tracking-wide transition-colors hover:bg-[var(--canvas-bg)]"
        style={{ color }}
      >
        <span
          className="inline-block h-2 w-2 rounded-full"
          style={{ backgroundColor: color }}
        />
        <span className="flex-1 text-left">{category}</span>
        <span
          className="text-[10px] transition-transform"
          style={{
            color: "var(--text-muted)",
            transform: open ? "rotate(0deg)" : "rotate(-90deg)",
          }}
        >
          ▼
        </span>
      </button>
      {open && (
        <div className="ml-2">
          {nodes.map((node) => (
            <NodeLibraryItem key={node.type} node={node} onAdd={onAddNode} />
          ))}
        </div>
      )}
    </div>
  );
}
