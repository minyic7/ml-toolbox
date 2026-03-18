import { useState } from "react";
import { type NodeDefinition, type PortType, PORT_COLORS } from "@/lib/types";

interface SidebarProps {
  definitions: NodeDefinition[];
}

function PortDot({ type }: { type: PortType }) {
  return (
    <span
      className="inline-block h-2 w-2 rounded-full"
      style={{ backgroundColor: PORT_COLORS[type] }}
      title={type}
    />
  );
}

function NodeTypeCard({ definition }: { definition: NodeDefinition }) {
  const onDragStart = (event: React.DragEvent) => {
    event.dataTransfer.setData(
      "application/reactflow",
      JSON.stringify(definition),
    );
    event.dataTransfer.effectAllowed = "move";
  };

  return (
    <div
      className="cursor-grab rounded-md border border-border bg-secondary p-2.5 transition-colors hover:border-ring active:cursor-grabbing"
      draggable
      onDragStart={onDragStart}
    >
      <div className="mb-1 text-xs font-medium text-foreground">
        {definition.label}
      </div>
      <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
        {definition.inputs.length > 0 && (
          <span className="flex items-center gap-1">
            In:
            {definition.inputs.map((p) => (
              <PortDot key={p.name} type={p.type} />
            ))}
          </span>
        )}
        {definition.outputs.length > 0 && (
          <span className="flex items-center gap-1">
            Out:
            {definition.outputs.map((p) => (
              <PortDot key={p.name} type={p.type} />
            ))}
          </span>
        )}
      </div>
    </div>
  );
}

export function NodeLibrarySidebar({ definitions }: SidebarProps) {
  const [search, setSearch] = useState("");

  const filtered = definitions.filter(
    (d) =>
      d.label.toLowerCase().includes(search.toLowerCase()) ||
      d.category.toLowerCase().includes(search.toLowerCase()),
  );

  // Group by category
  const grouped = filtered.reduce<Record<string, NodeDefinition[]>>(
    (acc, def) => {
      (acc[def.category] ??= []).push(def);
      return acc;
    },
    {},
  );

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-border px-4 py-3">
        <h2 className="mb-2 text-sm font-semibold text-foreground">
          Node Library
        </h2>
        <input
          type="text"
          placeholder="Search nodes..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full rounded-md border border-border bg-input px-2.5 py-1.5 text-xs text-foreground placeholder:text-muted-foreground focus:border-ring focus:outline-none"
        />
      </div>
      <div className="flex-1 overflow-y-auto px-4 py-3">
        {Object.entries(grouped).map(([category, defs]) => (
          <div key={category} className="mb-4">
            <h3 className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {category}
            </h3>
            <div className="flex flex-col gap-1.5">
              {defs.map((def) => (
                <NodeTypeCard key={def.type} definition={def} />
              ))}
            </div>
          </div>
        ))}
        {filtered.length === 0 && (
          <p className="text-xs text-muted-foreground">No nodes found.</p>
        )}
      </div>
    </div>
  );
}
