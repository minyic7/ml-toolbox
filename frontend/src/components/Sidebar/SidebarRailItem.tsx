import { useRef, useState } from "react";
import type { LucideIcon } from "lucide-react";
import type { NodeDefinition } from "../../lib/types";
import { CATEGORY_COLORS } from "../../lib/portColors";

interface SidebarRailItemProps {
  category: string;
  icon: LucideIcon;
  nodes: NodeDefinition[];
  onAddNode: (nodeType: string) => void;
}

export default function SidebarRailItem({
  category,
  icon: Icon,
  nodes,
  onAddNode,
}: SidebarRailItemProps) {
  const [hovered, setHovered] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const color = CATEGORY_COLORS[category] ?? "var(--text-muted)";

  function handleEnter() {
    clearTimeout(timeoutRef.current);
    setHovered(true);
  }

  function handleLeave() {
    timeoutRef.current = setTimeout(() => setHovered(false), 150);
  }

  return (
    <div
      className="relative flex justify-center"
      onMouseEnter={handleEnter}
      onMouseLeave={handleLeave}
    >
      <button
        className="flex h-8 w-8 items-center justify-center rounded transition-colors hover:bg-[var(--canvas-bg)]"
        title={category}
        aria-label={`${category} nodes`}
      >
        <Icon size={18} color={color} strokeWidth={2} />
      </button>

      {hovered && (
        <div
          className="absolute left-full top-0 z-50 ml-1 min-w-[160px] rounded-md border bg-popover p-1 shadow-md"
          onMouseEnter={handleEnter}
          onMouseLeave={handleLeave}
        >
          <div
            className="mb-1 px-2 py-1 text-xs font-semibold uppercase tracking-wide"
            style={{ color }}
          >
            {category}
          </div>
          {nodes.map((node) => (
            <button
              key={node.type}
              className="flex w-full rounded px-2 py-1 text-left text-xs transition-colors hover:bg-[var(--canvas-bg)]"
              style={{ color: "var(--text-primary)" }}
              onClick={() => {
                onAddNode(node.type);
                setHovered(false);
              }}
            >
              {node.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
