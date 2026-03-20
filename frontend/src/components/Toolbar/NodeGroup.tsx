import type { LucideIcon } from "lucide-react";
import type { NodeDefinition } from "../../lib/types";
import NodeIconChip from "./NodeIconChip";

interface CategoryColors {
  bg: string;
  border: string;
  icon: string;
  dot: string;
}

interface NodeGroupProps {
  category: string;
  nodes: NodeDefinition[];
  colors: CategoryColors;
  iconMap: Record<string, LucideIcon>;
  defaultIcon: LucideIcon;
  onAddNode: (nodeType: string) => void;
  isLast: boolean;
}

export default function NodeGroup({
  category,
  nodes,
  colors,
  iconMap,
  defaultIcon,
  onAddNode,
  isLast,
}: NodeGroupProps) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        paddingRight: isLast ? 0 : 12,
        marginRight: isLast ? 0 : 12,
        borderRight: isLast ? "none" : "1px solid #F1F5F9",
      }}
    >
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 800,
          fontSize: 9,
          textTransform: "uppercase",
          letterSpacing: "0.08em",
          color: "#94A3B8",
          whiteSpace: "nowrap",
          userSelect: "none",
        }}
      >
        {category}
      </span>
      <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
        {nodes.map((node) => (
          <NodeIconChip
            key={node.type}
            icon={iconMap[node.type] ?? iconMap[node.category] ?? defaultIcon}
            label={node.label}
            colors={colors}
            onClick={() => onAddNode(node.type)}
          />
        ))}
      </div>
    </div>
  );
}
