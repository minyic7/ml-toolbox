import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Download,
  Shuffle,
  Brain,
  BarChart3,
  Upload,
  Box,
  FileText,
  Database,
  Zap,
  ArrowLeftRight,
  CircleDot,
  Wrench,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { getNodeDefinitions } from "../../lib/api";
import type { NodeDefinition } from "../../lib/types";
import NodeGroup from "./NodeGroup";

/** Category colors for icon chips (from VISION.md lines 465-474) */
const CATEGORY_CHIP_COLORS: Record<
  string,
  { bg: string; border: string; icon: string; dot: string }
> = {
  ingest: { bg: "#EFF8F4", border: "#BBF7D0", icon: "#166534", dot: "#1D9E75" },
  transform: { bg: "#F5F3FF", border: "#DDD6FE", icon: "#5B21B6", dot: "#7F77DD" },
  train: { bg: "#EFF6FF", border: "#BFDBFE", icon: "#1D4ED8", dot: "#378ADD" },
  evaluate: { bg: "#FFFBEB", border: "#FDE68A", icon: "#92400E", dot: "#EF9F27" },
  export: { bg: "#FFF7ED", border: "#FED7AA", icon: "#9A3412", dot: "#D85A30" },
  demo: { bg: "#F8F9FB", border: "#E2E8F0", icon: "#64748B", dot: "#888780" },
};

const DEFAULT_CHIP_COLORS = CATEGORY_CHIP_COLORS.demo;

/** Icons per category (fallback) and per node type */
const CATEGORY_ICONS: Record<string, LucideIcon> = {
  ingest: Download,
  transform: Shuffle,
  train: Brain,
  evaluate: BarChart3,
  export: Upload,
  utility: Wrench,
  demo: Box,
};

/** More specific icons keyed by node type */
const NODE_TYPE_ICONS: Record<string, LucideIcon> = {
  file_input: FileText,
  csv_loader: FileText,
  sql_input: Database,
  clean: Zap,
  feature_eng: ArrowLeftRight,
  split: Shuffle,
  sklearn_train: CircleDot,
  xgb_train: CircleDot,
  export_table: Upload,
  export_model: Upload,
};

/** Stable category ordering */
const CATEGORY_ORDER = ["ingest", "transform", "train", "evaluate", "export", "demo"];

interface ToolbarProps {
  onAddNode: (nodeType: string) => void;
}

export default function Toolbar({ onAddNode }: ToolbarProps) {
  const { data: nodes = [] } = useQuery<NodeDefinition[]>({
    queryKey: ["nodeDefinitions"],
    queryFn: getNodeDefinitions,
    staleTime: 5 * 60 * 1000,
  });

  const grouped = useMemo(() => {
    const groups: Record<string, NodeDefinition[]> = {};
    for (const node of nodes) {
      const cat = node.category;
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(node);
    }
    return Object.entries(groups).sort(
      ([a], [b]) =>
        (CATEGORY_ORDER.indexOf(a) === -1 ? 99 : CATEGORY_ORDER.indexOf(a)) -
        (CATEGORY_ORDER.indexOf(b) === -1 ? 99 : CATEGORY_ORDER.indexOf(b)),
    );
  }, [nodes]);

  /** Merged icon map: node-type-specific icons + category fallbacks */
  const iconMap = useMemo(() => {
    const map: Record<string, LucideIcon> = { ...CATEGORY_ICONS, ...NODE_TYPE_ICONS };
    return map;
  }, []);

  return (
    <div
      data-testid="toolbar"
      style={{
        height: 46,
        minHeight: 46,
        display: "flex",
        alignItems: "center",
        gap: 0,
        paddingLeft: 16,
        paddingRight: 16,
        borderBottom: "1px solid var(--border-default)",
        backgroundColor: "var(--node-bg)",
        overflowX: "auto",
        overflowY: "hidden",
      }}
    >
      {grouped.map(([category, categoryNodes], idx) => (
        <NodeGroup
          key={category}
          category={category}
          nodes={categoryNodes}
          colors={CATEGORY_CHIP_COLORS[category] ?? DEFAULT_CHIP_COLORS}
          iconMap={iconMap}
          defaultIcon={Box}
          onAddNode={onAddNode}
          isLast={idx === grouped.length - 1}
        />
      ))}
    </div>
  );
}
