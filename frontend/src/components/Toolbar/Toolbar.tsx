import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Box } from "lucide-react";
import { getNodeDefinitions } from "../../lib/api";
import type { NodeDefinition } from "../../lib/types";
import type { NodeIcon } from "./NodeIconChip";
import NodeGroup from "./NodeGroup";
import {
  CsvReaderIcon,
  ExcelReaderIcon,
  ParquetReaderIcon,
  RandomHoldoutIcon,
  StratifiedHoldoutIcon,
  DistributionProfileIcon,
  MissingAnalysisIcon,
  CorrelationMatrixIcon,
  OutlierDetectionIcon,
  ColumnDropperIcon,
  ScalerTransformIcon,
  FeatureCreatorIcon,
  LogisticRegressionIcon,
  GradientBoostingIcon,
  FeatureSelectorIcon,
} from "./AlgorithmIcons";

/** Category colors for icon chips (from VISION.md lines 465-474) */
const CATEGORY_CHIP_COLORS: Record<
  string,
  { bg: string; border: string; icon: string; dot: string }
> = {
  ingest: { bg: "#EFF8F4", border: "#BBF7D0", icon: "#166534", dot: "#1D9E75" },
  preprocessing: { bg: "#FFF7ED", border: "#FDBA74", icon: "#9A3412", dot: "#F97316" },
  eda: { bg: "#F0F9FF", border: "#BAE6FD", icon: "#0369A1", dot: "#0EA5E9" },
  transform: { bg: '#F5F3FF', border: '#DDD6FE', icon: '#5B21B6', dot: '#7C3AED' },
  training: { bg: '#ECFDF5', border: '#A7F3D0', icon: '#065F46', dot: '#10B981' },
  evaluation: { bg: '#FFFBEB', border: '#FDE68A', icon: '#92400E', dot: '#F59E0B' },
};

const DEFAULT_CHIP_COLORS = { bg: "#F8F9FB", border: "#E2E8F0", icon: "#64748B", dot: "#888780" };

/** Custom SVG icon for every node type */
const NODE_TYPE_ICONS: Record<string, NodeIcon> = {
  csv_reader: CsvReaderIcon,
  excel_reader: ExcelReaderIcon,
  parquet_reader: ParquetReaderIcon,
  random_holdout: RandomHoldoutIcon,
  stratified_holdout: StratifiedHoldoutIcon,
  distribution_profile: DistributionProfileIcon,
  missing_analysis: MissingAnalysisIcon,
  correlation_matrix: CorrelationMatrixIcon,
  outlier_detection: OutlierDetectionIcon,
  column_dropper: ColumnDropperIcon,
  scaler_transform: ScalerTransformIcon,
  feature_creator: FeatureCreatorIcon,
  logistic_regression: LogisticRegressionIcon,
  gradient_boosting_train: GradientBoostingIcon,
  feature_selector: FeatureSelectorIcon,
};

/** Stable category ordering */
const CATEGORY_ORDER = ['ingest', 'preprocessing', 'eda', 'transform', 'training', 'evaluation'];

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
      const cat = node.category.toLowerCase();
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(node);
    }
    return Object.entries(groups).sort(
      ([a], [b]) =>
        (CATEGORY_ORDER.indexOf(a) === -1 ? 99 : CATEGORY_ORDER.indexOf(a)) -
        (CATEGORY_ORDER.indexOf(b) === -1 ? 99 : CATEGORY_ORDER.indexOf(b)),
    );
  }, [nodes]);

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
          iconMap={NODE_TYPE_ICONS}
          defaultIcon={Box}
          onAddNode={onAddNode}
          isLast={idx === grouped.length - 1}
        />
      ))}
    </div>
  );
}
