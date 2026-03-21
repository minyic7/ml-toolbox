import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Box } from "lucide-react";
import { getNodeDefinitions } from "../../lib/api";
import type { NodeDefinition } from "../../lib/types";
import type { NodeIcon } from "./NodeIconChip";
import NodeGroup from "./NodeGroup";
import {
  CsvReaderIcon,
  ParquetReaderIcon,
  CleanDataIcon,
  FeatureEngineeringIcon,
  TrainTestSplitIcon,
  ComputeStatsIcon,
  RandomForestClassifierIcon,
  GradientBoostingClassifierIcon,
  LogisticRegressionIcon,
  SvcClassifierIcon,
  DecisionTreeClassifierIcon,
  KnnClassifierIcon,
  LinearRegressionIcon,
  RandomForestRegressorIcon,
  GradientBoostingRegressorIcon,
  SvrIcon,
  XgboostIcon,
  ClassificationMetricsIcon,
  RegressionMetricsIcon,
  FeatureImportanceIcon,
  ExportTableIcon,
  ExportModelIcon,
  GenerateDataIcon,
  SummarizeDataIcon,
} from "./AlgorithmIcons";

/** Category colors for icon chips (from VISION.md lines 465-474) */
const CATEGORY_CHIP_COLORS: Record<
  string,
  { bg: string; border: string; icon: string; dot: string }
> = {
  ingest: { bg: "#EFF8F4", border: "#BBF7D0", icon: "#166534", dot: "#1D9E75" },
  transform: { bg: "#F5F3FF", border: "#DDD6FE", icon: "#5B21B6", dot: "#7F77DD" },
  classification: { bg: "#EFF6FF", border: "#BFDBFE", icon: "#1D4ED8", dot: "#378ADD" },
  regression: { bg: "#E0F2FE", border: "#BAE6FD", icon: "#0369A1", dot: "#0EA5E9" },
  train: { bg: "#EFF6FF", border: "#BFDBFE", icon: "#1D4ED8", dot: "#378ADD" },
  evaluate: { bg: "#FFFBEB", border: "#FDE68A", icon: "#92400E", dot: "#EF9F27" },
  export: { bg: "#FFF7ED", border: "#FED7AA", icon: "#9A3412", dot: "#D85A30" },
  demo: { bg: "#F8F9FB", border: "#E2E8F0", icon: "#64748B", dot: "#888780" },
};

const DEFAULT_CHIP_COLORS = CATEGORY_CHIP_COLORS.demo;

/** Custom SVG icon for every node type */
const NODE_TYPE_ICONS: Record<string, NodeIcon> = {
  // Ingest
  csv_reader: CsvReaderIcon,
  parquet_reader: ParquetReaderIcon,
  // Transform
  clean: CleanDataIcon,
  feature_eng: FeatureEngineeringIcon,
  split: TrainTestSplitIcon,
  compute_stats: ComputeStatsIcon,
  // Classification
  random_forest_classifier: RandomForestClassifierIcon,
  gradient_boosting_classifier: GradientBoostingClassifierIcon,
  logistic_regression: LogisticRegressionIcon,
  svc_classifier: SvcClassifierIcon,
  decision_tree_classifier: DecisionTreeClassifierIcon,
  knn_classifier: KnnClassifierIcon,
  // Regression
  linear_regression: LinearRegressionIcon,
  random_forest_regressor: RandomForestRegressorIcon,
  gradient_boosting_regressor: GradientBoostingRegressorIcon,
  svr_train: SvrIcon,
  // Train
  xgb_train: XgboostIcon,
  // Evaluate
  classification: ClassificationMetricsIcon,
  regression: RegressionMetricsIcon,
  feature_importance: FeatureImportanceIcon,
  // Export
  export_table: ExportTableIcon,
  export_model: ExportModelIcon,
  // Demo
  run: GenerateDataIcon,
  summarize_data: SummarizeDataIcon,
};

/** Stable category ordering */
const CATEGORY_ORDER = ["ingest", "transform", "classification", "regression", "train", "evaluate", "export", "demo"];

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
