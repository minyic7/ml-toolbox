import { NodeLibrarySidebar } from "@/components/Canvas/Sidebar";
import { PortType, type NodeDefinition } from "@/lib/types";

// Demo node definitions — will be replaced by API-fetched definitions in T11
const DEMO_NODE_DEFINITIONS: NodeDefinition[] = [
  {
    type: "csv_reader",
    label: "CSV Reader",
    category: "IO",
    description: "Read a CSV file into a table",
    inputs: [],
    outputs: [{ name: "table", type: PortType.TABLE }],
    params: [{ name: "path", type: "text", default: "" }],
  },
  {
    type: "json_reader",
    label: "JSON Reader",
    category: "IO",
    description: "Read a JSON file",
    inputs: [],
    outputs: [{ name: "data", type: PortType.TABLE }],
    params: [{ name: "path", type: "text", default: "" }],
  },
  {
    type: "linear_regression",
    label: "Linear Regression",
    category: "Models",
    description: "Train a linear regression model",
    inputs: [{ name: "train_data", type: PortType.TABLE }],
    outputs: [
      { name: "model", type: PortType.MODEL },
      { name: "metrics", type: PortType.METRICS },
    ],
    params: [
      { name: "target_column", type: "text", default: "" },
      {
        name: "learning_rate",
        type: "slider",
        default: 0.01,
        min: 0.001,
        max: 1,
        step: 0.001,
      },
    ],
  },
  {
    type: "random_forest",
    label: "Random Forest",
    category: "Models",
    description: "Train a random forest classifier",
    inputs: [{ name: "train_data", type: PortType.TABLE }],
    outputs: [{ name: "model", type: PortType.MODEL }],
    params: [
      { name: "n_estimators", type: "slider", default: 100, min: 10, max: 500, step: 10 },
      { name: "target_column", type: "text", default: "" },
    ],
  },
  {
    type: "evaluator",
    label: "Model Evaluator",
    category: "Evaluation",
    description: "Evaluate model performance",
    inputs: [
      { name: "model", type: PortType.MODEL },
      { name: "test_data", type: PortType.TABLE },
    ],
    outputs: [{ name: "metrics", type: PortType.METRICS }],
    params: [],
  },
  {
    type: "table_join",
    label: "Table Join",
    category: "Transform",
    description: "Join two tables",
    inputs: [
      { name: "left", type: PortType.TABLE },
      { name: "right", type: PortType.TABLE },
    ],
    outputs: [{ name: "result", type: PortType.TABLE }],
    params: [
      {
        name: "join_type",
        type: "select",
        default: "inner",
        options: ["inner", "left", "right", "outer"],
      },
      { name: "on_column", type: "text", default: "" },
    ],
  },
  {
    type: "filter_rows",
    label: "Filter Rows",
    category: "Transform",
    description: "Filter table rows by condition",
    inputs: [{ name: "table", type: PortType.TABLE }],
    outputs: [{ name: "filtered", type: PortType.TABLE }],
    params: [{ name: "condition", type: "text", default: "" }],
  },
];

export function Sidebar() {
  return <NodeLibrarySidebar definitions={DEMO_NODE_DEFINITIONS} />;
}
