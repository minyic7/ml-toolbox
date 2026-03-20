import type { PortType } from "./types";

/** Dot color for each port type (from VISION.md) */
export const PORT_COLORS: Record<PortType, string> = {
  TABLE: "var(--port-table)",
  MODEL: "var(--port-model)",
  METRICS: "var(--port-metrics)",
  ARRAY: "var(--port-array)",
  VALUE: "var(--port-value)",
  TENSOR: "var(--port-tensor)",
};

/** Toolbar / category badge colors */
export const CATEGORY_COLORS: Record<string, string> = {
  ingest: "var(--category-ingest)",
  transform: "var(--category-transform)",
  train: "var(--category-train)",
  evaluate: "var(--category-evaluate)",
  export: "var(--category-export)",
  demo: "var(--category-demo)",
};

/** Accent border color for each node category (4px left border on NodeCard) */
export const CATEGORY_ACCENT_COLORS: Record<string, string> = {
  ingest: "var(--category-ingest)",
  transform: "var(--category-transform)",
  train: "var(--category-train)",
  evaluate: "var(--category-evaluate)",
  export: "var(--category-export)",
  demo: "var(--category-demo)",
};
