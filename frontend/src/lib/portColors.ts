import type { PortType } from "./types";

/** Dot color for each port type (from VISION.md) */
export const PORT_COLORS: Record<PortType, string> = {
  TABLE: "#9CA3AF",
  MODEL: "#639922",
  METRICS: "#EF9F27",
  ARRAY: "#378ADD",
  VALUE: "#7F77DD",
  TENSOR: "#D85A30",
};

/** Toolbar / category badge colors */
export const CATEGORY_COLORS: Record<string, string> = {
  ingest: "#1D9E75",
  transform: "#7F77DD",
  train: "#378ADD",
  evaluate: "#EF9F27",
  export: "#D85A30",
  demo: "#888780",
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
