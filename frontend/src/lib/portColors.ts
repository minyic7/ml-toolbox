import type { PortType } from "./types";

/** Dot color for each port type (from VISION.md) */
export const PORT_COLORS: Record<PortType, string> = {
  TABLE: "#9CA3AF",
  MODEL: "#10B981",
  METRICS: "#EF9F27",
  ARRAY: "#4A4558",
  VALUE: "#7F77DD",
  TENSOR: "#D85A30",
};

/** Sidebar / category badge colors */
export const CATEGORY_COLORS: Record<string, string> = {
  ingest: "#1D9E75",
  transform: "#7F77DD",
  train: "#4A4558",
  evaluate: "#EF9F27",
  export: "#D85A30",
  demo: "#94A3B8",
};

/** Accent bar color for each node category (3px top bar on NodeCard) */
export const CATEGORY_ACCENT_COLORS: Record<string, string> = {
  ingest: "var(--category-ingest)",
  transform: "var(--category-transform)",
  train: "var(--category-train)",
  evaluate: "var(--category-evaluate)",
  export: "var(--category-export)",
  demo: "var(--category-demo)",
};
