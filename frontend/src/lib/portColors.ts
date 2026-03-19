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

/** Sidebar / category badge colors */
export const CATEGORY_COLORS: Record<string, string> = {
  ingest: "#378ADD",
  transform: "#639922",
  train: "#EF9F27",
  evaluate: "#7F77DD",
  export: "#D85A30",
  utility: "#9CA3AF",
};
