/**
 * Custom SVG mini-icons (16×16 viewBox) for all 24 ML-Toolbox nodes.
 * Stroke-based, monochrome — rendered at 14px inside 28×28 chips.
 */

interface IconProps {
  color?: string;
  size?: number;
}

// ── Ingest ──────────────────────────────────────────────────────

/** Spreadsheet grid with comma symbol */
export function CsvReaderIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="2" width="12" height="12" rx="1.5" stroke={color} strokeWidth="1.5" />
      <line x1="2" y1="6" x2="14" y2="6" stroke={color} strokeWidth="1" />
      <line x1="2" y1="10" x2="14" y2="10" stroke={color} strokeWidth="1" />
      <line x1="6" y1="2" x2="6" y2="14" stroke={color} strokeWidth="1" />
      <line x1="10" y1="2" x2="10" y2="14" stroke={color} strokeWidth="1" />
    </svg>
  );
}

/** Columnar blocks (parquet = columnar format) */
export function ParquetReaderIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="2" width="3" height="12" rx="1" stroke={color} strokeWidth="1.5" />
      <rect x="6.5" y="2" width="3" height="12" rx="1" stroke={color} strokeWidth="1.5" />
      <rect x="11" y="2" width="3" height="12" rx="1" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

// ── Transform ───────────────────────────────────────────────────

/** Sparkle (cleaning metaphor) */
export function CleanDataIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M8 1.5 L9.2 5.8 L13.5 7 L9.2 8.2 L8 12.5 L6.8 8.2 L2.5 7 L6.8 5.8 Z" stroke={color} strokeWidth="1.5" strokeLinejoin="round" />
      <path d="M12 11 L12.5 12.5 L14 13 L12.5 13.5 L12 15 L11.5 13.5 L10 13 L11.5 12.5 Z" stroke={color} strokeWidth="1" strokeLinejoin="round" />
    </svg>
  );
}

/** Gear + wrench */
export function FeatureEngineeringIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="6.5" cy="6.5" r="3" stroke={color} strokeWidth="1.5" />
      <circle cx="6.5" cy="6.5" r="1" fill={color} />
      <line x1="6.5" y1="2" x2="6.5" y2="3.5" stroke={color} strokeWidth="1.5" />
      <line x1="6.5" y1="9.5" x2="6.5" y2="11" stroke={color} strokeWidth="1.5" />
      <line x1="2" y1="6.5" x2="3.5" y2="6.5" stroke={color} strokeWidth="1.5" />
      <line x1="9.5" y1="6.5" x2="11" y2="6.5" stroke={color} strokeWidth="1.5" />
      <path d="M14.5 14 L11 10.5 L12 9.5 L14.5 11.5 Z" stroke={color} strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  );
}

/** One rectangle splitting into two */
export function TrainTestSplitIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="1.5" y="5.5" width="5" height="5" rx="1" stroke={color} strokeWidth="1.5" />
      <rect x="9.5" y="1.5" width="5" height="4" rx="1" stroke={color} strokeWidth="1.5" />
      <rect x="9.5" y="8.5" width="5" height="6" rx="1" stroke={color} strokeWidth="1.5" />
      <line x1="6.5" y1="4" x2="9.5" y2="3.5" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
      <line x1="6.5" y1="10" x2="9.5" y2="11.5" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

/** Sigma (Σ) symbol */
export function ComputeStatsIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M12 3 L4 3 L8 8 L4 13 L12 13" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

// ── Classification ──────────────────────────────────────────────

/** 3 small trees side by side */
export function RandomForestClassifierIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="3" cy="5" r="2.5" stroke={color} strokeWidth="1.3" />
      <line x1="3" y1="7.5" x2="3" y2="13" stroke={color} strokeWidth="1.3" />
      <circle cx="8" cy="4" r="2.5" stroke={color} strokeWidth="1.3" />
      <line x1="8" y1="6.5" x2="8" y2="13" stroke={color} strokeWidth="1.3" />
      <circle cx="13" cy="5" r="2.5" stroke={color} strokeWidth="1.3" />
      <line x1="13" y1="7.5" x2="13" y2="13" stroke={color} strokeWidth="1.3" />
    </svg>
  );
}

/** Progressive trees (small→large) with arrow */
export function GradientBoostingClassifierIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="3" cy="8" r="1.5" stroke={color} strokeWidth="1.3" />
      <line x1="3" y1="9.5" x2="3" y2="13" stroke={color} strokeWidth="1.3" />
      <circle cx="7.5" cy="6.5" r="2" stroke={color} strokeWidth="1.3" />
      <line x1="7.5" y1="8.5" x2="7.5" y2="13" stroke={color} strokeWidth="1.3" />
      <circle cx="12.5" cy="5" r="2.5" stroke={color} strokeWidth="1.3" />
      <line x1="12.5" y1="7.5" x2="12.5" y2="13" stroke={color} strokeWidth="1.3" />
      <path d="M2 2 L14 2" stroke={color} strokeWidth="1.3" strokeLinecap="round" />
      <path d="M12 1 L14 2 L12 3" stroke={color} strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** S-shaped sigmoid curve */
export function LogisticRegressionIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M2 13 C2 13, 5 13, 7 8 C9 3, 12 3, 14 3" stroke={color} strokeWidth="1.5" strokeLinecap="round" fill="none" />
      <line x1="1" y1="8" x2="15" y2="8" stroke={color} strokeWidth="0.8" strokeDasharray="1.5 1.5" />
    </svg>
  );
}

/** Two point groups + separating line */
export function SvcClassifierIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="4" cy="4" r="1" fill={color} />
      <circle cx="3" cy="6.5" r="1" fill={color} />
      <circle cx="5.5" cy="5" r="1" fill={color} />
      <circle cx="11" cy="10" r="1" fill={color} />
      <circle cx="12.5" cy="12" r="1" fill={color} />
      <circle cx="10" cy="12.5" r="1" fill={color} />
      <line x1="1" y1="14" x2="14" y2="1" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

/** Branching tree */
export function DecisionTreeClassifierIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="8" cy="3" r="1.5" stroke={color} strokeWidth="1.3" />
      <circle cx="4" cy="9" r="1.5" stroke={color} strokeWidth="1.3" />
      <circle cx="12" cy="9" r="1.5" stroke={color} strokeWidth="1.3" />
      <circle cx="2.5" cy="14" r="1" stroke={color} strokeWidth="1.3" />
      <circle cx="5.5" cy="14" r="1" stroke={color} strokeWidth="1.3" />
      <circle cx="10.5" cy="14" r="1" stroke={color} strokeWidth="1.3" />
      <circle cx="13.5" cy="14" r="1" stroke={color} strokeWidth="1.3" />
      <line x1="8" y1="4.5" x2="4" y2="7.5" stroke={color} strokeWidth="1.3" />
      <line x1="8" y1="4.5" x2="12" y2="7.5" stroke={color} strokeWidth="1.3" />
      <line x1="4" y1="10.5" x2="2.5" y2="13" stroke={color} strokeWidth="1.3" />
      <line x1="4" y1="10.5" x2="5.5" y2="13" stroke={color} strokeWidth="1.3" />
      <line x1="12" y1="10.5" x2="10.5" y2="13" stroke={color} strokeWidth="1.3" />
      <line x1="12" y1="10.5" x2="13.5" y2="13" stroke={color} strokeWidth="1.3" />
    </svg>
  );
}

/** Center point + surrounding neighbor dots */
export function KnnClassifierIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="8" cy="8" r="1.5" fill={color} />
      <circle cx="8" cy="8" r="4" stroke={color} strokeWidth="1" strokeDasharray="2 1.5" />
      <circle cx="5" cy="5.5" r="1" fill={color} />
      <circle cx="10.5" cy="6" r="1" fill={color} />
      <circle cx="6" cy="10.5" r="1" fill={color} />
      <circle cx="11" cy="10" r="1" fill={color} />
      <circle cx="3" cy="8.5" r="1" fill={color} />
    </svg>
  );
}

// ── Regression ──────────────────────────────────────────────────

/** Scatter dots + diagonal trend line */
export function LinearRegressionIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="3" cy="12" r="1" fill={color} />
      <circle cx="5" cy="9" r="1" fill={color} />
      <circle cx="8" cy="8" r="1" fill={color} />
      <circle cx="11" cy="5" r="1" fill={color} />
      <circle cx="13" cy="3" r="1" fill={color} />
      <line x1="2" y1="13" x2="14" y2="2" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

/** 3 trees + trend line */
export function RandomForestRegressorIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="3" cy="5" r="2" stroke={color} strokeWidth="1.2" />
      <line x1="3" y1="7" x2="3" y2="10" stroke={color} strokeWidth="1.2" />
      <circle cx="8" cy="4.5" r="2" stroke={color} strokeWidth="1.2" />
      <line x1="8" y1="6.5" x2="8" y2="10" stroke={color} strokeWidth="1.2" />
      <circle cx="13" cy="5" r="2" stroke={color} strokeWidth="1.2" />
      <line x1="13" y1="7" x2="13" y2="10" stroke={color} strokeWidth="1.2" />
      <line x1="1" y1="14" x2="15" y2="11" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

/** Progressive trees + trend line */
export function GradientBoostingRegressorIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="3" cy="7" r="1.5" stroke={color} strokeWidth="1.2" />
      <line x1="3" y1="8.5" x2="3" y2="10" stroke={color} strokeWidth="1.2" />
      <circle cx="7.5" cy="5.5" r="2" stroke={color} strokeWidth="1.2" />
      <line x1="7.5" y1="7.5" x2="7.5" y2="10" stroke={color} strokeWidth="1.2" />
      <circle cx="12.5" cy="4" r="2.5" stroke={color} strokeWidth="1.2" />
      <line x1="12.5" y1="6.5" x2="12.5" y2="10" stroke={color} strokeWidth="1.2" />
      <line x1="1" y1="14" x2="15" y2="11" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

/** Scatter + regression line with margin bands */
export function SvrIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="3" cy="11" r="1" fill={color} />
      <circle cx="6" cy="9" r="1" fill={color} />
      <circle cx="9" cy="7" r="1" fill={color} />
      <circle cx="12" cy="4" r="1" fill={color} />
      <line x1="1" y1="13" x2="15" y2="2" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
      <line x1="1" y1="11" x2="15" y2="0" stroke={color} strokeWidth="0.8" strokeDasharray="1.5 1.5" />
      <line x1="1" y1="15" x2="15" y2="4" stroke={color} strokeWidth="0.8" strokeDasharray="1.5 1.5" />
    </svg>
  );
}

// ── Train ───────────────────────────────────────────────────────

/** Stacked/layered trees with boost arrow */
export function XgboostIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="5" cy="4" r="2" stroke={color} strokeWidth="1.3" />
      <line x1="5" y1="6" x2="5" y2="9" stroke={color} strokeWidth="1.3" />
      <circle cx="11" cy="4" r="2" stroke={color} strokeWidth="1.3" />
      <line x1="11" y1="6" x2="11" y2="9" stroke={color} strokeWidth="1.3" />
      <line x1="3" y1="9" x2="13" y2="9" stroke={color} strokeWidth="1" />
      <path d="M6 12 L8 14.5 L10 12" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      <line x1="8" y1="9" x2="8" y2="14.5" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

// ── Evaluate ────────────────────────────────────────────────────

/** Confusion matrix (2×2 grid, diagonal highlighted) */
export function ClassificationMetricsIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="2" width="5.5" height="5.5" rx="0.5" fill={color} opacity="0.3" stroke={color} strokeWidth="1.3" />
      <rect x="8.5" y="2" width="5.5" height="5.5" rx="0.5" stroke={color} strokeWidth="1.3" />
      <rect x="2" y="8.5" width="5.5" height="5.5" rx="0.5" stroke={color} strokeWidth="1.3" />
      <rect x="8.5" y="8.5" width="5.5" height="5.5" rx="0.5" fill={color} opacity="0.3" stroke={color} strokeWidth="1.3" />
    </svg>
  );
}

/** Residual plot (dots scattered around a line) */
export function RegressionMetricsIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <line x1="1" y1="8" x2="15" y2="8" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="3" cy="5" r="1" fill={color} />
      <circle cx="5.5" cy="10" r="1" fill={color} />
      <circle cx="7" cy="6.5" r="1" fill={color} />
      <circle cx="9.5" cy="10.5" r="1" fill={color} />
      <circle cx="11" cy="6" r="1" fill={color} />
      <circle cx="13" cy="9.5" r="1" fill={color} />
    </svg>
  );
}

/** Horizontal bar chart (descending bars) */
export function FeatureImportanceIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="2" width="12" height="2.5" rx="0.8" fill={color} opacity="0.85" />
      <rect x="2" y="5.5" width="9" height="2.5" rx="0.8" fill={color} opacity="0.6" />
      <rect x="2" y="9" width="6" height="2.5" rx="0.8" fill={color} opacity="0.4" />
      <rect x="2" y="12.5" width="3.5" height="2.5" rx="0.8" fill={color} opacity="0.25" />
    </svg>
  );
}

// ── Export ───────────────────────────────────────────────────────

/** Table/grid with download arrow */
export function ExportTableIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="1.5" y="1.5" width="9" height="8" rx="1" stroke={color} strokeWidth="1.3" />
      <line x1="1.5" y1="5" x2="10.5" y2="5" stroke={color} strokeWidth="1" />
      <line x1="5.5" y1="1.5" x2="5.5" y2="9.5" stroke={color} strokeWidth="1" />
      <line x1="11" y1="10" x2="11" y2="15" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
      <path d="M9 13 L11 15 L13 13" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** Box/cube with download arrow */
export function ExportModelIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M2 5 L7 2 L12 5 L7 8 Z" stroke={color} strokeWidth="1.3" strokeLinejoin="round" />
      <line x1="2" y1="5" x2="2" y2="10" stroke={color} strokeWidth="1.3" />
      <line x1="12" y1="5" x2="12" y2="10" stroke={color} strokeWidth="1.3" />
      <path d="M2 10 L7 13 L12 10" stroke={color} strokeWidth="1.3" strokeLinejoin="round" />
      <line x1="7" y1="8" x2="7" y2="13" stroke={color} strokeWidth="1.3" />
      <line x1="14" y1="9" x2="14" y2="14.5" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
      <path d="M12.5 13 L14 14.5 L15.5 13" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

// ── Demo ────────────────────────────────────────────────────────

/** Dice / random dots */
export function GenerateDataIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="2" width="12" height="12" rx="2" stroke={color} strokeWidth="1.5" />
      <circle cx="5" cy="5" r="1.2" fill={color} />
      <circle cx="11" cy="5" r="1.2" fill={color} />
      <circle cx="8" cy="8" r="1.2" fill={color} />
      <circle cx="5" cy="11" r="1.2" fill={color} />
      <circle cx="11" cy="11" r="1.2" fill={color} />
    </svg>
  );
}

/** Bar chart with magnifying glass */
export function SummarizeDataIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="1.5" y="8" width="2.5" height="5.5" rx="0.5" fill={color} opacity="0.5" />
      <rect x="5" y="5" width="2.5" height="8.5" rx="0.5" fill={color} opacity="0.7" />
      <rect x="8.5" y="3" width="2.5" height="10.5" rx="0.5" fill={color} opacity="0.5" />
      <circle cx="12" cy="5" r="2.5" stroke={color} strokeWidth="1.5" />
      <line x1="13.8" y1="7" x2="15" y2="8.2" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
