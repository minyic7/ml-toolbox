/**
 * Custom SVG mini-icons (16×16 viewBox) for ML-Toolbox nodes.
 * Stroke-based, monochrome — rendered at 14px inside 28×28 chips.
 */

interface IconProps {
  color?: string;
  size?: number;
}

// ── Ingest ──────────────────────────────────────────────────────

/** Document with comma — CSV / comma-separated */
export function CsvReaderIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Page with folded corner */}
      <path
        d="M4 1.5h5.5l3 3V13.5a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1v-12A1 1 0 0 1 4 1.5z"
        stroke={color}
        strokeWidth="1.2"
        strokeLinejoin="round"
      />
      <path d="M9.5 1.5V4.5h3" stroke={color} strokeWidth="1.2" strokeLinejoin="round" />
      {/* Prominent comma symbol */}
      <text
        x="8"
        y="11.5"
        textAnchor="middle"
        fill={color}
        fontSize="8"
        fontFamily="sans-serif"
        fontWeight="bold"
      >
        ,
      </text>
    </svg>
  );
}

// ── Preprocessing ───────────────────────────────────────────────

/** Rectangle split into 3 unequal parts — train/val/test hold-out */
export function RandomHoldoutIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Outer rectangle */}
      <rect x="1" y="3" width="14" height="10" rx="1" stroke={color} strokeWidth="1.2" />
      {/* Dashed dividers at ~70% and ~85% to suggest 70/15/15 split */}
      <line x1="10" y1="3" x2="10" y2="13" stroke={color} strokeWidth="1" strokeDasharray="2 1" />
      <line x1="12.5" y1="3" x2="12.5" y2="13" stroke={color} strokeWidth="1" strokeDasharray="2 1" />
    </svg>
  );
}

// ── EDA ─────────────────────────────────────────────────────────

/** Histogram bars — distribution / profile */
export function DistributionProfileIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="2" y="9" width="2.5" height="5" rx="0.5" stroke={color} strokeWidth="1.2" />
      <rect x="5.5" y="5" width="2.5" height="9" rx="0.5" stroke={color} strokeWidth="1.2" />
      <rect x="9" y="3" width="2.5" height="11" rx="0.5" stroke={color} strokeWidth="1.2" />
      <rect x="12.5" y="7" width="2.5" height="7" rx="0.5" stroke={color} strokeWidth="1.2" />
    </svg>
  );
}

/** Grid with gaps — missing data cells */
export function MissingAnalysisIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* 3×3 grid of small squares — some filled, some dashed (missing) */}
      {/* Row 1 */}
      <rect x="1.5" y="1.5" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" />
      <rect x="6.25" y="1.5" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" strokeDasharray="1.5 1" />
      <rect x="11" y="1.5" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" />
      {/* Row 2 */}
      <rect x="1.5" y="6.25" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" strokeDasharray="1.5 1" />
      <rect x="6.25" y="6.25" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" />
      <rect x="11" y="6.25" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" strokeDasharray="1.5 1" />
      {/* Row 3 */}
      <rect x="1.5" y="11" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" />
      <rect x="6.25" y="11" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" />
      <rect x="11" y="11" width="3.5" height="3.5" rx="0.5" stroke={color} strokeWidth="1.1" strokeDasharray="1.5 1" />
    </svg>
  );
}

/** 3×3 heatmap grid — correlation matrix */
export function CorrelationMatrixIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Outer frame */}
      <rect x="1" y="1" width="14" height="14" rx="1" stroke={color} strokeWidth="1.2" />
      {/* Grid lines */}
      <line x1="5.67" y1="1" x2="5.67" y2="15" stroke={color} strokeWidth="0.8" />
      <line x1="10.33" y1="1" x2="10.33" y2="15" stroke={color} strokeWidth="0.8" />
      <line x1="1" y1="5.67" x2="15" y2="5.67" stroke={color} strokeWidth="0.8" />
      <line x1="1" y1="10.33" x2="15" y2="10.33" stroke={color} strokeWidth="0.8" />
      {/* Diagonal fill — strong correlation */}
      <rect x="1.4" y="1.4" width="3.87" height="3.87" fill={color} opacity="0.35" />
      <rect x="6.07" y="6.07" width="3.86" height="3.86" fill={color} opacity="0.35" />
      <rect x="10.73" y="10.73" width="3.87" height="3.87" fill={color} opacity="0.35" />
      {/* Off-diagonal light fill */}
      <rect x="6.07" y="1.4" width="3.86" height="3.87" fill={color} opacity="0.12" />
      <rect x="1.4" y="6.07" width="3.87" height="3.86" fill={color} opacity="0.12" />
    </svg>
  );
}

/** Scatter dots with one outlier — outlier detection */
export function OutlierDetectionIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Cluster of dots */}
      <circle cx="7" cy="9" r="1.3" fill={color} />
      <circle cx="9.5" cy="8" r="1.3" fill={color} />
      <circle cx="8" cy="11" r="1.3" fill={color} />
      <circle cx="10" cy="10.5" r="1.3" fill={color} />
      <circle cx="6" cy="10.5" r="1.3" fill={color} />
      {/* Outlier — far from cluster */}
      <circle cx="3" cy="3.5" r="1.3" fill={color} />
      {/* Dashed ring around outlier */}
      <circle cx="3" cy="3.5" r="2.8" stroke={color} strokeWidth="0.9" strokeDasharray="1.8 1.2" />
    </svg>
  );
}

// ── Transform ──────────────────────────────────────────────────

/** Table with column struck through — column dropper */
export function ColumnDropperIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Table header row */}
      <rect x="1" y="1.5" width="14" height="3" rx="0.8" stroke={color} strokeWidth="1.2" />
      {/* Column dividers in header */}
      <line x1="5.5" y1="1.5" x2="5.5" y2="4.5" stroke={color} strokeWidth="0.8" />
      <line x1="10.5" y1="1.5" x2="10.5" y2="4.5" stroke={color} strokeWidth="0.8" />
      {/* Body rows — left and right columns visible */}
      <line x1="1.5" y1="7" x2="4.5" y2="7" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <line x1="1.5" y1="9.5" x2="4.5" y2="9.5" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <line x1="1.5" y1="12" x2="4.5" y2="12" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <line x1="11.5" y1="7" x2="14.5" y2="7" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <line x1="11.5" y1="9.5" x2="14.5" y2="9.5" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <line x1="11.5" y1="12" x2="14.5" y2="12" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      {/* Diagonal strike-through on middle column */}
      <line x1="6.5" y1="5.5" x2="9.5" y2="13" stroke={color} strokeWidth="1.3" strokeLinecap="round" />
      <line x1="9.5" y1="5.5" x2="6.5" y2="13" stroke={color} strokeWidth="1.3" strokeLinecap="round" />
    </svg>
  );
}

/** Bell curve with arrows shrinking — feature scaling */
export function ScalerTransformIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Horizontal axis */}
      <line x1="1" y1="13" x2="15" y2="13" stroke={color} strokeWidth="1.2" strokeLinecap="round" />
      {/* Bell curve */}
      <path
        d="M2 13 C3 13, 4 4, 8 4 C12 4, 13 13, 14 13"
        stroke={color}
        strokeWidth="1.2"
        fill="none"
        strokeLinecap="round"
      />
      {/* Inward arrows — scaling / normalization */}
      <path d="M3 2L5.5 2" stroke={color} strokeWidth="1.2" strokeLinecap="round" />
      <path d="M3 2L4.2 1" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <path d="M3 2L4.2 3" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <path d="M13 2L10.5 2" stroke={color} strokeWidth="1.2" strokeLinecap="round" />
      <path d="M13 2L11.8 1" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
      <path d="M13 2L11.8 3" stroke={color} strokeWidth="1.1" strokeLinecap="round" />
    </svg>
  );
}

/** Function f(x) with plus sign — feature engineering / creation */
export function FeatureCreatorIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* f(x) text — feature function */}
      <text
        x="8"
        y="8.5"
        textAnchor="middle"
        fill={color}
        fontSize="6.5"
        fontFamily="serif"
        fontStyle="italic"
        fontWeight="bold"
      >
        f(x)
      </text>
      {/* Plus sign — creating new features */}
      <line x1="12" y1="11" x2="12" y2="15" stroke={color} strokeWidth="1.3" strokeLinecap="round" />
      <line x1="10" y1="13" x2="14" y2="13" stroke={color} strokeWidth="1.3" strokeLinecap="round" />
    </svg>
  );
}

/** Stacked columns with lightning bolt — Parquet columnar storage */
export function ParquetReaderIcon({ color = "currentColor", size = 16 }: IconProps) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Three columns of varying height — columnar storage */}
      <rect x="1.5" y="5" width="3" height="9.5" rx="0.5" stroke={color} strokeWidth="1.2" />
      <rect x="6.5" y="3" width="3" height="11.5" rx="0.5" stroke={color} strokeWidth="1.2" />
      <rect x="11.5" y="6.5" width="3" height="8" rx="0.5" stroke={color} strokeWidth="1.2" />
      {/* Lightning bolt — speed/compression */}
      <path
        d="M9 1L6.5 5.5h3L7 9"
        stroke={color}
        strokeWidth="1.3"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
