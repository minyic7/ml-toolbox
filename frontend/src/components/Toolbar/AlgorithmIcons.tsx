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
