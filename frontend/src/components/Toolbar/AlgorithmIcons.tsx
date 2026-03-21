/**
 * Custom SVG mini-icons (16×16 viewBox) for ML-Toolbox nodes.
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
