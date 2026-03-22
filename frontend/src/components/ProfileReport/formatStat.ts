/**
 * Shared number formatting utilities for EDA profile report dashboards.
 */

/** Format a numeric stat value with appropriate precision. */
export function formatStat(value: unknown, decimals?: number): string {
  if (value == null) return "\u2014";
  if (typeof value === "string") return value;
  if (typeof value !== "number") return String(value);
  if (!Number.isFinite(value)) return String(value);

  if (decimals != null) return value.toFixed(decimals);

  // Large integers: use locale string (1,234,567)
  if (Number.isInteger(value)) return value.toLocaleString();

  // Very small non-zero: 4 decimal places
  if (Math.abs(value) < 0.01 && value !== 0) return value.toFixed(4);

  // Normal decimals: 2 decimal places
  return value.toFixed(2);
}

/** Format a ratio (0–1) as a percentage string. */
export function formatPct(value: unknown, decimals = 1): string {
  if (value == null) return "\u2014";
  if (typeof value !== "number") return String(value);
  return `${(value * 100).toFixed(decimals)}%`;
}

/** Resolve a nested field from an object using dot notation (e.g. "stats.mean"). */
export function resolveField(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split(".");
  let current: unknown = obj;
  for (const part of parts) {
    if (current == null || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}
