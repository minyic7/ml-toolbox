import { describe, it, expect, vi, afterEach } from "vitest";
import {
  formatDuration,
  relativeTime,
  pipelineDotColor,
  dateLabel,
  groupByDate,
} from "./runConstants";
import type { GlobalRunRecord } from "./types";

/* ── Helpers ──────────────────────────────────────────────────── */

const FROZEN_NOW = new Date("2026-03-20T12:00:00Z");

function makeRun(overrides: Partial<GlobalRunRecord> & { id: string; started_at: string }): GlobalRunRecord {
  return {
    pipeline_id: "pipe-1",
    pipeline_name: "Test Pipeline",
    status: "done",
    completed_at: null,
    duration: null,
    dag_snapshot: [],
    artifacts: [],
    ...overrides,
  };
}

/* ── formatDuration ───────────────────────────────────────────── */

describe("formatDuration", () => {
  it("returns em-dash for null", () => {
    expect(formatDuration(null)).toBe("\u2014");
  });

  it("returns 0s for zero", () => {
    expect(formatDuration(0)).toBe("0s");
  });

  it("shows <1s for sub-second durations", () => {
    expect(formatDuration(0.3)).toBe("<1s");
    expect(formatDuration(0.9)).toBe("<1s");
  });

  it("returns seconds for values < 60", () => {
    expect(formatDuration(30)).toBe("30s");
  });

  it("rounds up fractional seconds", () => {
    expect(formatDuration(59.6)).toBe("60s");
  });

  it("formats minutes and seconds for 60", () => {
    expect(formatDuration(60)).toBe("1m 0s");
  });

  it("formats minutes and seconds for 90", () => {
    expect(formatDuration(90)).toBe("1m 30s");
  });

  it("formats hours and minutes for values >= 3600", () => {
    expect(formatDuration(3661)).toBe("1h 1m");
  });

  it("formats exact hour boundary", () => {
    expect(formatDuration(3600)).toBe("1h 0m");
  });

  it("formats multi-hour durations", () => {
    expect(formatDuration(7380)).toBe("2h 3m");
  });
});

/* ── relativeTime ─────────────────────────────────────────────── */

describe("relativeTime", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns "just now" for 10 seconds ago', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const iso = new Date(FROZEN_NOW.getTime() - 10_000).toISOString();
    expect(relativeTime(iso)).toBe("just now");
  });

  it('returns "5 min ago" for 5 minutes ago', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const iso = new Date(FROZEN_NOW.getTime() - 5 * 60_000).toISOString();
    expect(relativeTime(iso)).toBe("5 min ago");
  });

  it('returns "2h ago" for 2 hours ago', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const iso = new Date(FROZEN_NOW.getTime() - 2 * 3_600_000).toISOString();
    expect(relativeTime(iso)).toBe("2h ago");
  });

  it('returns "yesterday" for 1 day ago', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const iso = new Date(FROZEN_NOW.getTime() - 24 * 3_600_000).toISOString();
    expect(relativeTime(iso)).toBe("yesterday");
  });

  it('returns "3d ago" for 3 days ago', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const iso = new Date(FROZEN_NOW.getTime() - 3 * 24 * 3_600_000).toISOString();
    expect(relativeTime(iso)).toBe("3d ago");
  });
});

/* ── pipelineDotColor ─────────────────────────────────────────── */

describe("pipelineDotColor", () => {
  it("is deterministic — same ID always returns same color", () => {
    const a = pipelineDotColor("abc-123");
    const b = pipelineDotColor("abc-123");
    expect(a).toBe(b);
  });

  it("different IDs can return different colors", () => {
    const colors = new Set(
      ["id-a", "id-b", "id-c", "id-d", "id-e", "id-f", "id-g", "id-h"].map(pipelineDotColor),
    );
    expect(colors.size).toBeGreaterThan(1);
  });

  it("does not throw on empty string", () => {
    expect(() => pipelineDotColor("")).not.toThrow();
  });

  it("returns a value from PIPELINE_DOT_COLORS", () => {
    const expected = [
      "var(--category-ingest)",
      "var(--category-preprocessing)",
    ];
    expect(expected).toContain(pipelineDotColor("some-pipeline-id"));
  });
});

/* ── dateLabel ────────────────────────────────────────────────── */

describe("dateLabel", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns "Today" for today\'s date', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    expect(dateLabel(FROZEN_NOW.toISOString())).toBe("Today");
  });

  it('returns "Yesterday" for yesterday\'s date', () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const yesterday = new Date(FROZEN_NOW.getTime() - 86_400_000);
    expect(dateLabel(yesterday.toISOString())).toBe("Yesterday");
  });

  it("returns formatted date for 3 days ago", () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const threeDaysAgo = new Date(FROZEN_NOW.getTime() - 3 * 86_400_000);
    const result = dateLabel(threeDaysAgo.toISOString());
    expect(result).toBe("Mar 17, 2026");
  });

  it("handles midnight boundary correctly", () => {
    vi.useFakeTimers();
    // Set now to just after midnight
    vi.setSystemTime(new Date("2026-03-20T00:00:01Z"));
    // A timestamp from just before midnight should be "Yesterday"
    expect(dateLabel("2026-03-19T23:59:59Z")).toBe("Yesterday");
    // A timestamp from just after midnight should be "Today"
    expect(dateLabel("2026-03-20T00:00:01Z")).toBe("Today");
  });
});

/* ── groupByDate ──────────────────────────────────────────────── */

describe("groupByDate", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns empty array for empty input", () => {
    expect(groupByDate([])).toEqual([]);
  });

  it("groups all runs on the same day into one group", () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const runs = [
      makeRun({ id: "r1", started_at: "2026-03-20T10:00:00Z" }),
      makeRun({ id: "r2", started_at: "2026-03-20T11:00:00Z" }),
    ];
    const groups = groupByDate(runs);
    expect(groups).toHaveLength(1);
    expect(groups[0][0]).toBe("Today");
    expect(groups[0][1]).toHaveLength(2);
  });

  it("creates separate groups for runs across 3 days", () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const runs = [
      makeRun({ id: "r1", started_at: "2026-03-20T10:00:00Z" }),
      makeRun({ id: "r2", started_at: "2026-03-19T10:00:00Z" }),
      makeRun({ id: "r3", started_at: "2026-03-17T10:00:00Z" }),
    ];
    const groups = groupByDate(runs);
    expect(groups).toHaveLength(3);
    expect(groups[0][0]).toBe("Today");
    expect(groups[1][0]).toBe("Yesterday");
    expect(groups[2][0]).toBe("Mar 17, 2026");
  });

  it("preserves insertion order within each group", () => {
    vi.useFakeTimers();
    vi.setSystemTime(FROZEN_NOW);
    const runs = [
      makeRun({ id: "r1", started_at: "2026-03-20T08:00:00Z" }),
      makeRun({ id: "r2", started_at: "2026-03-20T09:00:00Z" }),
      makeRun({ id: "r3", started_at: "2026-03-20T10:00:00Z" }),
    ];
    const groups = groupByDate(runs);
    expect(groups[0][1].map((r) => r.id)).toEqual(["r1", "r2", "r3"]);
  });
});
