import { test, expect } from "@playwright/test";

// ── Fixture data ────────────────────────────────────────────────

const RUNS_FIXTURE = [
  {
    id: "run-today-1",
    pipeline_id: "pipe-1",
    pipeline_name: "Titanic Pipeline",
    status: "done",
    started_at: new Date().toISOString(),
    completed_at: new Date().toISOString(),
    duration: 72,
    dag_snapshot: [
      { node_id: "n1", node_name: "Generate Data", node_type: "demo.run", status: "done" },
      { node_id: "n2", node_name: "Clean Data", node_type: "transform.clean", status: "done" },
    ],
    artifacts: [
      { node_id: "n1", node_name: "Generate Data", filename: "df.parquet", type: "parquet", size: 40960, bars: null },
    ],
  },
  {
    id: "run-today-2",
    pipeline_id: "pipe-1",
    pipeline_name: "Titanic Pipeline",
    status: "error",
    started_at: new Date(Date.now() - 3600000).toISOString(),
    completed_at: new Date(Date.now() - 3500000).toISOString(),
    duration: 43,
    dag_snapshot: [
      { node_id: "n1", node_name: "Generate Data", node_type: "demo.run", status: "done" },
      { node_id: "n2", node_name: "Clean Data", node_type: "transform.clean", status: "error" },
    ],
    artifacts: [],
  },
  {
    id: "run-yesterday-1",
    pipeline_id: "pipe-2",
    pipeline_name: "Credit Scoring",
    status: "done",
    started_at: new Date(Date.now() - 86400000).toISOString(),
    completed_at: new Date(Date.now() - 86300000).toISOString(),
    duration: 231,
    dag_snapshot: [
      { node_id: "n1", node_name: "CSV Reader", node_type: "ingest.csv_reader", status: "done" },
    ],
    artifacts: [],
  },
];

const PIPELINES_FIXTURE = [
  { id: "pipe-1", name: "Titanic Pipeline", node_count: 3, created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z" },
  { id: "pipe-2", name: "Credit Scoring", node_count: 2, created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z" },
];

// ── Route mocks ─────────────────────────────────────────────────

function mockApi(
  page: import("@playwright/test").Page,
  options?: { runs?: typeof RUNS_FIXTURE },
) {
  const runs = options?.runs ?? RUNS_FIXTURE;

  page.route("**/api/runs", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({ json: runs });
    } else {
      route.continue();
    }
  });

  page.route("**/api/pipelines", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({ json: PIPELINES_FIXTURE });
    } else {
      route.continue();
    }
  });

  page.route("**/api/nodes", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({ json: [] });
    } else {
      route.continue();
    }
  });

  // WebSocket — abort silently
  page.route("**/ws/**", (route) => route.abort());
}

// ── Tests ───────────────────────────────────────────────────────

test.describe("Home screen — Runs dashboard", () => {
  test.beforeEach(async ({ page }) => {
    await mockApi(page);
  });

  test("runs dashboard loads with date-grouped entries", async ({ page }) => {
    await page.goto("/");

    // Default view is Runs — should see date headers
    await expect(page.locator("text=Today")).toBeVisible({ timeout: 5000 });
    await expect(page.getByText("Yesterday", { exact: true }).first()).toBeVisible();

    // Should see run entries (run IDs are sliced to first 8 chars in RunList)
    await expect(page.locator("text=run-toda").first()).toBeVisible();
    await expect(page.locator("text=run-yest")).toBeVisible();

    // Run count in filter row should show 3 runs
    await expect(page.locator("text=3 runs")).toBeVisible();
  });

  test("FilterRow status pill toggles filter the list", async ({ page }) => {
    await page.goto("/");

    // Wait for runs to load
    await expect(page.locator("text=3 runs")).toBeVisible({ timeout: 5000 });

    // Click the Success pill to deselect it (✓ Success is rendered as "✓ Done" → actually "✓ Success")
    // STATUS_LABELS.done = "Success", STATUS_PREFIX.done = "✓ "
    await page.locator("button", { hasText: "✓ Success" }).click();

    // Now only error + cancelled runs should be counted (1 error run)
    await expect(page.locator("text=1 run")).toBeVisible();

    // The error run should still be visible
    await expect(page.locator("text=run-toda").first()).toBeVisible();
  });

  test("clicking a run shows RunDetail", async ({ page }) => {
    await page.goto("/");

    // Wait for list to load — most recent run is auto-selected
    await expect(page.locator("text=run-toda").first()).toBeVisible({ timeout: 5000 });

    // Click the second run entry (the error run)
    // run-today-2 renders as "run-toda" (first 8 chars) — we need to click a specific row
    // The error run has "run-today-2" id, rendered as "run-toda" — click the row showing error status
    const errorRunRow = page.locator("text=run-toda").nth(1);
    await errorRunRow.click();

    // RunDetail should show the error run with Failed badge
    await expect(page.locator("text=Failed").first()).toBeVisible();

    // Run ID should appear in the detail header
    await expect(page.locator("text=run-today-2")).toBeVisible();

    // DAG thumbnail should render SVG with node rectangles
    await expect(page.locator("svg rect").first()).toBeVisible();
  });

  test("empty state when no runs exist", async ({ page }) => {
    // Override /api/runs to return empty array
    await page.route("**/api/runs", (route) => {
      if (route.request().method() === "GET") {
        route.fulfill({ json: [] });
      } else {
        route.continue();
      }
    });

    await page.goto("/");

    // Should show empty state message
    await expect(page.locator("text=No runs yet")).toBeVisible({ timeout: 5000 });

    // Run count should show 0
    await expect(page.locator("text=0 runs")).toBeVisible();
  });

  test("most recent run auto-selected on load", async ({ page }) => {
    await page.goto("/");

    // On page load, the first run (most recent) should be auto-selected
    // RunDetail should already show without clicking
    await expect(page.locator("text=Titanic Pipeline").first()).toBeVisible({ timeout: 5000 });

    // The auto-selected run's full ID should appear in the detail header
    await expect(page.locator("text=run-today-1")).toBeVisible();

    // Status badge should show Success (the first run is status: "done")
    await expect(page.locator("text=Success").first()).toBeVisible();

    // Artifacts section should show the parquet artifact
    await expect(page.locator("text=parquet").first()).toBeVisible();
  });
});
