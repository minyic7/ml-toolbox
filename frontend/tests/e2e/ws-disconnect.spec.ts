import { test, expect } from "@playwright/test";

const PIPELINE_ID = "test-pipeline-1";

/** Minimal pipeline data so PipelineScreen renders without errors. */
const PIPELINE = {
  id: PIPELINE_ID,
  name: "Test Pipeline",
  nodes: [],
  edges: [],
  settings: { viewport: { x: 0, y: 0, zoom: 1 } },
};

/**
 * Install API mocks so the pipeline page loads without a real backend.
 * WS connections are aborted so the socket hook immediately enters
 * "reconnecting" state (onerror → onclose → setWsStatus("reconnecting")).
 */
async function mockAPIs(page: import("@playwright/test").Page) {
  await page.route("**/api/pipelines/" + PIPELINE_ID, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(PIPELINE) }),
  );
  await page.route("**/api/pipelines/" + PIPELINE_ID + "/status", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ is_running: false, current_node_id: null, last_run_id: null }),
    }),
  );
  await page.route("**/api/pipelines/" + PIPELINE_ID + "/runs", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([]) }),
  );
  await page.route("**/api/nodes", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([]) }),
  );
  await page.route("**/api/health", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "ok" }) }),
  );
  // Abort WS so the socket hook fires onerror → onclose immediately.
  await page.route("**/ws/**", (route) => route.abort());
}

/** Helper: set wsStatus on the Zustand execution store via the dev-mode window bridge. */
async function setWsStatus(page: import("@playwright/test").Page, status: string) {
  await page.evaluate((s) => {
    const store = (window as Record<string, unknown>).__EXECUTION_STORE__ as {
      getState: () => { setWsStatus: (v: string) => void };
    };
    store.getState().setWsStatus(s);
  }, status);
}

test.describe("WebSocket disconnection banner", () => {
  test("banner appears with 'Connection lost' text on disconnect", async ({ page }) => {
    await mockAPIs(page);
    await page.goto(`./pipeline/${PIPELINE_ID}`);

    // WS is aborted → hook sets wsStatus = "reconnecting" → amber banner shows.
    const banner = page.getByText("Connection lost. Reconnecting\u2026");
    await expect(banner).toBeVisible({ timeout: 10_000 });
  });

  test("banner shows 'Reconnected' and auto-dismisses on reconnect", async ({ page }) => {
    await mockAPIs(page);
    await page.goto(`./pipeline/${PIPELINE_ID}`);

    // Wait for the disconnect banner first.
    const disconnectBanner = page.getByText("Connection lost. Reconnecting\u2026");
    await expect(disconnectBanner).toBeVisible({ timeout: 10_000 });

    // Simulate reconnection via the Zustand store.
    await setWsStatus(page, "connected");

    // Green "Reconnected" banner should appear.
    const reconnectedBanner = page.getByText("Reconnected");
    await expect(reconnectedBanner).toBeVisible({ timeout: 5_000 });

    // Banner auto-dismisses after ~2 s.
    await expect(reconnectedBanner).not.toBeVisible({ timeout: 5_000 });
  });
});
