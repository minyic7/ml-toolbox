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
 * WS connections are intercepted via routeWebSocket (not aborted).
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
}

test.describe("WebSocket disconnection banner", () => {
  test("banner appears with 'Connection lost' text on disconnect", async ({ page }) => {
    await mockAPIs(page);

    // Intercept the WebSocket: accept the connection, then close it to simulate disconnect.
    await page.routeWebSocket("**/ws/**", (ws) => {
      // Connection is accepted — app enters "connected" state via onopen.
      // Wait briefly so the app processes the connected state, then close.
      setTimeout(() => ws.close(), 500);
    });

    await page.goto(`./pipeline/${PIPELINE_ID}`);

    // After WS closes, the hook sets wsStatus = "reconnecting" → amber banner shows.
    const banner = page.getByText("Connection lost. Reconnecting\u2026");
    await expect(banner).toBeVisible({ timeout: 10_000 });
  });

  test("banner shows 'Reconnected' and auto-dismisses on reconnect", async ({ page }) => {
    await mockAPIs(page);

    let connectionCount = 0;

    await page.routeWebSocket("**/ws/**", (ws) => {
      connectionCount++;
      if (connectionCount === 1) {
        // First connection: accept then close after a brief moment to trigger disconnect.
        setTimeout(() => ws.close(), 500);
      }
      // Subsequent connections: keep open — app reconnects and enters "connected" state.
    });

    await page.goto(`./pipeline/${PIPELINE_ID}`);

    // Wait for the disconnect banner first.
    const disconnectBanner = page.getByText("Connection lost. Reconnecting\u2026");
    await expect(disconnectBanner).toBeVisible({ timeout: 10_000 });

    // The hook's backoff timer will reconnect. The second routeWebSocket call
    // keeps the connection open, so onopen fires → wsStatus = "connected".
    // Green "Reconnected" banner should appear.
    const reconnectedBanner = page.getByText("Reconnected");
    await expect(reconnectedBanner).toBeVisible({ timeout: 15_000 });

    // Banner auto-dismisses after ~2 s.
    await expect(reconnectedBanner).not.toBeVisible({ timeout: 5_000 });
  });
});
