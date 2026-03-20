import { test, expect } from "@playwright/test";

// ── Fixture data ────────────────────────────────────────────────

const NODES_FIXTURE = [
  {
    type: "generate_data",
    label: "Generate Data",
    category: "demo",
    description: "Generate sample data",
    inputs: [],
    outputs: [{ name: "df", type: "TABLE" }],
    params: [
      {
        name: "rows",
        type: "slider",
        default: 100,
        min: 10,
        max: 1000,
        step: 10,
      },
    ],
    code: 'def run(inputs, params):\n    return {"df": "out.parquet"}',
  },
  {
    type: "clean_data",
    label: "Clean Data",
    category: "demo",
    description: "Clean missing values",
    inputs: [{ name: "df", type: "TABLE" }],
    outputs: [{ name: "df", type: "TABLE" }],
    params: [
      {
        name: "strategy",
        type: "select",
        default: "mean",
        options: ["mean", "median", "mode", "drop"],
      },
    ],
    code: 'def run(inputs, params):\n    return {"df": "out.parquet"}',
  },
];

const PIPELINE_FIXTURE = {
  id: "test-pipeline-1",
  name: "Test Pipeline",
  nodes: [],
  edges: [],
  settings: { keep_outputs: true },
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

let nodeIdCounter = 0;

// ── Route mocks ─────────────────────────────────────────────────

function mockApi(page: import("@playwright/test").Page) {
  // Node definitions
  page.route("**/api/nodes", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({ json: NODES_FIXTURE });
    } else {
      route.continue();
    }
  });

  // Pipeline list
  page.route("**/api/pipelines", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({
        json: [
          {
            id: PIPELINE_FIXTURE.id,
            name: PIPELINE_FIXTURE.name,
            node_count: 0,
            created_at: PIPELINE_FIXTURE.created_at,
            updated_at: PIPELINE_FIXTURE.updated_at,
          },
        ],
      });
    } else if (route.request().method() === "POST") {
      route.fulfill({
        status: 201,
        json: {
          id: "new-pipeline",
          name: "New Pipeline",
          node_count: 0,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      });
    } else {
      route.continue();
    }
  });

  // Single pipeline
  page.route("**/api/pipelines/test-pipeline-1", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({ json: { ...PIPELINE_FIXTURE } });
    } else {
      route.fulfill({ json: { ...PIPELINE_FIXTURE } });
    }
  });

  page.route("**/api/pipelines/new-pipeline", (route) => {
    if (route.request().method() === "GET") {
      route.fulfill({
        json: {
          id: "new-pipeline",
          name: "New Pipeline",
          nodes: [],
          edges: [],
          settings: { keep_outputs: true },
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      });
    } else {
      route.fulfill({ json: {} });
    }
  });

  // Add node
  page.route("**/api/pipelines/*/nodes", (route) => {
    if (route.request().method() === "POST") {
      const id = `node-${++nodeIdCounter}`;
      const body = route.request().postDataJSON();
      route.fulfill({
        status: 201,
        json: {
          id,
          type: body.type,
          position: body.position ?? { x: 250, y: 150 },
          params: body.params ?? {},
          code: body.code ?? "",
          name: NODES_FIXTURE.find((n) => n.type === body.type)?.label ?? body.type,
          inputs: NODES_FIXTURE.find((n) => n.type === body.type)?.inputs ?? [],
          outputs: NODES_FIXTURE.find((n) => n.type === body.type)?.outputs ?? [],
        },
      });
    } else {
      route.continue();
    }
  });

  // Runs (empty)
  page.route("**/api/pipelines/*/runs", (route) => {
    route.fulfill({ json: [] });
  });

  // Status
  page.route("**/api/pipelines/*/status", (route) => {
    route.fulfill({
      json: { is_running: false, current_node_id: null, last_run_id: null },
    });
  });

  // WebSocket — let it fail silently (no mock needed)
  page.route("**/ws/**", (route) => route.abort());
}

// ── Tests ───────────────────────────────────────────────────────

test.describe("Canvas smoke tests", () => {
  test.beforeEach(async ({ page }) => {
    await mockApi(page);
  });

  test("home screen loads with pipeline list", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=Test Pipeline")).toBeVisible();
  });

  test("navigate to pipeline screen", async ({ page }) => {
    await page.goto(`/pipeline/${PIPELINE_FIXTURE.id}`);
    // Topbar should show pipeline name
    await expect(page.locator("text=Test Pipeline")).toBeVisible();
    // Sidebar should render with node categories
    await expect(page.locator("text=Generate Data")).toBeVisible({
      timeout: 5000,
    });
  });

  test("sidebar shows node library with categories", async ({ page }) => {
    await page.goto(`/pipeline/${PIPELINE_FIXTURE.id}`);
    await expect(page.locator("text=Generate Data")).toBeVisible({
      timeout: 5000,
    });
    await expect(page.locator("text=Clean Data")).toBeVisible();
  });

  test("click sidebar node adds it to canvas", async ({ page }) => {
    await page.goto(`/pipeline/${PIPELINE_FIXTURE.id}`);
    await expect(page.locator("text=Generate Data")).toBeVisible({
      timeout: 5000,
    });

    // Click the node in sidebar to add it
    await page.locator("text=Generate Data").click();

    // A React Flow node should appear on the canvas
    await expect(page.locator(".react-flow__node")).toBeVisible({
      timeout: 5000,
    });
  });

  test("clicking a node opens the right panel", async ({ page }) => {
    await page.goto(`/pipeline/${PIPELINE_FIXTURE.id}`);
    await expect(page.locator("text=Generate Data")).toBeVisible({
      timeout: 5000,
    });

    // Add a node
    await page.locator("text=Generate Data").click();
    await expect(page.locator(".react-flow__node")).toBeVisible({
      timeout: 5000,
    });

    // Click the node on canvas to select it
    await page.locator(".react-flow__node").click();

    // Right panel should open with tabs
    await expect(page.locator("text=Params")).toBeVisible({ timeout: 3000 });
    await expect(page.locator("text=Code")).toBeVisible();
    await expect(page.locator("text=Output")).toBeVisible();
  });

  test("run button is visible and disabled with no nodes", async ({
    page,
  }) => {
    await page.goto(`/pipeline/${PIPELINE_FIXTURE.id}`);
    const runButton = page.locator("button", { hasText: "Run All" });
    await expect(runButton).toBeVisible({ timeout: 5000 });
    await expect(runButton).toBeDisabled();
  });
});
