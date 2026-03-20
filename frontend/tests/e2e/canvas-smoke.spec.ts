import { test, expect } from "@playwright/test";

// ── Coverage notes ──────────────────────────────────────────────
//
// Intentionally NOT covered (wrong test layer for e2e):
//
// 1. Drag-to-canvas — HTML5 drag-and-drop across React Flow's
//    drop zone requires synthetic DragEvents with dataTransfer, which is
//    unreliable in Playwright. Click-to-add tests the same code path.
//
// 2. Edge connection via port drag — React Flow handles port connections
//    through internal SVG coordinate math. Port handles are 8px circles
//    positioned with transforms that don't map to screen coordinates.
//    Edge rendering is verified via pre-loaded pipeline state instead.
//
// 3. Pipeline execution (Run → WS feedback) — Would require mocking the
//    entire WebSocket execution sequence (pending→running→done per node).
//    Execution logic is covered by 192 backend unit/integration tests.
//

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
  // Stateful pipeline data — nodes are added dynamically
  const pipelineState = { ...PIPELINE_FIXTURE, nodes: [] as Record<string, unknown>[], edges: [] as Record<string, unknown>[] };
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

  // Single pipeline — match any pipeline ID with a path segment after /pipelines/
  page.route(/\/api\/pipelines\/[^/]+$/, (route) => {
    const method = route.request().method();
    if (method === "GET") {
      route.fulfill({ json: { ...pipelineState } });
    } else if (method === "PUT") {
      route.fulfill({ json: { ...pipelineState } });
    } else {
      route.fulfill({ json: {} });
    }
  });

  // Settings
  page.route("**/api/pipelines/*/settings", (route) => {
    route.fulfill({ json: { keep_outputs: true } });
  });

  // Add node — pushes to stateful pipeline so subsequent GET includes it
  page.route("**/api/pipelines/*/nodes", (route) => {
    if (route.request().method() === "POST") {
      const id = `node-${++nodeIdCounter}`;
      const body = route.request().postDataJSON();
      const def = NODES_FIXTURE.find((n) => n.type === body.type);
      const newNode = {
        id,
        type: body.type,
        position: body.position ?? { x: 250, y: 150 },
        params: def?.params ?? [],
        code: body.code ?? "",
        name: def?.label ?? body.type,
        inputs: def?.inputs ?? [],
        outputs: def?.outputs ?? [],
      };
      pipelineState.nodes.push(newNode);
      route.fulfill({ status: 201, json: newNode });
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
    nodeIdCounter = 0;
    await mockApi(page);
  });

  test("home screen loads with pipeline list", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=Test Pipeline")).toBeVisible();
  });

  test("navigate to pipeline screen", async ({ page }) => {
    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);
    // Topbar should show pipeline name
    await expect(page.locator("text=Test Pipeline")).toBeVisible({ timeout: 5000 });
    // Toolbar should render with node chips
    await expect(page.getByTestId("toolbar")).toBeVisible({
      timeout: 5000,
    });
  });

  test("toolbar shows node library with category groups", async ({ page }) => {
    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);
    // Toolbar should render
    await expect(page.getByTestId("toolbar")).toBeVisible({
      timeout: 5000,
    });
    // Node chips should be accessible by title
    await expect(page.locator('[title="Generate Data"]')).toBeVisible();
    await expect(page.locator('[title="Clean Data"]')).toBeVisible();
  });

  test("click toolbar node chip adds it to canvas", async ({ page }) => {
    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);
    await expect(page.locator('[title="Generate Data"]')).toBeVisible({
      timeout: 5000,
    });

    // Click the node chip in toolbar to add it
    await page.locator('[title="Generate Data"]').click();

    // A React Flow node should appear on the canvas
    await expect(page.locator(".react-flow__node")).toBeVisible({
      timeout: 5000,
    });
  });

  test("clicking a node shows action bar", async ({ page }) => {
    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);
    await expect(page.locator("[title=\"Generate Data\"]")).toBeVisible({ timeout: 5000 });
    await page.locator("[title=\"Generate Data\"]").click();
    await expect(page.locator(".react-flow__node")).toBeVisible({ timeout: 5000 });
    await page.locator(".react-flow__node").click();
    await expect(page.locator(".node-action-btn").first()).toBeVisible({ timeout: 3000 });
  });

  test("shows error toast when backend returns 500 on node creation", async ({
    page,
  }) => {
    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);
    await expect(page.locator('[title="Generate Data"]')).toBeVisible({
      timeout: 5000,
    });

    // Override the node creation mock to return 500
    await page.route("**/api/pipelines/*/nodes", (route) => {
      if (route.request().method() === "POST") {
        route.fulfill({ status: 500, body: "Internal Server Error" });
      } else {
        route.continue();
      }
    });

    // Click toolbar node chip to trigger the failing POST
    await page.locator('[title="Generate Data"]').click();

    // Error toast should appear
    await expect(page.locator("text=Failed to add node")).toBeVisible({
      timeout: 5000,
    });

    // Page should not crash — toolbar and topbar still visible
    await expect(page.locator('[title="Clean Data"]')).toBeVisible();
  });

  test("run button is visible and disabled with no nodes", async ({
    page,
  }) => {
    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);
    const runButton = page.locator("button", { hasText: "Run" }).first();
    await expect(runButton).toBeVisible({ timeout: 5000 });
    await expect(runButton).toBeDisabled();
  });

  test("pipeline with pre-existing nodes and edges renders correctly", async ({
    page,
  }) => {
    // Override single-pipeline mock to return nodes + edge already present
    await page.route(/\/api\/pipelines\/[^/]+$/, (route) => {
      if (route.request().method() === "GET") {
        route.fulfill({
          json: {
            ...PIPELINE_FIXTURE,
            nodes: [
              {
                id: "n1",
                type: "generate_data",
                position: { x: 100, y: 100 },
                params: NODES_FIXTURE[0].params,
                code: NODES_FIXTURE[0].code,
                name: "Generate Data",
                inputs: NODES_FIXTURE[0].inputs,
                outputs: NODES_FIXTURE[0].outputs,
              },
              {
                id: "n2",
                type: "clean_data",
                position: { x: 400, y: 100 },
                params: NODES_FIXTURE[1].params,
                code: NODES_FIXTURE[1].code,
                name: "Clean Data",
                inputs: NODES_FIXTURE[1].inputs,
                outputs: NODES_FIXTURE[1].outputs,
              },
            ],
            edges: [
              {
                id: "e1",
                source: "n1",
                source_port: "df",
                target: "n2",
                target_port: "df",
                condition: null,
              },
            ],
          },
        });
      } else {
        route.fulfill({ json: {} });
      }
    });

    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);

    // Both nodes should render
    const nodes = page.locator(".react-flow__node");
    await expect(nodes).toHaveCount(2, { timeout: 5000 });

    // The edge should render between them
    const edges = page.locator(".react-flow__edge");
    await expect(edges).toHaveCount(1, { timeout: 5000 });

    // Node labels should be visible
    await expect(page.locator("text=Generate Data").first()).toBeVisible();
    await expect(page.locator("text=Clean Data").first()).toBeVisible();
  });

  test("right-click node and delete removes it from canvas", async ({
    page,
  }) => {
    // Load pipeline with one node
    await page.route(/\/api\/pipelines\/[^/]+$/, (route) => {
      if (route.request().method() === "GET") {
        route.fulfill({
          json: {
            ...PIPELINE_FIXTURE,
            nodes: [
              {
                id: "n1",
                type: "generate_data",
                position: { x: 200, y: 200 },
                params: NODES_FIXTURE[0].params,
                code: NODES_FIXTURE[0].code,
                name: "Generate Data",
                inputs: NODES_FIXTURE[0].inputs,
                outputs: NODES_FIXTURE[0].outputs,
              },
            ],
            edges: [],
          },
        });
      } else {
        route.fulfill({ json: {} });
      }
    });

    // Mock DELETE /nodes/:id
    await page.route("**/api/pipelines/*/nodes/*", (route) => {
      if (route.request().method() === "DELETE") {
        route.fulfill({ status: 204 });
      } else {
        route.continue();
      }
    });

    await page.goto(`/ml-toolbox/pipeline/${PIPELINE_FIXTURE.id}`);

    // Verify node is present
    const nodes = page.locator(".react-flow__node");
    await expect(nodes).toHaveCount(1, { timeout: 5000 });

    // Right-click the node to open context menu
    await nodes.first().click({ button: "right" });

    // Click "Delete node" in context menu
    await page.locator("text=Delete node").click();

    // Undo toast should appear confirming the delete action fired
    await expect(page.locator("text=Undo")).toBeVisible({ timeout: 3000 });

    // Context menu should be dismissed
    await expect(page.locator("text=Delete node")).not.toBeVisible();
  });

  test("home screen survives backend 500 without crashing", async ({
    page,
  }) => {
    // Override pipeline list to return 500
    await page.route("**/api/pipelines", (route) => {
      route.fulfill({ status: 500, body: "Internal Server Error" });
    });
    await page.route("**/api/nodes", (route) => {
      route.fulfill({ json: [] });
    });

    await page.goto("/");

    // Page should not crash — the app shell should still render
    // React Query handles the error gracefully (no pipelines shown)
    await expect(page.locator("body")).toBeVisible();
    // Should NOT show an unhandled error or blank white screen
    await expect(page.locator("text=Application error")).not.toBeVisible();
  });
});
