import { test, expect } from "@playwright/test";

const NODES_FIXTURE = [
  {
    type: "generate_data", label: "Generate Data", category: "demo",
    description: "Generate sample data", inputs: [],
    outputs: [{ name: "df", type: "TABLE" }],
    params: [{ name: "rows", type: "slider", default: 100, min: 10, max: 1000, step: 10 }],
    code: 'def run(inputs, params):\n    return {"df": "out.parquet"}',
  },
];

const PIPELINE_FIXTURE = {
  id: "test-pipeline-1", name: "Test Pipeline",
  nodes: [] as Record<string, unknown>[], edges: [] as Record<string, unknown>[],
  settings: { keep_outputs: true },
};

function mockApi(page: import("@playwright/test").Page) {
  const pipelineState = { ...PIPELINE_FIXTURE, nodes: [...PIPELINE_FIXTURE.nodes], edges: [...PIPELINE_FIXTURE.edges] };
  let nodeIdCounter = 0;
  
  page.route("**/api/nodes", (route) => {
    if (route.request().method() === "GET") route.fulfill({ json: NODES_FIXTURE });
    else route.continue();
  });
  page.route("**/api/pipelines", (route) => {
    if (route.request().method() === "GET") route.fulfill({ json: [{ id: PIPELINE_FIXTURE.id, name: PIPELINE_FIXTURE.name, node_count: 0 }] });
    else route.continue();
  });
  page.route(/\/api\/pipelines\/[^/]+$/, (route) => {
    const m = route.request().method();
    if (m === "GET") route.fulfill({ json: { ...pipelineState, nodes: [...pipelineState.nodes], edges: [...pipelineState.edges] } });
    else route.fulfill({ json: {} });
  });
  page.route("**/api/pipelines/*/settings", (route) => route.fulfill({ json: { keep_outputs: true } }));
  page.route("**/api/pipelines/*/nodes", (route) => {
    if (route.request().method() === "POST") {
      const id = "node-" + (++nodeIdCounter);
      const body = route.request().postDataJSON();
      const def = NODES_FIXTURE.find((n) => n.type === body.type);
      const newNode = { id, type: body.type, position: body.position ?? { x: 250, y: 150 }, params: def?.params ?? [], code: body.code ?? "", name: def?.label ?? body.type, inputs: def?.inputs ?? [], outputs: def?.outputs ?? [] };
      pipelineState.nodes.push(newNode);
      route.fulfill({ status: 201, json: newNode });
    } else route.continue();
  });
  page.route("**/api/pipelines/*/runs", (route) => route.fulfill({ json: [] }));
  page.route("**/api/pipelines/*/status", (route) => route.fulfill({ json: { is_running: false, current_node_id: null, last_run_id: null } }));
  page.route("**/ws/**", (route) => route.abort());
}

test("debug selection", async ({ page }) => {
  page.on('console', msg => {
    if (msg.text().includes('SELECTION')) console.log('PAGE:', msg.text());
  });
  
  await mockApi(page);
  
  // Inject a spy on onSelectionChange
  await page.addInitScript(() => {
    const origAddEventListener = EventTarget.prototype.addEventListener;
    EventTarget.prototype.addEventListener = function(type: string, ...args: unknown[]) {
      return origAddEventListener.call(this, type, ...args as Parameters<typeof origAddEventListener>[1][]);
    };
  });
  
  await page.goto("/ml-toolbox/pipeline/test-pipeline-1");
  await expect(page.locator('[title="Generate Data"]')).toBeVisible({ timeout: 5000 });
  
  await page.locator('[title="Generate Data"]').click();
  await expect(page.locator(".react-flow__node")).toBeVisible({ timeout: 5000 });
  await page.waitForTimeout(500);
  
  // Check if the node is in the react-flow selection before clicking
  const beforeClick = await page.evaluate(() => {
    const nodes = document.querySelectorAll('.react-flow__node');
    return Array.from(nodes).map(n => ({ id: n.getAttribute('data-id'), selected: n.classList.contains('selected') }));
  });
  console.log("Before click:", JSON.stringify(beforeClick));
  
  // Click the node
  await page.locator(".react-flow__node").click();
  await page.waitForTimeout(1000);
  
  const afterClick = await page.evaluate(() => {
    const nodes = document.querySelectorAll('.react-flow__node');
    return Array.from(nodes).map(n => ({ id: n.getAttribute('data-id'), selected: n.classList.contains('selected') }));
  });
  console.log("After click:", JSON.stringify(afterClick));
  
  // Check what aria-selected or data attributes the node has
  const nodeAttrs = await page.evaluate(() => {
    const node = document.querySelector('.react-flow__node');
    if (!node) return null;
    return {
      id: node.getAttribute('data-id'),
      ariaSelected: node.getAttribute('aria-selected'),
      classes: node.className,
    };
  });
  console.log("Node attrs:", JSON.stringify(nodeAttrs));
  
  // Check right panel
  const rpState = await page.evaluate(() => {
    const rp = document.querySelector('.border-l.border-border');
    return {
      width: (rp as HTMLElement)?.offsetWidth,
      className: rp?.className,
      childCount: rp?.childElementCount,
    };
  });
  console.log("RightPanel:", JSON.stringify(rpState));
});
