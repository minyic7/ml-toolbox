import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { NodeCardData } from "../../lib/rfAdapters";
import type { NodeStatus, PortDefinition } from "../../lib/types";

// Mock @xyflow/react Handle used by PortDot
vi.mock("@xyflow/react", () => ({
  Handle: ({ id, title }: { id: string; title: string }) => (
    <div data-testid={`handle-${id}`} title={title} />
  ),
  Position: { Left: "left", Right: "right" },
}));

// Mock zustand store used by PortDot
vi.mock("../../store/executionStore", () => ({
  useExecutionStore: () => null,
}));

// Import after mocks
const { default: NodeCard } = await import("./NodeCard");

// ── Helpers ──────────────────────────────────────────────────────────

const BASE_INPUTS: PortDefinition[] = [
  { name: "data_in", type: "TABLE" },
];
const BASE_OUTPUTS: PortDefinition[] = [
  { name: "result", type: "MODEL" },
];

function makeData(overrides: Partial<NodeCardData> = {}): NodeCardData {
  return {
    label: "Test Node",
    type: "transform/clean",
    category: "transform",
    status: "idle",
    inputs: BASE_INPUTS,
    outputs: BASE_OUTPUTS,
    params: [],
    code: "",
    ...overrides,
  };
}

function renderCard(overrides: Partial<NodeCardData> = {}, selected = false) {
  const data = makeData(overrides);
  return render(
    <NodeCard
      id="node-1"
      data={data}
      selected={selected}
      type="nodeCard"
      dragging={false}
      zIndex={0}
      isConnectable={true}
      positionAbsoluteX={0}
      positionAbsoluteY={0}
      draggable
      deletable
      selectable
      parentId=""
      sourcePosition={undefined}
      targetPosition={undefined}
      dragHandle=""
    />,
  );
}

// ── Status rendering ─────────────────────────────────────────────────

const ALL_STATUSES: NodeStatus[] = [
  "idle", "dirty", "pending", "running", "done", "error", "skipped", "cached",
];

// New design: only certain statuses show a label
const STATUS_LABEL_MAP: Partial<Record<NodeStatus, string>> = {
  pending: "queued",
  running: "running",
  done: "done",
  error: "error",
  cached: "cached",
};

describe("NodeCard", () => {
  describe("status states", () => {
    it.each(ALL_STATUSES)("renders %s status without crashing", (status) => {
      const { container } = renderCard({ status });
      const card = container.querySelector(".node-card");
      expect(card).not.toBeNull();
    });

    it.each(
      Object.entries(STATUS_LABEL_MAP) as [NodeStatus, string][],
    )("shows status label for %s", (status, label) => {
      renderCard({ status });
      // "error" status renders label in both status area and error strip
      if (status === "error") {
        expect(screen.getAllByText(label, { exact: false }).length).toBeGreaterThanOrEqual(1);
      } else {
        expect(screen.getByText(label)).toBeInTheDocument();
      }
    });

    it("does not show status label for idle", () => {
      renderCard({ status: "idle" });
      expect(screen.queryByText("queued")).not.toBeInTheDocument();
      expect(screen.queryByText("running")).not.toBeInTheDocument();
      expect(screen.queryByText("done")).not.toBeInTheDocument();
    });

    it("shows error strip for error status", () => {
      renderCard({ status: "error" });
      expect(screen.getByText("Open Code")).toBeInTheDocument();
    });

    it("shows cached strip for cached status", () => {
      renderCard({ status: "cached" });
      expect(screen.getByText(/using cached output/)).toBeInTheDocument();
    });

    it("does not show error strip for non-error status", () => {
      renderCard({ status: "done" });
      expect(screen.queryByText("Open Code")).not.toBeInTheDocument();
    });

    it("shows progress shimmer only when running", () => {
      const { container } = renderCard({ status: "running" });
      const shimmer = container.querySelector('[style*="animation"]');
      expect(shimmer).not.toBeNull();
    });

    it("does not show shimmer when idle", () => {
      const { container } = renderCard({ status: "idle" });
      const shimmer = container.querySelector('[style*="shimmer"]');
      expect(shimmer).toBeNull();
    });
  });

  describe("left accent border", () => {
    it("uses category accent CSS variable for known category", () => {
      const { container } = renderCard({ category: "train" });
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.borderLeft).toContain("var(--category-train)");
    });

    it("falls back to --border-default for unknown category", () => {
      const { container } = renderCard({ category: "unknown" });
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.borderLeft).toContain("var(--border-default)");
    });
  });

  describe("width and shape", () => {
    it("renders with 210px width", () => {
      const { container } = renderCard();
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.width).toBe("210px");
    });

    it("has flat left edge rounded right border-radius", () => {
      const { container } = renderCard();
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.borderRadius).toBe("0 8px 8px 0");
    });
  });

  describe("selected state", () => {
    it("shows outline when selected", () => {
      const { container } = renderCard({}, true);
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.outline).toBe("2px solid var(--border-selected)");
      expect(card.style.outlineOffset).toBe("2px");
    });

    it("has no outline when not selected", () => {
      const { container } = renderCard({}, false);
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.outline).toBe("none");
    });
  });

  describe("port rows", () => {
    it("renders input port labels", () => {
      renderCard({ inputs: [{ name: "features", type: "TABLE" }] });
      expect(screen.getByText("features")).toBeInTheDocument();
    });

    it("renders output port labels", () => {
      renderCard({ outputs: [{ name: "predictions", type: "ARRAY" }] });
      expect(screen.getByText("predictions")).toBeInTheDocument();
    });

    it("renders type badges for ports", () => {
      renderCard({
        inputs: [{ name: "x", type: "TENSOR" }],
        outputs: [{ name: "y", type: "MODEL" }],
      });
      expect(screen.getByTitle("Port type: TENSOR")).toBeInTheDocument();
      expect(screen.getByTitle("Port type: MODEL")).toBeInTheDocument();
    });

    it("renders PortDot handles for each port", () => {
      renderCard({
        inputs: [{ name: "a", type: "TABLE" }],
        outputs: [{ name: "b", type: "VALUE" }],
      });
      expect(screen.getByTestId("handle-a")).toBeInTheDocument();
      expect(screen.getByTestId("handle-b")).toBeInTheDocument();
    });
  });

  describe("action bar", () => {
    it("renders Run, Code, and Del action buttons", () => {
      renderCard();
      expect(screen.getByText("Run")).toBeInTheDocument();
      expect(screen.getByText("Code")).toBeInTheDocument();
      expect(screen.getByText("Del")).toBeInTheDocument();
    });
  });

  describe("missing/broken data edge cases", () => {
    it("renders without crashing when label is empty", () => {
      const { container } = renderCard({ label: "" });
      const card = container.querySelector(".node-card");
      expect(card).not.toBeNull();
      expect(screen.getByText(/transform/)).toBeInTheDocument();
    });

    it("renders card without port rows when inputs and outputs are empty", () => {
      renderCard({ inputs: [], outputs: [] });
      // Action bar still present
      expect(screen.getByText("Run")).toBeInTheDocument();
      // No port handles rendered
      expect(screen.queryByTestId(/^handle-/)).not.toBeInTheDocument();
    });

    it("falls back to --border-default for empty category", () => {
      const { container } = renderCard({ category: "" });
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.borderLeft).toContain("var(--border-default)");
    });

    it("renders card structure intact with all edge-case fields combined", () => {
      const { container } = renderCard({
        label: "",
        inputs: [],
        outputs: [],
        category: "",
        status: "idle",
      });
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card).not.toBeNull();
      expect(card.style.width).toBe("210px");
    });
  });
});
