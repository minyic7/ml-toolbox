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

const STATUS_LABELS: Record<NodeStatus, string> = {
  idle: "Idle",
  dirty: "Dirty",
  pending: "Pending",
  running: "Running…",
  done: "Done",
  error: "Error",
  skipped: "Skipped",
  cached: "Cached",
};

describe("NodeCard", () => {
  describe("status states", () => {
    it.each(ALL_STATUSES)("renders %s status label", (status) => {
      renderCard({ status });
      // "error" status renders "Error" in both the status label and error strip
      if (status === "error") {
        expect(screen.getAllByText(STATUS_LABELS[status]).length).toBeGreaterThanOrEqual(1);
      } else {
        expect(screen.getByText(STATUS_LABELS[status])).toBeInTheDocument();
      }
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

  describe("accent bar color", () => {
    it("uses category accent CSS variable for known category", () => {
      const { container } = renderCard({ category: "train" });
      // Accent bar is the first absolute-positioned child div
      const accentBar = container.querySelector(
        '.node-card > div[style*="position: absolute"]',
      );
      expect(accentBar).not.toBeNull();
      expect((accentBar as HTMLElement).style.background).toBe(
        "var(--category-train)",
      );
    });

    it("falls back to --border-default for unknown category", () => {
      const { container } = renderCard({ category: "unknown" });
      const accentBar = container.querySelector(
        '.node-card > div[style*="position: absolute"]',
      );
      expect((accentBar as HTMLElement).style.background).toBe(
        "var(--border-default)",
      );
    });
  });

  describe("width", () => {
    it("renders with 232px width", () => {
      const { container } = renderCard();
      const card = container.querySelector(".node-card") as HTMLElement;
      expect(card.style.width).toBe("232px");
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

  describe("tab bar", () => {
    it("renders Params, Code, and Output tabs", () => {
      renderCard();
      expect(screen.getByText("Params")).toBeInTheDocument();
      expect(screen.getByText("Code")).toBeInTheDocument();
      expect(screen.getByText("Output")).toBeInTheDocument();
    });
  });
});
