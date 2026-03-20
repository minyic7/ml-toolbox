import { render, screen, act, fireEvent } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { ParamControl } from "./ParamControl";
import type { ParamDefinition } from "../../lib/types";

// ── Helpers ──────────────────────────────────────────────────────────

function selectParam(overrides: Partial<ParamDefinition> = {}): ParamDefinition {
  return {
    type: "select",
    name: "Activation",
    default: "relu",
    options: ["relu", "sigmoid", "tanh"],
    ...overrides,
  };
}

function sliderParam(overrides: Partial<ParamDefinition> = {}): ParamDefinition {
  return {
    type: "slider",
    name: "Learning Rate",
    default: 0.01,
    min: 0,
    max: 1,
    step: 0.01,
    ...overrides,
  };
}

function textParam(overrides: Partial<ParamDefinition> = {}): ParamDefinition {
  return {
    type: "text",
    name: "Model Name",
    default: "my-model",
    ...overrides,
  };
}

function toggleParam(overrides: Partial<ParamDefinition> = {}): ParamDefinition {
  return {
    type: "toggle",
    name: "Shuffle",
    default: false,
    ...overrides,
  };
}

// ── Tests ────────────────────────────────────────────────────────────

describe("ParamControl", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("select type", () => {
    it("renders a select trigger with label", () => {
      render(
        <ParamControl param={selectParam()} value="relu" onChange={vi.fn()} />,
      );
      expect(screen.getByText("Activation")).toBeInTheDocument();
      // Radix Select renders a trigger button
      expect(screen.getByRole("combobox")).toBeInTheDocument();
    });
  });

  describe("slider type", () => {
    it("renders a range slider with label and current value", () => {
      render(
        <ParamControl param={sliderParam()} value={0.5} onChange={vi.fn()} />,
      );
      expect(screen.getByText("Learning Rate")).toBeInTheDocument();
      const slider = screen.getByRole("slider");
      expect(slider).toBeInTheDocument();
      expect(slider).toHaveAttribute("min", "0");
      expect(slider).toHaveAttribute("max", "1");
      expect(slider).toHaveAttribute("step", "0.01");
    });

    it("displays the current numeric value", () => {
      render(
        <ParamControl param={sliderParam()} value={0.42} onChange={vi.fn()} />,
      );
      expect(screen.getByText("0.42")).toBeInTheDocument();
    });
  });

  describe("text type", () => {
    it("renders a text input with label", () => {
      render(
        <ParamControl param={textParam()} value="test" onChange={vi.fn()} />,
      );
      expect(screen.getByText("Model Name")).toBeInTheDocument();
      expect(screen.getByRole("textbox")).toHaveValue("test");
    });
  });

  describe("toggle type", () => {
    it("renders a switch with label", () => {
      render(
        <ParamControl param={toggleParam()} value={false} onChange={vi.fn()} />,
      );
      expect(screen.getByText("Shuffle")).toBeInTheDocument();
      const sw = screen.getByRole("switch");
      expect(sw).toBeInTheDocument();
      expect(sw).toHaveAttribute("aria-checked", "false");
    });

    it("calls onChange when toggled", () => {
      const onChange = vi.fn();
      render(
        <ParamControl param={toggleParam()} value={false} onChange={onChange} />,
      );
      screen.getByRole("switch").click();
      expect(onChange).toHaveBeenCalledWith("Shuffle", true);
    });
  });

  describe("debounce", () => {
    it("fires onChange after debounce delay on text blur", () => {
      const onChange = vi.fn();
      render(
        <ParamControl param={textParam()} value="old" onChange={onChange} />,
      );
      const input = screen.getByRole("textbox");
      // Simulate typing and blur using fireEvent to avoid fake-timer deadlocks
      fireEvent.change(input, { target: { value: "new-value" } });
      fireEvent.blur(input);
      // Not yet fired (debounced)
      expect(onChange).not.toHaveBeenCalled();
      // Advance past the 500ms debounce
      act(() => {
        vi.advanceTimersByTime(500);
      });
      expect(onChange).toHaveBeenCalledWith("Model Name", "new-value");
    });
  });

  describe("disabled state", () => {
    it("applies disabled opacity to select", () => {
      const { container } = render(
        <ParamControl param={selectParam()} value="relu" onChange={vi.fn()} disabled />,
      );
      const wrapper = container.firstChild as HTMLElement;
      expect(wrapper.style.opacity).toBe("0.6");
      expect(wrapper.style.pointerEvents).toBe("none");
    });

    it("applies disabled opacity to slider", () => {
      const { container } = render(
        <ParamControl param={sliderParam()} value={0.5} onChange={vi.fn()} disabled />,
      );
      const wrapper = container.firstChild as HTMLElement;
      expect(wrapper.style.opacity).toBe("0.6");
    });

    it("applies disabled opacity to text", () => {
      const { container } = render(
        <ParamControl param={textParam()} value="x" onChange={vi.fn()} disabled />,
      );
      const wrapper = container.firstChild as HTMLElement;
      expect(wrapper.style.opacity).toBe("0.6");
    });

    it("applies disabled opacity to toggle", () => {
      const { container } = render(
        <ParamControl param={toggleParam()} value={true} onChange={vi.fn()} disabled />,
      );
      const wrapper = container.firstChild as HTMLElement;
      expect(wrapper.style.opacity).toBe("0.6");
    });
  });

  describe("invalid value edge cases", () => {
    it("renders slider gracefully when value exceeds max", () => {
      render(
        <ParamControl
          param={sliderParam({ min: 0, max: 100, step: 1 })}
          value={999}
          onChange={vi.fn()}
        />,
      );
      const slider = screen.getByRole("slider");
      expect(slider).toBeInTheDocument();
      // The numeric display shows the raw value; HTML range input clamps visually
      expect(screen.getByText("999")).toBeInTheDocument();
    });

    it("renders slider gracefully when value is below min", () => {
      render(
        <ParamControl
          param={sliderParam({ min: 10, max: 100, step: 1 })}
          value={-5}
          onChange={vi.fn()}
        />,
      );
      const slider = screen.getByRole("slider");
      expect(slider).toBeInTheDocument();
      expect(screen.getByText("-5")).toBeInTheDocument();
    });

    it("renders text input with a very long string without crashing", () => {
      const longValue = "x".repeat(10_000);
      render(
        <ParamControl param={textParam()} value={longValue} onChange={vi.fn()} />,
      );
      const input = screen.getByRole("textbox") as HTMLInputElement;
      expect(input).toBeInTheDocument();
      expect(input.value).toBe(longValue);
    });

    it("renders select without crashing when value is not in options", () => {
      render(
        <ParamControl
          param={selectParam({ options: ["relu", "sigmoid", "tanh"] })}
          value="nonexistent_option"
          onChange={vi.fn()}
        />,
      );
      // Component renders with the combobox trigger present
      expect(screen.getByRole("combobox")).toBeInTheDocument();
      expect(screen.getByText("Activation")).toBeInTheDocument();
    });

    it("renders select without crashing when options list is empty", () => {
      render(
        <ParamControl
          param={selectParam({ options: [] })}
          value="relu"
          onChange={vi.fn()}
        />,
      );
      expect(screen.getByRole("combobox")).toBeInTheDocument();
    });
  });
});
