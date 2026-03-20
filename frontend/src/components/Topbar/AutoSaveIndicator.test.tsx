import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import AutoSaveIndicator from "./AutoSaveIndicator";

describe("AutoSaveIndicator", () => {
  describe("saving state", () => {
    it("shows 'Saving…' text", () => {
      render(<AutoSaveIndicator status="saving" />);
      expect(screen.getByText("Saving…")).toBeInTheDocument();
    });

    it("shows an amber pulse dot", () => {
      const { container } = render(<AutoSaveIndicator status="saving" />);
      const dot = container.querySelector(".animate-pulse");
      expect(dot).not.toBeNull();
      expect((dot as HTMLElement).style.backgroundColor).toBe(
        "var(--warning-amber)",
      );
    });

    it("does not show Retry button", () => {
      render(<AutoSaveIndicator status="saving" />);
      expect(screen.queryByText("Retry")).not.toBeInTheDocument();
    });
  });

  describe("saved state", () => {
    it("shows 'Saved' text", () => {
      render(<AutoSaveIndicator status="saved" />);
      expect(screen.getByText("Saved")).toBeInTheDocument();
    });

    it("shows a green dot", () => {
      const { container } = render(<AutoSaveIndicator status="saved" />);
      const dot = container.querySelector("span > span");
      expect((dot as HTMLElement).style.backgroundColor).toBe(
        "var(--success-green)",
      );
    });

    it("does not show Retry button", () => {
      render(<AutoSaveIndicator status="saved" />);
      expect(screen.queryByText("Retry")).not.toBeInTheDocument();
    });
  });

  describe("error state", () => {
    it("shows 'Save failed' text in red", () => {
      const { container } = render(
        <AutoSaveIndicator status="error" onRetry={vi.fn()} />,
      );
      expect(screen.getByText("Save failed")).toBeInTheDocument();
      // Root span should have error-red color
      const root = container.firstChild as HTMLElement;
      expect(root.style.color).toBe("var(--error-red)");
    });

    it("shows a red dot", () => {
      const { container } = render(<AutoSaveIndicator status="error" />);
      const dots = container.querySelectorAll("span > span");
      const redDot = dots[0] as HTMLElement;
      expect(redDot.style.backgroundColor).toBe("var(--error-red)");
    });

    it("shows Retry button when onRetry is provided", () => {
      render(<AutoSaveIndicator status="error" onRetry={vi.fn()} />);
      expect(screen.getByText("Retry")).toBeInTheDocument();
    });

    it("does not show Retry button when onRetry is not provided", () => {
      render(<AutoSaveIndicator status="error" />);
      expect(screen.queryByText("Retry")).not.toBeInTheDocument();
    });

    it("calls onRetry when Retry button is clicked", async () => {
      const user = userEvent.setup();
      const onRetry = vi.fn();
      render(<AutoSaveIndicator status="error" onRetry={onRetry} />);
      await user.click(screen.getByText("Retry"));
      expect(onRetry).toHaveBeenCalledOnce();
    });

    it("disables Retry button when retryDisabled is true", () => {
      render(
        <AutoSaveIndicator
          status="error"
          onRetry={vi.fn()}
          retryDisabled
        />,
      );
      expect(screen.getByText("Retry")).toBeDisabled();
    });
  });
});
