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

    it("renders as a pill badge with correct background", () => {
      const { container } = render(<AutoSaveIndicator status="saving" />);
      const pill = container.firstChild as HTMLElement;
      expect(pill.style.background).toBe("var(--saving-bg)");
      expect(pill.style.color).toBe("var(--saving-text)");
    });

    it("does not show Retry button", () => {
      render(<AutoSaveIndicator status="saving" />);
      expect(screen.queryByText("Retry")).not.toBeInTheDocument();
    });
  });

  describe("saved state", () => {
    it("shows 'Saved ✓' text", () => {
      render(<AutoSaveIndicator status="saved" />);
      expect(screen.getByText("Saved ✓")).toBeInTheDocument();
    });

    it("renders as a pill badge with correct background", () => {
      const { container } = render(<AutoSaveIndicator status="saved" />);
      const pill = container.firstChild as HTMLElement;
      expect(pill.style.background).toBe("var(--save-bg)");
      expect(pill.style.color).toBe("var(--save-text)");
    });

    it("does not show Retry button", () => {
      render(<AutoSaveIndicator status="saved" />);
      expect(screen.queryByText("Retry")).not.toBeInTheDocument();
    });
  });

  describe("error state", () => {
    it("shows 'Save failed' text", () => {
      render(
        <AutoSaveIndicator status="error" onRetry={vi.fn()} />,
      );
      expect(screen.getByText("Save failed")).toBeInTheDocument();
    });

    it("renders as a pill badge with error background", () => {
      const { container } = render(<AutoSaveIndicator status="error" />);
      const pill = container.firstChild as HTMLElement;
      expect(pill.style.background).toBe("var(--error-bg-light)");
      expect(pill.style.color).toBe("var(--error-red)");
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
