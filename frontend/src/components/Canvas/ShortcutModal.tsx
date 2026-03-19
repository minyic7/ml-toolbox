import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

interface ShortcutModalProps {
  open: boolean;
  onClose: () => void;
}

const SHORTCUTS = [
  { keys: "Delete / Backspace", action: "Delete selected node or edge" },
  { keys: "Ctrl + A", action: "Select all nodes" },
  { keys: "Ctrl + F", action: "Fit view" },
  { keys: "?", action: "Show keyboard shortcuts" },
  { keys: "Escape", action: "Deselect / Close modal" },
];

export default function ShortcutModal({ open, onClose }: ShortcutModalProps) {
  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Keyboard shortcuts</DialogTitle>
          <DialogDescription className="sr-only">
            Available keyboard shortcuts
          </DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2">
          {SHORTCUTS.map((s) => (
            <div
              key={s.keys}
              className="flex items-center justify-between gap-4"
            >
              <span
                className="text-[13px]"
                style={{ color: "var(--text-primary)" }}
              >
                {s.action}
              </span>
              <kbd className="rounded border border-border bg-muted px-2 py-0.5 text-xs text-[var(--text-secondary)] whitespace-nowrap">
                {s.keys}
              </kbd>
            </div>
          ))}
        </div>
        <div className="flex justify-end mt-2">
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
