import type { PipelineSettings } from "../../lib/types";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";

interface SettingsModalProps {
  open: boolean;
  onClose: () => void;
  settings: PipelineSettings;
  onUpdate: (patch: Partial<PipelineSettings>) => void;
}

export default function SettingsModal({
  open,
  onClose,
  settings,
  onUpdate,
}: SettingsModalProps) {
  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Pipeline Settings</DialogTitle>
          <DialogDescription className="sr-only">
            Configure pipeline settings
          </DialogDescription>
        </DialogHeader>

        <Label className="flex items-center justify-between gap-3 cursor-pointer">
          <div>
            <div
              className="text-sm font-medium"
              style={{ color: "var(--text-primary)" }}
            >
              Keep outputs
            </div>
            <div
              className="text-xs mt-0.5"
              style={{ color: "var(--text-secondary)" }}
            >
              Preserve output files from previous runs
            </div>
          </div>
          <button
            type="button"
            role="switch"
            aria-checked={settings.keep_outputs}
            onClick={() => onUpdate({ keep_outputs: !settings.keep_outputs })}
            className="relative inline-flex h-5 w-9 shrink-0 rounded-full transition-colors"
            style={{
              backgroundColor: settings.keep_outputs
                ? "var(--accent-primary)"
                : "var(--border-default)",
            }}
          >
            <span
              className="inline-block h-4 w-4 rounded-full bg-white shadow-sm transition-transform mt-0.5"
              style={{
                transform: settings.keep_outputs
                  ? "translateX(18px)"
                  : "translateX(2px)",
              }}
            />
          </button>
        </Label>
      </DialogContent>
    </Dialog>
  );
}
