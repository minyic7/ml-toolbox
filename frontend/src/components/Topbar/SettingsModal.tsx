import { useEffect, useRef } from "react";
import type { PipelineSettings } from "../../lib/types";

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
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (open && !dialog.open) dialog.showModal();
    if (!open && dialog.open) dialog.close();
  }, [open]);

  if (!open) return null;

  return (
    <dialog
      ref={dialogRef}
      onClose={onClose}
      className="rounded-lg p-0 backdrop:bg-black/30 shadow-lg max-w-md w-full"
      style={{ backgroundColor: "var(--node-bg)" }}
    >
      <div className="p-5">
        <div className="flex items-center justify-between mb-4">
          <h2
            className="text-base font-semibold"
            style={{ color: "var(--text-primary)" }}
          >
            Pipeline Settings
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="p-1 rounded hover:bg-black/5 transition-colors"
            style={{ color: "var(--text-secondary)" }}
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
              <path d="M4.646 4.646a.5.5 0 01.708 0L8 7.293l2.646-2.647a.5.5 0 01.708.708L8.707 8l2.647 2.646a.5.5 0 01-.708.708L8 8.707l-2.646 2.647a.5.5 0 01-.708-.708L7.293 8 4.646 5.354a.5.5 0 010-.708z" />
            </svg>
          </button>
        </div>

        <label className="flex items-center justify-between gap-3 cursor-pointer">
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
                ? "var(--accent-blue)"
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
        </label>
      </div>
    </dialog>
  );
}
