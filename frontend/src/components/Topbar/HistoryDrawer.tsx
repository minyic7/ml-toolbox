import { useEffect, useRef } from "react";
import type { RunInfo } from "../../lib/types";

interface HistoryDrawerProps {
  open: boolean;
  onClose: () => void;
  runs: RunInfo[];
  onDeleteRun: (runId: string) => void;
}

export default function HistoryDrawer({
  open,
  onClose,
  runs,
  onDeleteRun,
}: HistoryDrawerProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/20 z-40"
        onClick={onClose}
      />

      {/* Drawer */}
      <div
        ref={panelRef}
        className="fixed top-0 right-0 h-full w-80 z-50 shadow-lg overflow-y-auto"
        style={{ backgroundColor: "var(--node-bg)" }}
      >
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h2
              className="text-sm font-semibold"
              style={{ color: "var(--text-primary)" }}
            >
              Run History
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

          {runs.length === 0 ? (
            <p
              className="text-sm"
              style={{ color: "var(--text-muted)" }}
            >
              No runs yet.
            </p>
          ) : (
            <ul className="space-y-2">
              {runs.map((run) => (
                <li
                  key={run.id}
                  className="flex items-center justify-between p-2 rounded border text-sm"
                  style={{ borderColor: "var(--border-default)" }}
                >
                  <div>
                    <div
                      className="font-mono text-xs"
                      style={{ color: "var(--text-primary)" }}
                    >
                      {run.id.slice(0, 8)}
                    </div>
                    <div
                      className="text-xs mt-0.5"
                      style={{ color: "var(--text-secondary)" }}
                    >
                      {new Date(run.started_at).toLocaleString()}
                    </div>
                    <div
                      className="text-xs mt-0.5 capitalize"
                      style={{
                        color:
                          run.status === "error"
                            ? "var(--error-red)"
                            : run.status === "done"
                              ? "var(--success-green)"
                              : "var(--text-muted)",
                      }}
                    >
                      {run.status}
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => onDeleteRun(run.id)}
                    className="p-1 rounded hover:bg-black/5 transition-colors"
                    style={{ color: "var(--text-muted)" }}
                    title="Delete run"
                  >
                    <svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor">
                      <path d="M5.5 1a.5.5 0 00-.5.5V2H2.5a.5.5 0 000 1h.441l.58 8.7A1.5 1.5 0 005.02 13h3.96a1.5 1.5 0 001.499-1.3l.58-8.7h.441a.5.5 0 000-1H9v-.5a.5.5 0 00-.5-.5h-3zM6 2v-.5h2V2H6zm-2.058 1h6.116l-.574 8.6a.5.5 0 01-.5.4H5.016a.5.5 0 01-.5-.4L3.942 3z" />
                    </svg>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </>
  );
}
