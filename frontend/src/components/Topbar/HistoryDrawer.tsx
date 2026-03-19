import { useEffect, useRef } from "react";
import type { RunInfo } from "../../lib/types";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { X, Trash2, Eye } from "lucide-react";

interface HistoryDrawerProps {
  open: boolean;
  onClose: () => void;
  runs: RunInfo[];
  onDeleteRun: (runId: string) => void;
  onViewRun?: (runId: string) => void;
}

export default function HistoryDrawer({
  open,
  onClose,
  runs,
  onDeleteRun,
  onViewRun,
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
        className="fixed top-0 right-0 h-full w-80 z-50 shadow-lg overflow-y-auto bg-background"
      >
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h2
              className="text-sm font-semibold"
              style={{ color: "var(--text-primary)" }}
            >
              Run History
            </h2>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-[var(--text-secondary)]"
              onClick={onClose}
            >
              <X className="h-4 w-4" />
            </Button>
          </div>

          <Separator className="mb-4" />

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
                  <div className="flex items-center gap-1">
                    {onViewRun && (
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7 text-[var(--text-muted)]"
                        onClick={() => {
                          onViewRun(run.id);
                          onClose();
                        }}
                        title="View run output"
                      >
                        <Eye className="h-3.5 w-3.5" />
                      </Button>
                    )}
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-[var(--text-muted)]"
                      onClick={() => onDeleteRun(run.id)}
                      title="Delete run"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </li>
              ))}
            </ul>
          )}

          {runs.length > 0 && (
            <>
              <Separator className="my-4" />
              <Button
                variant="ghost"
                size="sm"
                className="w-full text-xs"
                style={{ color: "var(--text-muted)" }}
                onClick={() => {
                  const completed = runs.filter(
                    (r) => r.status === "done" || r.status === "error",
                  );
                  for (const r of completed) {
                    onDeleteRun(r.id);
                  }
                }}
              >
                Clear completed
              </Button>
            </>
          )}
        </div>
      </div>
    </>
  );
}
