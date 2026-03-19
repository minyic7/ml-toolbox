import { useEffect, useRef } from "react";
import type { PipelineListItem } from "../../lib/types";

interface NavDrawerProps {
  open: boolean;
  onClose: () => void;
  pipelines: PipelineListItem[];
  currentPipelineId: string;
  onSelect: (id: string) => void;
  onCreate: () => void;
}

export default function NavDrawer({
  open,
  onClose,
  pipelines,
  currentPipelineId,
  onSelect,
  onCreate,
}: NavDrawerProps) {
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
        className="fixed top-0 left-0 h-full w-72 z-50 shadow-lg overflow-y-auto"
        style={{ backgroundColor: "var(--node-bg)" }}
      >
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h2
              className="text-sm font-semibold"
              style={{ color: "var(--text-primary)" }}
            >
              Pipelines
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

          <button
            type="button"
            onClick={onCreate}
            className="w-full mb-3 px-3 py-1.5 rounded-md text-sm font-medium text-white transition-colors"
            style={{ backgroundColor: "var(--accent-blue)" }}
          >
            + New Pipeline
          </button>

          <ul className="space-y-1">
            {pipelines.map((p) => (
              <li key={p.id}>
                <button
                  type="button"
                  onClick={() => {
                    onSelect(p.id);
                    onClose();
                  }}
                  className="w-full text-left px-3 py-2 rounded-md text-sm transition-colors"
                  style={{
                    backgroundColor:
                      p.id === currentPipelineId
                        ? "var(--accent-blue)"
                        : "transparent",
                    color:
                      p.id === currentPipelineId
                        ? "#FFFFFF"
                        : "var(--text-primary)",
                  }}
                >
                  <div className="truncate font-medium">{p.name}</div>
                  <div
                    className="text-xs mt-0.5"
                    style={{
                      color:
                        p.id === currentPipelineId
                          ? "rgba(255,255,255,0.7)"
                          : "var(--text-muted)",
                    }}
                  >
                    {p.node_count} node{p.node_count !== 1 ? "s" : ""}
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </>
  );
}
