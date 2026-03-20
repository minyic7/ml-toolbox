import { useEffect, useRef } from "react";
import type { PipelineListItem } from "../../lib/types";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { X, Plus } from "lucide-react";

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
        className="fixed top-0 left-0 h-full w-72 z-50 shadow-lg overflow-y-auto bg-background"
        style={{ animation: "fadeIn 0.15s ease-out" }}
      >
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h2
              className="text-sm font-semibold"
              style={{ color: "var(--text-primary)" }}
            >
              Pipelines
            </h2>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-[var(--text-secondary)]"
              aria-label="Close drawer"
              onClick={onClose}
            >
              <X className="h-4 w-4" />
            </Button>
          </div>

          <Button
            className="w-full mb-3"
            onClick={onCreate}
          >
            <Plus className="h-4 w-4" />
            New Pipeline
          </Button>

          <Separator className="mb-3" />

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
