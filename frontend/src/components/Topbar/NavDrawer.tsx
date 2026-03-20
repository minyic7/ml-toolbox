import type { PipelineListItem } from "../../lib/types";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Plus } from "lucide-react";

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
  return (
    <Sheet open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <SheetContent side="left" className="w-72" aria-describedby={undefined}>
        <div className="p-4">
          <SheetHeader>
            <SheetTitle>Pipelines</SheetTitle>
          </SheetHeader>

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
      </SheetContent>
    </Sheet>
  );
}
