import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";

interface ContextMenuProps {
  x: number;
  y: number;
  nodeId: string;
  onRunFrom: (nodeId: string) => void;
  onRename: (nodeId: string) => void;
  onDelete: (nodeId: string) => void;
  onClose: () => void;
}

export default function ContextMenu({
  x,
  y,
  nodeId,
  onRunFrom,
  onRename,
  onDelete,
  onClose,
}: ContextMenuProps) {
  return (
    <div
      style={{ position: "fixed", inset: 0, zIndex: 49 }}
      onClick={onClose}
    >
      <div
        className="z-50 min-w-[160px] overflow-hidden rounded-md border border-border bg-popover p-1 text-popover-foreground shadow-md"
        style={{ position: "fixed", left: x, top: y }}
      >
        <Button
          variant="ghost"
          className="w-full justify-start h-8 px-2 text-[13px] font-normal"
          onClick={() => {
            onRunFrom(nodeId);
            onClose();
          }}
        >
          Run from here
        </Button>
        <Button
          variant="ghost"
          className="w-full justify-start h-8 px-2 text-[13px] font-normal"
          onClick={() => {
            onRename(nodeId);
            onClose();
          }}
        >
          Rename
        </Button>
        <Separator className="my-1" />
        <Button
          variant="ghost"
          className="w-full justify-start h-8 px-2 text-[13px] font-normal text-[var(--error-red)] hover:text-[var(--error-red)]"
          onClick={() => {
            onDelete(nodeId);
            onClose();
          }}
        >
          Delete node
        </Button>
      </div>
    </div>
  );
}
