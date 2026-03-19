import { Button } from "@/components/ui/button";

interface CanvasContextMenuProps {
  x: number;
  y: number;
  onFitView: () => void;
  onClose: () => void;
}

export default function CanvasContextMenu({
  x,
  y,
  onFitView,
  onClose,
}: CanvasContextMenuProps) {
  return (
    <div
      style={{ position: "fixed", inset: 0, zIndex: 49 }}
      onClick={onClose}
    >
      <div
        className="z-50 min-w-[140px] overflow-hidden rounded-md border border-border bg-popover p-1 text-popover-foreground shadow-md"
        style={{ position: "fixed", left: x, top: y }}
      >
        <Button
          variant="ghost"
          className="w-full justify-start h-8 px-2 text-[13px] font-normal"
          onClick={() => {
            onFitView();
            onClose();
          }}
        >
          Fit view
        </Button>
      </div>
    </div>
  );
}
