export interface CanvasProps {
  onNodeSelect?: () => void;
}

export function Canvas({ onNodeSelect: _onNodeSelect }: CanvasProps) {
  return (
    <div className="flex h-full items-center justify-center text-muted-foreground">
      Canvas placeholder
    </div>
  );
}
