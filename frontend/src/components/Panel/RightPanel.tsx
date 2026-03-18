export interface RightPanelProps {
  onClose?: () => void;
}

export function RightPanel({ onClose: _onClose }: RightPanelProps) {
  return (
    <div className="flex h-full flex-col p-4 text-muted-foreground">
      <h2 className="mb-4 text-sm font-semibold text-foreground">
        Properties
      </h2>
      <p className="text-xs">Right panel placeholder</p>
    </div>
  );
}
