import { useCallback, useState } from "react";
import { Copy, Check, RotateCcw, X } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

interface CodePaneHeaderProps {
  nodeName: string;
  onCopy: () => void;
  onReset: () => void;
  onSave: () => void;
  onClose: () => void;
  hasDefault: boolean;
}

export default function CodePaneHeader({
  nodeName,
  onCopy,
  onReset,
  onSave,
  onClose,
  hasDefault,
}: CodePaneHeaderProps) {
  const [copied, setCopied] = useState(false);
  const [resetOpen, setResetOpen] = useState(false);

  const handleCopy = useCallback(() => {
    onCopy();
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [onCopy]);

  const handleResetConfirm = useCallback(() => {
    onReset();
    setResetOpen(false);
  }, [onReset]);

  return (
    <>
      <div
        className="flex items-center gap-2 px-3 shrink-0"
        style={{
          height: 38,
          background: "var(--codepane-header-bg)",
          borderBottom: "1px solid var(--codepane-border)",
        }}
      >
        {/* Title */}
        <span
          style={{
            fontFamily: "'Inter', sans-serif",
            fontWeight: 600,
            fontSize: 11,
            color: "#E2E0F0",
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
            maxWidth: 160,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
        >
          {nodeName} — Code
        </span>

        <div style={{ flex: 1 }} />

        {/* Copy button */}
        <IconBtn
          onClick={handleCopy}
          title="Copy code"
        >
          {copied ? <Check size={13} style={{ color: "var(--success-green)" }} /> : <Copy size={13} />}
        </IconBtn>

        {/* Reset button */}
        {hasDefault && (
          <IconBtn
            onClick={() => setResetOpen(true)}
            title="Reset to default"
          >
            <RotateCcw size={13} />
          </IconBtn>
        )}

        {/* Save button */}
        <button
          onClick={onSave}
          title="Save (⌘S)"
          style={{
            height: 24,
            padding: "0 10px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid var(--accent-primary)",
            borderRadius: 4,
            background: "#1E1A35",
            cursor: "pointer",
            color: "#9B8FC4",
            fontFamily: "'Inter', sans-serif",
            fontWeight: 700,
            fontSize: 10,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#252040";
            e.currentTarget.style.color = "#C4B9E8";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "#1E1A35";
            e.currentTarget.style.color = "#9B8FC4";
          }}
        >
          Save
        </button>

        {/* Close button */}
        <IconBtn onClick={onClose} title="Close (Esc)">
          <X size={13} />
        </IconBtn>
      </div>

      {/* Reset confirmation dialog */}
      <Dialog open={resetOpen} onOpenChange={setResetOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Reset code to default?</DialogTitle>
            <DialogDescription>
              This will replace your current code with the original default. This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setResetOpen(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleResetConfirm}>
              Reset
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function IconBtn({
  onClick,
  title,
  children,
}: {
  onClick: () => void;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      style={{
        width: 24,
        height: 24,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        border: "1px solid #3D3650",
        borderRadius: 4,
        background: "transparent",
        cursor: "pointer",
        color: "var(--codepane-icon-color)",
        flexShrink: 0,
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = "#2D2640";
        e.currentTarget.style.borderColor = "#4D4660";
        e.currentTarget.style.color = "#E2E0F0";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = "transparent";
        e.currentTarget.style.borderColor = "#3D3650";
        e.currentTarget.style.color = "var(--codepane-icon-color)";
      }}
      onMouseDown={(e) => {
        e.currentTarget.style.transform = "scale(0.93)";
      }}
      onMouseUp={(e) => {
        e.currentTarget.style.transform = "scale(1)";
      }}
    >
      {children}
    </button>
  );
}
