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

export type CodePaneTab = "params" | "code" | "output";

const TABS: { key: CodePaneTab; label: string; icon: string }[] = [
  { key: "params", label: "Params", icon: "▤" },
  { key: "code", label: "Code", icon: "</>" },
  { key: "output", label: "Output", icon: "▦" },
];

interface CodePaneHeaderProps {
  nodeName: string;
  onCopy: () => void;
  onReset: () => void;
  onSave: () => void;
  onClose: () => void;
  hasDefault: boolean;
  activeTab: CodePaneTab;
  onTabChange: (tab: CodePaneTab) => void;
}

export default function CodePaneHeader({
  nodeName,
  onCopy,
  onReset,
  onSave,
  onClose,
  hasDefault,
  activeTab,
  onTabChange,
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
            color: "var(--codepane-title-color)",
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
            background: "var(--codepane-save-bg)",
            cursor: "pointer",
            color: "var(--codepane-save-color)",
            fontFamily: "'Inter', sans-serif",
            fontWeight: 700,
            fontSize: 10,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "var(--codepane-save-hover-bg)";
            e.currentTarget.style.color = "var(--codepane-save-hover-color)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "var(--codepane-save-bg)";
            e.currentTarget.style.color = "var(--codepane-save-color)";
          }}
        >
          Save
        </button>

        {/* Close button */}
        <IconBtn onClick={onClose} title="Close (Esc)">
          <X size={13} />
        </IconBtn>
      </div>

      {/* Tab bar */}
      <div
        className="flex items-center gap-1 px-3 shrink-0"
        style={{
          height: 32,
          background: "var(--codepane-header-bg)",
          borderBottom: "1px solid var(--codepane-border)",
        }}
      >
        {TABS.map((tab) => {
          const isActive = activeTab === tab.key;
          return (
            <button
              key={tab.key}
              onClick={() => onTabChange(tab.key)}
              style={{
                fontFamily: "'Inter', sans-serif",
                fontWeight: 700,
                fontSize: 10,
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                color: isActive ? "#E2E0F0" : "#4D4660",
                background: "none",
                border: "none",
                borderBottomStyle: "solid",
                borderBottomWidth: 2,
                borderBottomColor: isActive ? "#4A4558" : "transparent",
                padding: "4px 8px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 4,
                transition: "color 150ms",
              }}
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.color = "#7C7A8E";
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.color = "#4D4660";
                }
              }}
            >
              <span style={{ fontSize: 11 }}>{tab.icon}</span>
              {tab.label}
            </button>
          );
        })}
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
        border: "1px solid var(--codepane-btn-border)",
        borderRadius: 4,
        background: "transparent",
        cursor: "pointer",
        color: "var(--codepane-btn-color)",
        flexShrink: 0,
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = "var(--codepane-btn-hover-bg)";
        e.currentTarget.style.borderColor = "var(--codepane-btn-hover-border)";
        e.currentTarget.style.color = "var(--codepane-btn-hover-color)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = "transparent";
        e.currentTarget.style.borderColor = "var(--codepane-btn-border)";
        e.currentTarget.style.color = "var(--codepane-btn-color)";
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
