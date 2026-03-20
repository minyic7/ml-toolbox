import { Copy, Check, RotateCcw, X, Save } from "lucide-react";
import { useState, useCallback } from "react";

interface CodePaneHeaderProps {
  title: string;
  readOnly: boolean;
  unsaved: boolean;
  onCopy: () => void;
  onReset: () => void;
  onSave: () => void;
  onClose: () => void;
}

export function CodePaneHeader({
  title,
  readOnly,
  unsaved,
  onCopy,
  onReset,
  onSave,
  onClose,
}: CodePaneHeaderProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    onCopy();
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [onCopy]);

  return (
    <div
      className="flex items-center justify-between px-3 shrink-0"
      style={{
        backgroundColor: "var(--codepane-header-bg)",
        borderBottom: "1px solid var(--codepane-border)",
        height: 40,
        minHeight: 40,
      }}
    >
      {/* Title */}
      <span
        style={{
          fontFamily: "'Inter', sans-serif",
          fontWeight: 600,
          fontSize: 11,
          color: "var(--codepane-title-color)",
        }}
      >
        {title}
      </span>

      {/* Action buttons */}
      <div className="flex items-center gap-1.5">
        {/* Copy */}
        <button
          onClick={handleCopy}
          className="codepane-icon-btn"
          style={{
            width: 24,
            height: 24,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            borderRadius: 4,
            border: "1px solid var(--codepane-icon-border)",
            color: copied ? "var(--codepane-syntax-strings)" : "var(--codepane-icon-color)",
            backgroundColor: "transparent",
            cursor: "pointer",
            transition: "all 150ms ease",
          }}
          title="Copy code"
        >
          {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
        </button>

        {/* Reset */}
        {!readOnly && (
          <button
            onClick={onReset}
            className="codepane-icon-btn"
            style={{
              width: 24,
              height: 24,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              borderRadius: 4,
              border: "1px solid var(--codepane-icon-border)",
              color: "var(--codepane-icon-color)",
              backgroundColor: "transparent",
              cursor: "pointer",
              transition: "all 150ms ease",
            }}
            title="Reset to default"
          >
            <RotateCcw className="h-3.5 w-3.5" />
          </button>
        )}

        {/* Save */}
        {!readOnly && (
          <button
            onClick={onSave}
            style={{
              height: 24,
              display: "flex",
              alignItems: "center",
              gap: 4,
              borderRadius: 4,
              border: "1px solid var(--codepane-save-border)",
              backgroundColor: "var(--codepane-save-bg)",
              color: unsaved ? "var(--codepane-unsaved-amber)" : "var(--codepane-save-color)",
              cursor: "pointer",
              transition: "all 150ms ease",
              fontFamily: "'Inter', sans-serif",
              fontWeight: 700,
              fontSize: 10,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
              padding: "0 8px",
            }}
            title="Save (Cmd+S)"
          >
            <Save className="h-3 w-3" />
            Save
          </button>
        )}

        {/* Close */}
        <button
          onClick={onClose}
          className="codepane-icon-btn"
          style={{
            width: 24,
            height: 24,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            borderRadius: 4,
            border: "1px solid var(--codepane-icon-border)",
            color: "var(--codepane-icon-color)",
            backgroundColor: "transparent",
            cursor: "pointer",
            transition: "all 150ms ease",
          }}
          title="Close (Esc)"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );
}
