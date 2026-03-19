import { useCallback, useEffect, useRef, useState } from "react";
import Editor, { type OnMount } from "@monaco-editor/react";
import { Copy, Check, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

interface CodeTabProps {
  code: string;
  defaultCode: string | undefined;
  onChange: (code: string) => void;
  onSave: (code: string) => void;
}

export function CodeTab({ code, defaultCode, onChange, onSave }: CodeTabProps) {
  const readOnly = !defaultCode;
  const [copied, setCopied] = useState(false);
  const [savedIndicator, setSavedIndicator] = useState<"saved" | "unsaved" | null>(null);
  const lastSavedRef = useRef(code);
  const savedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Clear saved indicator timer on unmount
  useEffect(() => {
    return () => {
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    };
  }, []);

  const showSaved = useCallback(() => {
    setSavedIndicator("saved");
    if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    savedTimerRef.current = setTimeout(() => setSavedIndicator(null), 2000);
  }, []);

  const [resetOpen, setResetOpen] = useState(false);
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);

  const handleCopy = useCallback(async () => {
    const value = editorRef.current?.getValue() ?? code;
    await navigator.clipboard.writeText(value);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  const handleResetConfirm = useCallback(() => {
    if (!defaultCode) return;
    onChange(defaultCode);
    onSave(defaultCode);
    setResetOpen(false);
  }, [defaultCode, onChange, onSave]);

  const handleEditorMount: OnMount = useCallback(
    (editor) => {
      editorRef.current = editor;

      // Cmd+S / Ctrl+S save
      editor.addCommand(
        // Monaco KeyMod.CtrlCmd | KeyCode.KeyS
        2048 | 49, // eslint-disable-line no-bitwise
        () => {
          const value = editor.getValue();
          onSave(value);
          lastSavedRef.current = value;
          showSaved();
        },
      );
    },
    [onSave],
  );

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div
        className="flex items-center gap-1.5 px-3 py-1.5"
        style={{ backgroundColor: "var(--canvas-bg)" }}
      >
        {readOnly && (
          <span
            className="rounded-sm px-1.5 py-0.5 text-[10px] font-medium uppercase"
            style={{
              backgroundColor: "var(--border-default)",
              color: "var(--text-secondary)",
            }}
          >
            read-only
          </span>
        )}

        <div className="flex-1" />

        <Button
          variant="ghost"
          size="sm"
          className="h-6 gap-1 px-2 text-[11px]"
          style={{ color: "var(--text-secondary)" }}
          onClick={handleCopy}
        >
          {copied ? (
            <>
              <Check className="h-3 w-3" style={{ color: "var(--success-green)" }} />
              <span style={{ color: "var(--success-green)" }}>Copied!</span>
            </>
          ) : (
            <>
              <Copy className="h-3 w-3" />
              Copy
            </>
          )}
        </Button>

        {!readOnly && (
          <Button
            variant="ghost"
            size="sm"
            className="h-6 gap-1 px-2 text-[11px]"
            style={{ color: "var(--text-secondary)" }}
            onClick={() => setResetOpen(true)}
          >
            <RotateCcw className="h-3 w-3" />
            Reset
          </Button>
        )}

        {savedIndicator === "unsaved" && (
          <span className="text-[10px]" style={{ color: "var(--warning-amber)" }}>
            ● Unsaved
          </span>
        )}
        {savedIndicator === "saved" && (
          <span className="text-[10px]" style={{ color: "var(--success-green)" }}>
            ✓ Saved
          </span>
        )}
      </div>

      {/* Editor */}
      <div
        className="min-h-0 flex-1"
        onBlur={() => {
          const value = editorRef.current?.getValue();
          if (value !== undefined) {
            onSave(value);
            lastSavedRef.current = value;
            showSaved();
          }
        }}
      >
        <Editor
          height="100%"
          language="python"
          theme="vs-dark"
          value={code}
          onChange={(v) => {
            onChange(v ?? "");
            if ((v ?? "") !== lastSavedRef.current) {
              setSavedIndicator("unsaved");
            } else {
              setSavedIndicator(null);
            }
          }}
          onMount={handleEditorMount}
          options={{
            readOnly,
            minimap: { enabled: false },
            lineNumbers: "on",
            fontFamily: "JetBrains Mono, monospace",
            fontSize: 13,
            scrollBeyondLastLine: false,
            automaticLayout: true,
            padding: { top: 12 },
            wordWrap: "on",
          }}
        />
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
    </div>
  );
}
