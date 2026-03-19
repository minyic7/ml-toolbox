import { lazy, Suspense, useCallback, useEffect, useRef, useState } from "react";
import type { OnMount } from "@monaco-editor/react";

const Editor = lazy(() => import("@monaco-editor/react").then((m) => ({ default: m.default })));
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
  lastSaveOk?: boolean;
}

export function CodeTab({ code, defaultCode, onChange, onSave, lastSaveOk }: CodeTabProps) {
  const readOnly = !defaultCode;
  const [copied, setCopied] = useState(false);
  const [unsaved, setUnsaved] = useState(false);
  const [showSaved, setShowSaved] = useState(false);
  const lastSavedRef = useRef(code);
  const savedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Show "✓ Saved" only after parent confirms save succeeded
  const codeRef = useRef(code);
  codeRef.current = code;

  useEffect(() => {
    if (lastSaveOk) {
      lastSavedRef.current = codeRef.current;
      setUnsaved(false);
      setShowSaved(true);
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
      savedTimerRef.current = setTimeout(() => setShowSaved(false), 2000);
    }
  }, [lastSaveOk]);

  // Cleanup timer on unmount
  useEffect(() => {
    return () => {
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    };
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
        },
      );
    },
    [onSave],
  );

  return (
    <div className="flex h-full flex-col">
      {/* Section heading */}
      <div className="px-3 pt-2 pb-1">
        <span className="text-[11px] font-medium uppercase tracking-wider" style={{ color: "var(--text-muted)" }}>
          Python
        </span>
      </div>

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

        {unsaved && (
          <span className="text-[10px]" style={{ color: "var(--warning-amber)" }}>
            ● Unsaved
          </span>
        )}
        {showSaved && (
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
          }
        }}
      >
        <Suspense
          fallback={
            <div
              className="flex items-center justify-center h-full"
              style={{ color: "var(--text-muted)" }}
            >
              Loading editor…
            </div>
          }
        >
          <Editor
            height="100%"
            language="python"
            theme="vs-dark"
            value={code}
            onChange={(v) => {
              onChange(v ?? "");
              if ((v ?? "") !== lastSavedRef.current) {
                setUnsaved(true);
                setShowSaved(false);
              } else {
                setUnsaved(false);
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
        </Suspense>
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
