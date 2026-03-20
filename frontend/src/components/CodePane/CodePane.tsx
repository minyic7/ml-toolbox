import { lazy, Suspense, useCallback, useEffect, useRef, useState } from "react";
import type { OnMount } from "@monaco-editor/react";
import { CodePaneHeader } from "./CodePaneHeader";
import { CodePaneFooter } from "./CodePaneFooter";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

const Editor = lazy(() =>
  import("@monaco-editor/react").then((m) => ({ default: m.default })),
);

interface CodePaneProps {
  open: boolean;
  code: string;
  defaultCode: string | undefined;
  title: string;
  onChange: (code: string) => void;
  onSave: (code: string) => void;
  lastSaveOk?: boolean;
  onClose: () => void;
}

export function CodePane({
  open,
  code,
  defaultCode,
  title,
  onChange,
  onSave,
  lastSaveOk,
  onClose,
}: CodePaneProps) {
  const readOnly = !defaultCode;
  const [unsaved, setUnsaved] = useState(false);
  const [showSaved, setShowSaved] = useState(false);
  const lastSavedRef = useRef(code);
  const savedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);
  const [resetOpen, setResetOpen] = useState(false);

  const codeRef = useRef(code);
  codeRef.current = code;

  // Show "Saved" after parent confirms save succeeded
  useEffect(() => {
    if (lastSaveOk) {
      lastSavedRef.current = codeRef.current;
      setUnsaved(false);
      setShowSaved(true);
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
      savedTimerRef.current = setTimeout(() => setShowSaved(false), 2000);
    }
  }, [lastSaveOk]);

  useEffect(() => {
    return () => {
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    };
  }, []);

  // Esc to close (only when not in dialog)
  useEffect(() => {
    if (!open) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !resetOpen) {
        const tag = (e.target as HTMLElement)?.tagName;
        if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
        e.stopPropagation();
        // Save on close via blur
        const value = editorRef.current?.getValue();
        if (value !== undefined) {
          onSave(value);
        }
        onClose();
      }
    };
    window.addEventListener("keydown", handleKey, true);
    return () => window.removeEventListener("keydown", handleKey, true);
  }, [open, resetOpen, onSave, onClose]);

  const handleCopy = useCallback(async () => {
    const value = editorRef.current?.getValue() ?? code;
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      toast.error("Failed to copy");
    }
  }, [code]);

  const handleResetConfirm = useCallback(() => {
    if (!defaultCode) return;
    onChange(defaultCode);
    onSave(defaultCode);
    setResetOpen(false);
  }, [defaultCode, onChange, onSave]);

  const handleSave = useCallback(() => {
    const value = editorRef.current?.getValue() ?? code;
    onSave(value);
  }, [code, onSave]);

  const handleEditorMount: OnMount = useCallback(
    (editor) => {
      editorRef.current = editor;
      // Cmd+S / Ctrl+S save
      editor.addCommand(
        2048 | 49, // CtrlCmd | KeyS
        () => {
          const value = editor.getValue();
          onSave(value);
        },
      );
    },
    [onSave],
  );

  // Save on blur
  const handleBlur = useCallback(() => {
    const value = editorRef.current?.getValue();
    if (value !== undefined) {
      onSave(value);
    }
  }, [onSave]);

  return (
    <div
      className="flex flex-col overflow-hidden transition-all"
      style={{
        width: open ? "var(--codepane-width)" : 0,
        minWidth: open ? "var(--codepane-width)" : 0,
        backgroundColor: "var(--codepane-bg)",
        borderLeft: open ? "1px solid var(--codepane-border)" : "none",
        transitionDuration: "250ms",
        transitionTimingFunction: "ease",
      }}
    >
      {open && (
        <>
          <CodePaneHeader
            title={title}
            readOnly={readOnly}
            unsaved={unsaved}
            onCopy={handleCopy}
            onReset={() => setResetOpen(true)}
            onSave={handleSave}
            onClose={() => {
              handleBlur();
              onClose();
            }}
          />

          {/* Editor area */}
          <div className="min-h-0 flex-1" onBlur={handleBlur}>
            <Suspense
              fallback={
                <div
                  className="flex items-center justify-center h-full"
                  style={{ color: "var(--codepane-icon-color)" }}
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
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: 11,
                  lineHeight: 1.8,
                  scrollBeyondLastLine: false,
                  automaticLayout: true,
                  padding: { top: 12 },
                  wordWrap: "on",
                }}
              />
            </Suspense>
          </div>

          <CodePaneFooter unsaved={unsaved && !showSaved} readOnly={readOnly} />

          {/* Reset confirmation dialog */}
          <Dialog open={resetOpen} onOpenChange={setResetOpen}>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Reset code to default?</DialogTitle>
                <DialogDescription>
                  This will replace your current code with the original default.
                  This action cannot be undone.
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
      )}
    </div>
  );
}
