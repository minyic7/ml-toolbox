import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { BeforeMount, OnMount } from "@monaco-editor/react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import CodePaneHeader from "./CodePaneHeader";
import CodePaneFooter from "./CodePaneFooter";
import { CODEPANE_THEME_NAME, codepaneTheme } from "./codepaneTheme";

const Editor = lazy(() =>
  import("@monaco-editor/react").then((m) => ({ default: m.default })),
);

interface CodePaneProps {
  node: NodeInstance;
  definition: NodeDefinition;
  onSave: (nodeId: string, code: string) => void;
  onClose: () => void;
}

export default function CodePane({
  node,
  definition,
  onSave,
  onClose,
}: CodePaneProps) {
  const [localCode, setLocalCode] = useState(node.code);
  const [unsaved, setUnsaved] = useState(false);
  const lastSavedRef = useRef(node.code);
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);

  // Sync when node changes (different node selected)
  const prevNodeIdRef = useRef(node.id);
  useEffect(() => {
    if (prevNodeIdRef.current !== node.id) {
      prevNodeIdRef.current = node.id;
      setLocalCode(node.code);
      lastSavedRef.current = node.code;
      setUnsaved(false);
    }
  }, [node.id, node.code]);

  // Also sync if the server code changes externally (e.g. after save confirmation)
  useEffect(() => {
    if (node.code !== lastSavedRef.current && node.code !== localCode) {
      // Server code changed externally — update
      setLocalCode(node.code);
      lastSavedRef.current = node.code;
      setUnsaved(false);
    }
  }, [node.code, localCode]);

  const handleSave = useCallback(() => {
    const value = editorRef.current?.getValue() ?? localCode;
    onSave(node.id, value);
    lastSavedRef.current = value;
    setUnsaved(false);
  }, [localCode, node.id, onSave]);

  const handleCopy = useCallback(async () => {
    const value = editorRef.current?.getValue() ?? localCode;
    await navigator.clipboard.writeText(value);
  }, [localCode]);

  const handleReset = useCallback(() => {
    const defaultCode = definition.default_code ?? "";
    setLocalCode(defaultCode);
    onSave(node.id, defaultCode);
    lastSavedRef.current = defaultCode;
    setUnsaved(false);
  }, [definition.default_code, node.id, onSave]);

  const handleEditorMount: OnMount = useCallback(
    (editor) => {
      editorRef.current = editor;
      // Cmd+S / Ctrl+S
      editor.addCommand(
        2048 | 49, // KeyMod.CtrlCmd | KeyCode.KeyS
        () => {
          const value = editor.getValue();
          onSave(node.id, value);
          lastSavedRef.current = value;
          setUnsaved(false);
        },
      );
      // Escape → close pane
      editor.addCommand(
        9, // KeyCode.Escape
        () => {
          // Save before closing if unsaved
          const value = editor.getValue();
          if (value !== lastSavedRef.current) {
            onSave(node.id, value);
          }
          onClose();
        },
      );
    },
    [node.id, onSave, onClose],
  );

  const handleEditorChange = useCallback(
    (value: string | undefined) => {
      const v = value ?? "";
      setLocalCode(v);
      setUnsaved(v !== lastSavedRef.current);
    },
    [],
  );

  // Save on blur
  const handleBlur = useCallback(() => {
    const value = editorRef.current?.getValue();
    if (value !== undefined && value !== lastSavedRef.current) {
      onSave(node.id, value);
      lastSavedRef.current = value;
      setUnsaved(false);
    }
  }, [node.id, onSave]);

  // Cmd+S at the pane level (covers focus outside the editor)
  const paneRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = paneRef.current;
    if (!el) return;
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "s") {
        e.preventDefault();
        handleSave();
      }
    };
    el.addEventListener("keydown", handler);
    return () => el.removeEventListener("keydown", handler);
  }, [handleSave]);

  const handleBeforeMount: BeforeMount = useCallback((monaco) => {
    monaco.editor.defineTheme(CODEPANE_THEME_NAME, codepaneTheme);
  }, []);

  // ── Resizable width ──────────────────────────────────────────
  const [width, setWidth] = useState(340);
  const [dragging, setDragging] = useState(false);
  const [handleHovered, setHandleHovered] = useState(false);
  const dragStartX = useRef(0);
  const dragStartWidth = useRef(340);

  const minWidth = 300;
  const maxWidth = useMemo(() => Math.floor(window.innerWidth * 0.5), []);

  useEffect(() => {
    if (!dragging) return;
    const handleMouseMove = (e: MouseEvent) => {
      e.preventDefault();
      const delta = dragStartX.current - e.clientX;
      const newWidth = Math.min(maxWidth, Math.max(minWidth, dragStartWidth.current + delta));
      setWidth(newWidth);
    };
    const handleMouseUp = () => {
      setDragging(false);
    };
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
  }, [dragging, maxWidth]);

  const handleDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragStartX.current = e.clientX;
    dragStartWidth.current = width;
    setDragging(true);
  }, [width]);

  const displayName = node.name || definition.label || node.type;

  return (
    <div
      ref={paneRef}
      className="flex flex-col h-full"
      style={{
        width,
        minWidth: width,
        background: "var(--codepane-bg)",
        borderLeft: "1px solid var(--codepane-border)",
        position: "relative",
      }}
    >
      {/* Drag handle for resizing */}
      <div
        onMouseDown={handleDragStart}
        onMouseEnter={() => setHandleHovered(true)}
        onMouseLeave={() => setHandleHovered(false)}
        style={{
          position: "absolute",
          top: 0,
          left: 0,
          bottom: 0,
          width: 4,
          cursor: "col-resize",
          zIndex: 10,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <div
          style={{
            width: 2,
            height: 32,
            borderRadius: 1,
            background: dragging || handleHovered ? "var(--text-muted)" : "var(--border-default)",
            transition: "background 150ms",
          }}
        />
      </div>
      <CodePaneHeader
        nodeName={displayName}
        onCopy={handleCopy}
        onReset={handleReset}
        onSave={handleSave}
        onClose={onClose}
        hasDefault={!!definition.default_code}
      />

      {/* Editor content */}
      <div className="flex-1 min-h-0" onBlur={handleBlur}>
        <Suspense
          fallback={
            <div
              className="flex items-center justify-center h-full"
              style={{
                color: "var(--codepane-icon-color)",
                fontFamily: "'Inter', sans-serif",
                fontSize: 11,
              }}
            >
              Loading editor...
            </div>
          }
        >
          <Editor
            height="100%"
            language="python"
            theme={CODEPANE_THEME_NAME}
            value={localCode}
            onChange={handleEditorChange}
            beforeMount={handleBeforeMount}
            onMount={handleEditorMount}
            options={{
              minimap: { enabled: false },
              lineNumbers: "on",
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 11,
              lineHeight: 1.8 * 11,
              scrollBeyondLastLine: false,
              automaticLayout: true,
              padding: { top: 12 },
              wordWrap: "on",
              renderLineHighlight: "none",
              overviewRulerBorder: false,
              hideCursorInOverviewRuler: true,
              scrollbar: {
                verticalScrollbarSize: 6,
                horizontalScrollbarSize: 6,
              },
            }}
          />
        </Suspense>
      </div>

      <CodePaneFooter unsaved={unsaved} />
    </div>
  );
}
