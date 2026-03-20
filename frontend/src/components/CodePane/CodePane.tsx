import { lazy, Suspense, useCallback, useEffect, useRef, useState } from "react";
import type { BeforeMount, OnMount } from "@monaco-editor/react";
import type { NodeInstance, NodeDefinition } from "../../lib/types";
import CodePaneHeader from "./CodePaneHeader";
import type { CodePaneTab } from "./CodePaneHeader";
import CodePaneFooter from "./CodePaneFooter";
import { CODEPANE_THEME_NAME, codepaneTheme } from "./codepaneTheme";
import { ParamsTab } from "../Drawer/ParamsTab";
import { OutputTab } from "../Drawer/OutputTab";

const Editor = lazy(() =>
  import("@monaco-editor/react").then((m) => ({ default: m.default })),
);

interface CodePaneProps {
  node: NodeInstance;
  definition: NodeDefinition;
  onSave: (nodeId: string, code: string) => void;
  onClose: () => void;
  pipelineId: string;
  onParamChange: (nodeId: string, name: string, value: unknown) => void;
  paramSaving?: boolean;
  onRunFrom?: (nodeId: string) => void;
}

export default function CodePane({
  node,
  definition,
  onSave,
  onClose,
  pipelineId,
  onParamChange,
  paramSaving,
  onRunFrom,
}: CodePaneProps) {
  const [activeTab, setActiveTab] = useState<CodePaneTab>("code");
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

  const displayName = node.name || definition.label || node.type;

  return (
    <div
      ref={paneRef}
      className="flex flex-col h-full"
      style={{
        width: "var(--codepane-width)",
        minWidth: "var(--codepane-width)",
        background: "var(--codepane-bg)",
        borderLeft: "1px solid var(--codepane-border)",
      }}
    >
      <CodePaneHeader
        nodeName={displayName}
        onCopy={handleCopy}
        onReset={handleReset}
        onSave={handleSave}
        onClose={onClose}
        hasDefault={!!definition.default_code}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />

      {/* Tab content */}
      {activeTab === "code" && (
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
      )}

      {activeTab === "params" && (
        <div
          style={{
            background: "#1A1625",
            color: "#E2E0F0",
            padding: 12,
            overflowY: "auto",
            flex: 1,
          }}
        >
          <ParamsTab
            params={definition.params}
            values={buildParamValues(node)}
            onChange={(name, value) => onParamChange(node.id, name, value)}
            disabled={paramSaving}
          />
        </div>
      )}

      {activeTab === "output" && (
        <div
          style={{
            background: "#1A1625",
            color: "#E2E0F0",
            padding: 12,
            overflowY: "auto",
            flex: 1,
          }}
        >
          <OutputTab
            pipelineId={pipelineId}
            nodeId={node.id}
            onRunFrom={onRunFrom ? () => onRunFrom(node.id) : undefined}
          />
        </div>
      )}

      <CodePaneFooter unsaved={unsaved} activeTab={activeTab} />
    </div>
  );
}

function buildParamValues(node: NodeInstance): Record<string, unknown> {
  if (Array.isArray(node.params)) {
    const values: Record<string, unknown> = {};
    for (const p of node.params) {
      values[p.name] = p.default;
    }
    return values;
  }
  return { ...(node.params as Record<string, unknown>) };
}
