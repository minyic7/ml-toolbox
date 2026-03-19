import Editor from "@monaco-editor/react";

interface CodeTabProps {
  code: string;
  defaultCode: string | undefined;
  onChange: (code: string) => void;
  onBlur: () => void;
}

export function CodeTab({ code, defaultCode, onChange, onBlur }: CodeTabProps) {
  const readOnly = !defaultCode;

  return (
    <div className="flex h-full flex-col">
      {readOnly && (
        <div
          className="flex items-center gap-1.5 px-4 py-2 text-xs"
          style={{ color: "var(--text-muted)", backgroundColor: "var(--canvas-bg)" }}
        >
          <span
            className="rounded-sm px-1.5 py-0.5 text-[10px] font-medium uppercase"
            style={{
              backgroundColor: "var(--border-default)",
              color: "var(--text-secondary)",
            }}
          >
            read-only
          </span>
        </div>
      )}
      <div className="min-h-0 flex-1" onBlur={onBlur}>
        <Editor
          height="100%"
          language="python"
          theme="vs-dark"
          value={code}
          onChange={(v) => onChange(v ?? "")}
          options={{
            readOnly,
            minimap: { enabled: false },
            lineNumbers: "on",
            fontSize: 13,
            scrollBeyondLastLine: false,
            automaticLayout: true,
            padding: { top: 12 },
            wordWrap: "on",
          }}
        />
      </div>
    </div>
  );
}
