import Editor from "@monaco-editor/react";

export interface CodeTabProps {
  code: string;
  readOnly?: boolean;
  onChange?: (value: string) => void;
  onBlur?: () => void;
}

export function CodeTab({ code, readOnly, onChange, onBlur }: CodeTabProps) {
  return (
    <div className="flex min-h-[300px] flex-1 flex-col overflow-hidden rounded-md border border-border">
      {readOnly && (
        <div className="border-b border-border bg-secondary/50 px-3 py-1 text-[10px] text-muted-foreground">
          Read-only
        </div>
      )}
      <Editor
        defaultLanguage="python"
        theme="vs-dark"
        value={code}
        onChange={(v) => onChange?.(v ?? "")}
        options={{
          readOnly,
          minimap: { enabled: false },
          fontSize: 13,
          lineNumbers: "on",
          scrollBeyondLastLine: false,
          wordWrap: "on",
          padding: { top: 8 },
          automaticLayout: true,
        }}
        onMount={(editor) => {
          if (onBlur) {
            editor.onDidBlurEditorWidget(() => onBlur());
          }
        }}
      />
    </div>
  );
}
