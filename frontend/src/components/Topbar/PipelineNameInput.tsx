import { useState, useCallback, useRef, useEffect } from "react";

interface PipelineNameInputProps {
  name: string;
  onRename: (name: string) => void;
}

export default function PipelineNameInput({
  name,
  onRename,
}: PipelineNameInputProps) {
  const [isEditing, setIsEditing] = useState(false);
  const [draft, setDraft] = useState(name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!isEditing) setDraft(name);
  }, [name, isEditing]);

  useEffect(() => {
    if (isEditing) inputRef.current?.select();
  }, [isEditing]);

  const commit = useCallback(() => {
    setIsEditing(false);
    const trimmed = draft.trim();
    if (trimmed && trimmed !== name) {
      onRename(trimmed);
    } else {
      setDraft(name);
    }
  }, [draft, name, onRename]);

  if (!isEditing) {
    return (
      <button
        type="button"
        className="font-semibold text-sm truncate max-w-60 px-1.5 py-0.5 rounded hover:bg-black/5 transition-colors cursor-text"
        style={{ color: "var(--text-primary)" }}
        onClick={() => setIsEditing(true)}
        title="Click to rename"
      >
        {name}
      </button>
    );
  }

  return (
    <input
      ref={inputRef}
      type="text"
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") commit();
        if (e.key === "Escape") {
          setDraft(name);
          setIsEditing(false);
        }
      }}
      className="font-semibold text-sm max-w-60 px-1.5 py-0.5 rounded outline-none border"
      style={{
        color: "var(--text-primary)",
        borderColor: "var(--accent-blue)",
      }}
    />
  );
}
