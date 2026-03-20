import { useState, useCallback, useRef, useEffect } from "react";
import { Input } from "@/components/ui/input";
import { Pencil } from "lucide-react";

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
  const [isHovered, setIsHovered] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!isEditing) setDraft(name);
  }, [name, isEditing]);

  useEffect(() => {
    if (isEditing) inputRef.current?.select();
  }, [isEditing]);

  const commit = useCallback(() => {
    const trimmed = draft.trim();
    if (!trimmed) {
      // Shake on empty
      inputRef.current?.classList.add("shake");
      setTimeout(() => inputRef.current?.classList.remove("shake"), 300);
      return;
    }
    setIsEditing(false);
    if (trimmed !== name) {
      onRename(trimmed);
    } else {
      setDraft(name);
    }
  }, [draft, name, onRename]);

  if (!isEditing) {
    return (
      <button
        type="button"
        className="flex items-center gap-1.5 truncate max-w-60 cursor-text bg-transparent border-none p-0"
        style={{
          fontFamily: "'Manrope', sans-serif",
          fontWeight: 600,
          fontSize: 13,
          color: "rgba(124, 58, 237, 0.85)",
        }}
        onClick={() => setIsEditing(true)}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
        title="Click to rename"
      >
        <span className="truncate">{name}</span>
        <Pencil
          className="shrink-0 transition-opacity duration-150"
          style={{
            width: 12,
            height: 12,
            opacity: isHovered ? 0.7 : 0,
            color: "rgba(124, 58, 237, 0.85)",
          }}
        />
      </button>
    );
  }

  return (
    <Input
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
      className="max-w-60 h-7 px-1.5 py-0.5"
      style={{
        fontFamily: "'Manrope', sans-serif",
        fontWeight: 600,
        fontSize: 13,
        color: "rgba(124, 58, 237, 0.85)",
        borderColor: "var(--accent-primary)",
      }}
    />
  );
}
