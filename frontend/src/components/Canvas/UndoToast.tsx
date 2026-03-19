import { useEffect, useState } from "react";

export interface UndoToastData {
  message: string;
  onUndo: () => void;
}

interface UndoToastProps {
  data: UndoToastData | null;
  onDismiss: () => void;
}

const TOAST_DURATION = 4000;

export default function UndoToast({ data, onDismiss }: UndoToastProps) {
  const [phase, setPhase] = useState<"enter" | "exit" | "hidden">("hidden");

  useEffect(() => {
    if (!data) {
      setPhase("hidden");
      return;
    }

    // Reset to enter on new data
    setPhase("enter");

    const exitTimer = setTimeout(() => {
      setPhase("exit");
    }, TOAST_DURATION - 300);

    const dismissTimer = setTimeout(() => {
      onDismiss();
    }, TOAST_DURATION);

    return () => {
      clearTimeout(exitTimer);
      clearTimeout(dismissTimer);
    };
  }, [data, onDismiss]);

  if (phase === "hidden" || !data) return null;

  return (
    <div
      style={{
        position: "absolute",
        bottom: 24,
        left: "50%",
        transform: `translateX(-50%) translateY(${phase === "enter" ? "0" : "8px"})`,
        opacity: phase === "enter" ? 1 : 0,
        transition: "opacity 300ms ease, transform 300ms ease",
        zIndex: 50,
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "10px 16px",
        borderRadius: 8,
        background: "var(--node-bg)",
        color: "var(--text-primary)",
        fontSize: 13,
        fontWeight: 500,
        boxShadow: "0 2px 8px rgba(0,0,0,0.12)",
        border: "1px solid var(--border-default)",
        pointerEvents: "auto",
      }}
    >
      <span>{data.message}</span>
      <button
        onClick={(e) => {
          e.stopPropagation();
          data.onUndo();
          onDismiss();
        }}
        style={{
          background: "none",
          border: "none",
          color: "var(--accent-blue)",
          cursor: "pointer",
          fontWeight: 600,
          fontSize: 13,
          padding: "2px 4px",
          borderRadius: 4,
        }}
      >
        Undo
      </button>
    </div>
  );
}
