import { useEffect } from "react";
import { toast } from "sonner";

export interface UndoToastData {
  message: string;
  onUndo: () => void;
}

interface UndoToastProps {
  data: UndoToastData | null;
  onDismiss: () => void;
}

export default function UndoToast({ data, onDismiss }: UndoToastProps) {
  useEffect(() => {
    if (!data) return;

    const toastId = toast(data.message, {
      action: {
        label: "Undo",
        onClick: () => {
          data.onUndo();
        },
      },
      duration: 4000,
      onDismiss: () => onDismiss(),
      onAutoClose: () => onDismiss(),
    });

    return () => {
      toast.dismiss(toastId);
    };
  }, [data, onDismiss]);

  return null;
}
