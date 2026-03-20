import { useCancelPipeline } from "../../hooks/useExecution";

interface CancelButtonProps {
  pipelineId: string;
}

export default function CancelButton({ pipelineId }: CancelButtonProps) {
  const cancelMutation = useCancelPipeline(pipelineId);

  return (
    <button
      type="button"
      onClick={() => cancelMutation.mutate()}
      disabled={cancelMutation.isPending}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: "0 10px",
        height: 28,
        borderRadius: 7,
        background: "#FFF7F7",
        border: "1px solid #FECDD3",
        color: "var(--error-red)",
        fontFamily: "'Inter', sans-serif",
        fontWeight: 700,
        fontSize: 11,
        cursor: cancelMutation.isPending ? "not-allowed" : "pointer",
        opacity: cancelMutation.isPending ? 0.6 : 1,
        transition: "background 0.15s",
      }}
      onMouseEnter={(e) => {
        if (!cancelMutation.isPending) e.currentTarget.style.background = "#FFE4E6";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = "#FFF7F7";
      }}
    >
      <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor">
        <rect x="1" y="1" width="8" height="8" rx="1" />
      </svg>
      Cancel
    </button>
  );
}
