import type { ComponentType } from "react";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";

interface CategoryColors {
  bg: string;
  border: string;
  icon: string;
  dot: string;
}

/** Any component that accepts color + size — works for both Lucide and custom SVGs */
export type NodeIcon = ComponentType<{ color?: string; size?: number }>;

interface NodeIconChipProps {
  icon: NodeIcon;
  label: string;
  colors: CategoryColors;
  onClick: () => void;
}

export default function NodeIconChip({
  icon: Icon,
  label,
  colors,
  onClick,
}: NodeIconChipProps) {
  return (
    <Tooltip delayDuration={0}>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={label}
          onClick={onClick}
          style={{
            width: 28,
            height: 28,
            borderRadius: 7,
            backgroundColor: colors.bg,
            border: `1px solid ${colors.border}`,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            position: "relative",
            cursor: "pointer",
            padding: 0,
            flexShrink: 0,
            transition: "border-color 150ms, transform 150ms",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = colors.dot;
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = colors.border;
          }}
          onMouseDown={(e) => {
            e.currentTarget.style.transform = "scale(0.96)";
          }}
          onMouseUp={(e) => {
            e.currentTarget.style.transform = "scale(1)";
          }}
        >
          <Icon size={14} color={colors.icon} />
          {/* Badge dot */}
          <span
            style={{
              position: "absolute",
              top: -3,
              right: -3,
              width: 7,
              height: 7,
              borderRadius: "50%",
              backgroundColor: colors.dot,
              border: "1.5px solid var(--node-bg)",
            }}
          />
        </button>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="text-xs font-medium">
        {label}
      </TooltipContent>
    </Tooltip>
  );
}
