import { Handle, Position } from "@xyflow/react";
import type { PortDefinition } from "../../lib/types";
import { PORT_COLORS } from "../../lib/portColors";
import { useExecutionStore } from "../../store/executionStore";

interface PortDotProps {
  port: PortDefinition;
  side: "input" | "output";
  index: number;
  total: number;
}

export default function PortDot({ port, side, index, total }: PortDotProps) {
  const color = PORT_COLORS[port.type];
  const isInput = side === "input";
  const draggingPortType = useExecutionStore((s) => s.draggingPortType);

  const isDragging = draggingPortType !== null;
  const isMatch = isDragging && port.type === draggingPortType;
  const isMismatch = isDragging && port.type !== draggingPortType;

  // Distribute ports evenly along the node height
  const offset = total === 1 ? 50 : 20 + (60 / (total - 1)) * index;

  // Build className for hover pseudo-class styling
  const className = isMatch
    ? "port-dot-match"
    : isMismatch
      ? "port-dot-mismatch"
      : "";

  return (
    <Handle
      type={isInput ? "target" : "source"}
      position={isInput ? Position.Left : Position.Right}
      id={port.name}
      title={`${port.name} (${port.type})`}
      className={className}
      style={{
        top: `${offset}%`,
        width: 10,
        height: 10,
        borderRadius: "50%",
        backgroundColor: color,
        border: "2px solid var(--node-bg)",
        boxShadow: isMatch
          ? `0 0 0 1px ${color}, 0 0 6px 2px ${color}`
          : `0 0 0 1px ${color}`,
        opacity: isMismatch ? 0.4 : 1,
        transition: "box-shadow 0.15s ease, opacity 0.15s ease, transform 0.15s ease",
      }}
    />
  );
}
