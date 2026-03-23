import { Handle, Position } from "@xyflow/react";
import type { PortDefinition } from "../../lib/types";
import { PORT_COLORS } from "../../lib/portColors";
import { useExecutionStore } from "../../store/executionStore";
import { useState } from "react";

interface PortDotProps {
  port: PortDefinition;
  side: "input" | "output";
}

const DOT_SIZE = 10;
const DOT_HOVER_SIZE = 14;

export default function PortDot({ port, side }: PortDotProps) {
  const color = PORT_COLORS[port.type];
  const isInput = side === "input";
  const draggingPortType = useExecutionStore((s) => s.draggingPortType);
  const [hovered, setHovered] = useState(false);

  const isDragging = draggingPortType !== null;
  const isMatch = isDragging && port.type === draggingPortType;
  const isMismatch = isDragging && port.type !== draggingPortType;

  const className = isMismatch ? "port-dot-mismatch" : "";

  // Enlarge on hover without position shift:
  // increase width/height and use negative margin to compensate
  const enlarged = hovered || isMatch;
  const size = enlarged ? DOT_HOVER_SIZE : DOT_SIZE;
  const offset = enlarged ? -(DOT_HOVER_SIZE - DOT_SIZE) / 2 : 0;

  return (
    <Handle
      type={isInput ? "target" : "source"}
      position={isInput ? Position.Left : Position.Right}
      id={port.name}
      title={`${port.name} (${port.type})`}
      className={className}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        width: size,
        height: size,
        marginTop: offset,
        marginBottom: offset,
        borderRadius: "50%",
        backgroundColor: color,
        border: "2px solid var(--node-bg)",
        boxShadow: isMatch
          ? `0 0 0 2px ${color}, 0 0 8px 3px ${color}`
          : hovered
            ? `0 0 0 2px ${color}`
            : `0 0 0 1px ${color}`,
        opacity: isMismatch ? 0.4 : 1,
        transition: "width 0.12s ease, height 0.12s ease, margin 0.12s ease, box-shadow 0.15s ease, opacity 0.15s ease",
      }}
    />
  );
}
