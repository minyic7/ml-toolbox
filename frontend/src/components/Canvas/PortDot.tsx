import { Handle, Position } from "@xyflow/react";
import type { PortDefinition } from "../../lib/types";
import { PORT_COLORS } from "../../lib/portColors";

interface PortDotProps {
  port: PortDefinition;
  side: "input" | "output";
  index: number;
  total: number;
}

export default function PortDot({ port, side, index, total }: PortDotProps) {
  const color = PORT_COLORS[port.type];
  const isInput = side === "input";

  // Distribute ports evenly along the node height
  const offset = total === 1 ? 50 : 20 + (60 / (total - 1)) * index;

  return (
    <Handle
      type={isInput ? "target" : "source"}
      position={isInput ? Position.Left : Position.Right}
      id={port.name}
      title={`${port.name} (${port.type})`}
      style={{
        top: `${offset}%`,
        width: 10,
        height: 10,
        borderRadius: "50%",
        backgroundColor: color,
        border: "2px solid var(--node-bg)",
        boxShadow: `0 0 0 1px ${color}`,
      }}
    />
  );
}
