import type { ParamDefinition, Edge } from "../../lib/types";
import { ParamControl } from "./ParamControl";

interface ParamsTabProps {
  params: ParamDefinition[];
  values: Record<string, unknown>;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
  pipelineId?: string;
  edges?: Edge[];
  nodeId?: string;
}

export function ParamsTab({ params, values, onChange, disabled, pipelineId, edges, nodeId }: ParamsTabProps) {
  if (params.length === 0) {
    return (
      <div
        className="flex items-center justify-center"
        style={{
          color: "var(--text-muted)",
          fontFamily: "'Inter', sans-serif",
          fontWeight: 600,
          fontSize: 11,
          height: "100%",
        }}
      >
        No parameters
      </div>
    );
  }

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: "10px 24px",
        padding: "10px 16px",
      }}
    >
      {params.map((param) => (
        <ParamControl
          key={param.name}
          param={param}
          value={values[param.name]}
          onChange={onChange}
          disabled={disabled}
          pipelineId={pipelineId}
          edges={edges}
          nodeId={nodeId}
        />
      ))}
    </div>
  );
}
