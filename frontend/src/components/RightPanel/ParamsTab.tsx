import type { ParamDefinition } from "../../lib/types";
import { ParamControl } from "./ParamControl";

interface ParamsTabProps {
  params: ParamDefinition[];
  values: Record<string, unknown>;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
}

export function ParamsTab({ params, values, onChange, disabled }: ParamsTabProps) {
  if (params.length === 0) {
    return (
      <div
        className="flex h-32 items-center justify-center text-sm"
        style={{ color: "var(--text-muted)" }}
      >
        No parameters
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      <span className="text-[11px] font-medium uppercase tracking-wider" style={{ color: "var(--text-muted)" }}>
        Input Config
      </span>
      {params.map((param) => (
        <ParamControl
          key={param.name}
          param={param}
          value={values[param.name]}
          onChange={onChange}
          disabled={disabled}
        />
      ))}
    </div>
  );
}
