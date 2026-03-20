import type { ParamDefinition } from "../../lib/types";
import { ParamControl } from "./ParamControl";

interface ParamsTabProps {
  params: ParamDefinition[];
  values: Record<string, unknown>;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
  onCodePaneOpen?: () => void;
}

export function ParamsTab({ params, values, onChange, disabled, onCodePaneOpen }: ParamsTabProps) {
  if (params.length === 0) {
    return (
      <div
        className="flex h-full items-center justify-center text-xs"
        style={{ color: "var(--text-muted)" }}
      >
        No parameters
      </div>
    );
  }

  return (
    <div className="p-3 overflow-y-auto h-full">
      <span
        style={{
          fontFamily: "var(--drawer-label-font)",
          fontWeight: 700,
          fontSize: 9,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          color: "var(--text-muted)",
        }}
      >
        Input Config
      </span>
      <div
        className="grid gap-x-4 gap-y-3 mt-2"
        style={{ gridTemplateColumns: "1fr 1fr" }}
      >
        {params.map((param) => (
          <div
            key={param.name}
            style={
              /* sliders and toggles work better full-width */
              param.type === "slider" || param.type === "toggle"
                ? { gridColumn: "1 / -1" }
                : undefined
            }
          >
            <ParamControl
              key={param.name}
              param={param}
              value={values[param.name]}
              onChange={onChange}
              disabled={disabled}
            />
          </div>
        ))}
      </div>

      {onCodePaneOpen && (
        <button
          onClick={onCodePaneOpen}
          className="mt-3 transition-opacity"
          style={{
            fontFamily: "var(--drawer-label-font)",
            fontWeight: 700,
            fontSize: 10,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
            color: "var(--accent-blue)",
            opacity: 0.7,
            background: "none",
            border: "none",
            cursor: "pointer",
            padding: 0,
          }}
          onMouseEnter={(e) => { e.currentTarget.style.opacity = "1"; }}
          onMouseLeave={(e) => { e.currentTarget.style.opacity = "0.7"; }}
        >
          {"{ }"} View / edit code &rarr;
        </button>
      )}
    </div>
  );
}
