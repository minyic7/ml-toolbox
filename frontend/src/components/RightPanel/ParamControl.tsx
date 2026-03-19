import { useState } from "react";
import type { ParamDefinition } from "../../lib/types";

interface ParamControlProps {
  param: ParamDefinition;
  value: unknown;
  onChange: (name: string, value: unknown) => void;
}

export function ParamControl({ param, value, onChange }: ParamControlProps) {
  const [textValue, setTextValue] = useState(String(value ?? param.default ?? ""));

  switch (param.type) {
    case "select":
      return (
        <div className="flex flex-col gap-1.5">
          <label
            className="text-xs font-medium"
            style={{ color: "var(--text-secondary)" }}
          >
            {param.name}
          </label>
          <select
            value={String(value ?? param.default ?? "")}
            onChange={(e) => onChange(param.name, e.target.value)}
            className="h-8 rounded-md border px-2 text-sm outline-none"
            style={{
              borderColor: "var(--border-default)",
              backgroundColor: "var(--node-bg)",
              color: "var(--text-primary)",
            }}
          >
            {param.options?.map((opt) => (
              <option key={opt} value={opt}>
                {opt}
              </option>
            ))}
          </select>
        </div>
      );

    case "slider": {
      const min = param.min ?? 0;
      const max = param.max ?? 100;
      const step = param.step ?? 1;
      const numValue = Number(value ?? param.default ?? min);
      return (
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <label
              className="text-xs font-medium"
              style={{ color: "var(--text-secondary)" }}
            >
              {param.name}
            </label>
            <span
              className="text-xs tabular-nums"
              style={{ color: "var(--text-muted)" }}
            >
              {numValue}
            </span>
          </div>
          <input
            type="range"
            min={min}
            max={max}
            step={step}
            value={numValue}
            onChange={(e) => onChange(param.name, Number(e.target.value))}
            className="h-1.5 w-full cursor-pointer appearance-none rounded-full"
            style={{ accentColor: "var(--accent-blue)" }}
          />
        </div>
      );
    }

    case "text":
      return (
        <div className="flex flex-col gap-1.5">
          <label
            className="text-xs font-medium"
            style={{ color: "var(--text-secondary)" }}
          >
            {param.name}
          </label>
          <input
            type="text"
            value={textValue}
            onChange={(e) => setTextValue(e.target.value)}
            onBlur={() => onChange(param.name, textValue)}
            className="h-8 rounded-md border px-2 text-sm outline-none"
            style={{
              borderColor: "var(--border-default)",
              backgroundColor: "var(--node-bg)",
              color: "var(--text-primary)",
            }}
          />
        </div>
      );

    case "toggle": {
      const checked = !!(value ?? param.default);
      return (
        <div className="flex items-center justify-between">
          <label
            className="text-xs font-medium"
            style={{ color: "var(--text-secondary)" }}
          >
            {param.name}
          </label>
          <button
            type="button"
            role="switch"
            aria-checked={checked}
            onClick={() => onChange(param.name, !checked)}
            className="relative h-5 w-9 rounded-full transition-colors duration-200"
            style={{
              backgroundColor: checked
                ? "var(--accent-blue)"
                : "var(--border-default)",
            }}
          >
            <span
              className="absolute top-0.5 left-0.5 h-4 w-4 rounded-full transition-transform duration-200"
              style={{
                backgroundColor: "white",
                transform: checked
                  ? "translateX(16px)"
                  : "translateX(0)",
              }}
            />
          </button>
        </div>
      );
    }

    default:
      return null;
  }
}
