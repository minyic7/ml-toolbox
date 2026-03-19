import { useCallback, useEffect, useRef, useState } from "react";
import type { ParamDefinition } from "../../lib/types";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface ParamControlProps {
  param: ParamDefinition;
  value: unknown;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
}

export function ParamControl({ param, value, onChange, disabled }: ParamControlProps) {
  const [textValue, setTextValue] = useState(String(value ?? param.default ?? ""));
  const [textError, setTextError] = useState(false);

  // Sync text value when external value changes (e.g. revert on error)
  useEffect(() => {
    setTextValue(String(value ?? param.default ?? ""));
    setTextError(false);
  }, [value, param.default]);

  // Debounce helper for select/text
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const debouncedOnChange = useCallback(
    (name: string, val: unknown) => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        onChange(name, val);
      }, 500);
    },
    [onChange],
  );

  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  // Slider local state (commit on release, not during drag)
  const [sliderLocal, setSliderLocal] = useState<number | null>(null);

  switch (param.type) {
    case "select":
      return (
        <div className="flex flex-col gap-1.5">
          <Label className="text-xs font-medium text-[var(--text-secondary)]">
            {param.name}
          </Label>
          <Select
            value={String(value ?? param.default ?? "")}
            onValueChange={(v) => debouncedOnChange(param.name, v)}
            disabled={disabled}
          >
            <SelectTrigger className="h-8 text-sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {param.options?.map((opt: string) => (
                <SelectItem key={opt} value={opt}>
                  {opt}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      );

    case "slider": {
      const min = param.min ?? 0;
      const max = param.max ?? 100;
      const step = param.step ?? 1;
      const numValue = sliderLocal ?? Number(value ?? param.default ?? min);
      return (
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <Label className="text-xs font-medium text-[var(--text-secondary)]">
              {param.name}
            </Label>
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
            disabled={disabled}
            onChange={(e) => setSliderLocal(Number(e.target.value))}
            onPointerUp={() => {
              if (sliderLocal !== null) {
                onChange(param.name, sliderLocal);
                setSliderLocal(null);
              }
            }}
            onBlur={() => {
              if (sliderLocal !== null) {
                onChange(param.name, sliderLocal);
                setSliderLocal(null);
              }
            }}
            className="h-1.5 w-full cursor-pointer appearance-none rounded-full"
            style={{ accentColor: "var(--accent-blue)" }}
          />
        </div>
      );
    }

    case "text": {
      const isNumeric = typeof (param.default ?? value) === "number";
      return (
        <div className="flex flex-col gap-1.5">
          <Label className="text-xs font-medium text-[var(--text-secondary)]">
            {param.name}
          </Label>
          <Input
            type="text"
            value={textValue}
            disabled={disabled}
            onChange={(e) => {
              setTextValue(e.target.value);
              setTextError(false);
            }}
            onBlur={() => {
              if (isNumeric) {
                const num = Number(textValue);
                if (isNaN(num)) {
                  setTextError(true);
                  return;
                }
                debouncedOnChange(param.name, num);
              } else {
                debouncedOnChange(param.name, textValue);
              }
            }}
            className="h-8 text-sm"
            style={textError ? { borderColor: "var(--error-red)" } : undefined}
          />
          {textError && (
            <span className="text-[10px]" style={{ color: "var(--error-red)" }}>
              Must be a valid number
            </span>
          )}
        </div>
      );
    }

    case "toggle": {
      const checked = !!(value ?? param.default);
      return (
        <div className="flex items-center justify-between">
          <Label className="text-xs font-medium text-[var(--text-secondary)]">
            {param.name}
          </Label>
          <button
            type="button"
            role="switch"
            aria-checked={checked}
            disabled={disabled}
            onClick={() => onChange(param.name, !checked)}
            className="relative h-5 w-9 rounded-full transition-colors duration-200"
            style={{
              backgroundColor: checked
                ? "var(--accent-blue)"
                : "var(--border-default)",
              opacity: disabled ? 0.5 : 1,
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
