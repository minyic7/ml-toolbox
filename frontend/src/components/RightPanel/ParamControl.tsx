import { useCallback, useEffect, useRef, useState } from "react";
import { Info, Upload } from "lucide-react";
import type { ParamDefinition } from "../../lib/types";
import { uploadFile } from "../../lib/api";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface ParamControlProps {
  param: ParamDefinition;
  value: unknown;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
}

function ParamLabel({ param }: { param: ParamDefinition }) {
  return (
    <div className="flex items-center gap-1">
      <Label className="text-xs font-medium text-[var(--text-secondary)]">
        {param.name}
      </Label>
      {param.description && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Info size={12} className="text-[var(--text-muted)] cursor-help shrink-0" />
          </TooltipTrigger>
          <TooltipContent side="top" className="max-w-[200px] text-xs">
            {param.description}
          </TooltipContent>
        </Tooltip>
      )}
    </div>
  );
}

export function ParamControl({ param, value, onChange, disabled }: ParamControlProps) {
  const [textValue, setTextValue] = useState(String(value ?? param.default ?? ""));
  const [textError, setTextError] = useState(false);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Sync text value when external value changes (e.g. revert on error)
  useEffect(() => {
    setTextValue(String(value ?? param.default ?? ""));
    setTextError(false);
  }, [value, param.default]);

  // Debounce helper for select/text — flushes pending change on unmount
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingRef = useRef<(() => void) | null>(null);
  const debouncedOnChange = useCallback(
    (name: string, val: unknown) => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      const commit = () => onChange(name, val);
      pendingRef.current = commit;
      debounceRef.current = setTimeout(() => {
        commit();
        pendingRef.current = null;
      }, 500);
    },
    [onChange],
  );

  // Flush pending debounce on unmount (don't lose user input)
  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      pendingRef.current?.();
    };
  }, []);

  // Slider local state (commit on release, not during drag)
  const [sliderLocal, setSliderLocal] = useState<number | null>(null);

  const disabledStyle: React.CSSProperties | undefined = disabled
    ? { opacity: 0.6, pointerEvents: "none" }
    : undefined;

  switch (param.type) {
    case "select":
      return (
        <div className="flex flex-col gap-1.5" style={disabledStyle}>
          <ParamLabel param={param} />
          <Select
            value={String(value ?? param.default ?? "")}
            onValueChange={(v) => onChange(param.name, v)}
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
        <div className="flex flex-col gap-1.5" style={disabledStyle}>
          <div className="flex items-center justify-between">
            <ParamLabel param={param} />
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
            style={{ accentColor: "var(--accent-primary)" }}
          />
        </div>
      );
    }

    case "text": {
      const isNumeric = typeof (param.default ?? value) === "number";
      const isPathParam = param.name === "path";
      return (
        <div className="flex flex-col gap-1.5" style={disabledStyle}>
          <ParamLabel param={param} />
          <div className="flex gap-1.5">
            <Input
              type="text"
              value={textValue}
              disabled={disabled}
              placeholder={param.placeholder ?? ""}
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
              className="h-8 text-sm flex-1"
              style={textError ? { borderColor: "var(--error-red)" } : undefined}
            />
            {isPathParam && (
              <>
                <button
                  type="button"
                  disabled={disabled || uploading}
                  onClick={() => fileInputRef.current?.click()}
                  className="h-8 px-2 border rounded text-xs shrink-0 hover:bg-[var(--bg-hover)] disabled:opacity-50"
                  style={{ borderColor: "var(--border-default)" }}
                  title="Upload file"
                >
                  <Upload size={14} />
                </button>
                <input
                  ref={fileInputRef}
                  type="file"
                  className="hidden"
                  accept=".csv,.parquet,.tsv,.json"
                  onChange={async (e) => {
                    const file = e.target.files?.[0];
                    if (!file) return;
                    setUploading(true);
                    try {
                      const result = await uploadFile(file);
                      onChange(param.name, result.path);
                      setTextValue(result.path);
                    } catch {
                      setTextError(true);
                    } finally {
                      setUploading(false);
                      // Reset so re-selecting the same file triggers onChange
                      e.target.value = "";
                    }
                  }}
                />
              </>
            )}
          </div>
          {textError && (
            <span className="text-[10px]" role="alert" aria-live="polite" style={{ color: "var(--error-red)" }}>
              {isPathParam ? "Upload failed" : "Must be a valid number"}
            </span>
          )}
        </div>
      );
    }

    case "toggle": {
      const checked = !!(value ?? param.default);
      return (
        <div className="flex items-center justify-between" style={disabledStyle}>
          <ParamLabel param={param} />
          <button
            type="button"
            role="switch"
            aria-checked={checked}
            disabled={disabled}
            onClick={() => onChange(param.name, !checked)}
            className="relative h-5 w-9 rounded-full transition-colors duration-200"
            style={{
              backgroundColor: checked
                ? "var(--accent-primary)"
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
