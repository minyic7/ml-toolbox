import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Info, Upload, ChevronDown, Check } from "lucide-react";
import { toast } from "sonner";
import type { ParamDefinition, Edge, PortDefinition } from "../../lib/types";
import { uploadFile } from "../../lib/api";
import { useOutput } from "../../hooks/useOutputs";
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
  pipelineId?: string;
  edges?: Edge[];
  nodeId?: string;
  nodeInputs?: PortDefinition[];
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

/** Find the source node ID for a TABLE input connected to the given node. */
function findUpstreamTableNodeId(
  nodeId: string,
  edges: Edge[],
  nodeInputs?: PortDefinition[],
): string | undefined {
  // Find TABLE input port names from the node definition
  const tablePortNames = (nodeInputs ?? [])
    .filter((p) => p.type === "TABLE")
    .map((p) => p.name);

  // If we know TABLE ports, filter edges to only those ports
  if (tablePortNames.length > 0) {
    const tableEdge = edges.find(
      (e) => e.target === nodeId && tablePortNames.includes(e.target_port),
    );
    return tableEdge?.source;
  }

  // Fallback: any incoming edge (when node definition isn't available)
  return edges.find((e) => e.target === nodeId)?.source;
}

/** Column dropdown for target_column params with upstream column discovery. */
function ColumnSelect({
  columns,
  value,
  onChange,
  disabled,
  paramName,
}: {
  columns: string[];
  value: string;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
  paramName: string;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  const filtered = useMemo(
    () =>
      search
        ? columns.filter((c) => c.toLowerCase().includes(search.toLowerCase()))
        : columns,
    [columns, search],
  );

  // Focus search input when dropdown opens
  useEffect(() => {
    if (open) {
      // Small delay to ensure DOM is ready
      requestAnimationFrame(() => inputRef.current?.focus());
    } else {
      setSearch("");
    }
  }, [open]);

  return (
    <div className="relative">
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen(!open)}
        className="flex h-8 w-full items-center justify-between rounded-md border px-3 text-sm"
        style={{
          borderColor: "var(--border-default)",
          background: "var(--bg-primary)",
          opacity: disabled ? 0.6 : 1,
        }}
      >
        <span
          className="truncate"
          style={{ color: value ? "var(--text-primary)" : "var(--text-muted)" }}
        >
          {value || "Select column..."}
        </span>
        <ChevronDown size={14} className="shrink-0 text-[var(--text-muted)]" />
      </button>

      {open && (
        <div
          ref={listRef}
          className="absolute z-50 mt-1 w-full rounded-md border shadow-md"
          style={{
            background: "var(--bg-primary)",
            borderColor: "var(--border-default)",
            maxHeight: 200,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div className="p-1.5" style={{ borderBottom: "1px solid var(--border-default)" }}>
            <input
              ref={inputRef}
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search columns..."
              className="h-7 w-full rounded border-none bg-transparent px-2 text-xs outline-none"
              style={{ color: "var(--text-primary)" }}
              onKeyDown={(e) => {
                if (e.key === "Escape") setOpen(false);
                if (e.key === "Enter" && filtered.length > 0) {
                  onChange(paramName, filtered[0]);
                  setOpen(false);
                }
              }}
            />
          </div>
          <div className="overflow-y-auto p-1" style={{ maxHeight: 160 }}>
            {filtered.length === 0 ? (
              <div
                className="px-2 py-1.5 text-xs"
                style={{ color: "var(--text-muted)" }}
              >
                No columns match
              </div>
            ) : (
              filtered.map((col) => (
                <button
                  key={col}
                  type="button"
                  onClick={() => {
                    onChange(paramName, col);
                    setOpen(false);
                  }}
                  className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs hover:bg-[var(--bg-hover)]"
                  style={{ color: "var(--text-primary)" }}
                >
                  <Check
                    size={12}
                    className="shrink-0"
                    style={{
                      opacity: col === value ? 1 : 0,
                      color: "var(--accent-primary)",
                    }}
                  />
                  {col}
                </button>
              ))
            )}
          </div>
        </div>
      )}

      {/* Close on outside click */}
      {open && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setOpen(false)}
        />
      )}
    </div>
  );
}

export function ParamControl({ param, value, onChange, disabled, pipelineId, edges, nodeId, nodeInputs }: ParamControlProps) {
  const [textValue, setTextValue] = useState(String(value ?? param.default ?? ""));
  const [textError, setTextError] = useState(false);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Find upstream TABLE node for target_column params
  // TODO: future improvement — auto-clear value when upstream schema changes (stale selection)
  const isTargetColumn = param.name === "target_column" && param.type === "text";
  const upstreamNodeId = useMemo(
    () => (isTargetColumn && nodeId && edges ? findUpstreamTableNodeId(nodeId, edges, nodeInputs) : undefined),
    [isTargetColumn, nodeId, edges, nodeInputs],
  );
  const { data: upstreamOutput } = useOutput(
    pipelineId ?? "",
    upstreamNodeId ?? "",
  );
  const upstreamColumns = useMemo(
    () => upstreamOutput?.preview?.columns ?? [],
    [upstreamOutput],
  );

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

      // Render column dropdown for target_column when upstream columns are available
      if (isTargetColumn && upstreamColumns.length > 0) {
        return (
          <div className="flex flex-col gap-1.5" style={disabledStyle}>
            <ParamLabel param={param} />
            <ColumnSelect
              columns={upstreamColumns}
              value={String(value ?? param.default ?? "")}
              onChange={onChange}
              disabled={disabled}
              paramName={param.name}
            />
          </div>
        );
      }

      return (
        <div className="flex flex-col gap-1.5" style={disabledStyle}>
          <ParamLabel param={param} />
          <div className="flex gap-1.5">
            <Input
              type="text"
              value={textValue}
              disabled={disabled}
              placeholder={
                isTargetColumn && upstreamColumns.length === 0
                  ? "Run upstream node first to see columns"
                  : (param.placeholder ?? "")
              }
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
                    const MAX_SIZE = 100 * 1024 * 1024; // 100MB, matches backend
                    if (file.size > MAX_SIZE) {
                      toast.error(`File too large (${Math.round(file.size / 1024 / 1024)}MB). Maximum is 100MB.`);
                      e.target.value = "";
                      return;
                    }
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
