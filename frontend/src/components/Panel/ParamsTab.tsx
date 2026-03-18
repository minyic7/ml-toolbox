import type { NodeParam } from "@/lib/types";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { Switch } from "@/components/ui/switch";

export interface ParamsTabProps {
  params: NodeParam[];
  values: Record<string, string | number | boolean>;
  onParamChange: (paramName: string, value: string | number | boolean) => void;
}

export function ParamsTab({ params, values, onParamChange }: ParamsTabProps) {
  if (params.length === 0) {
    return (
      <p className="py-4 text-center text-xs text-muted-foreground">
        No parameters for this node.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {params.map((param) => (
        <ParamControl
          key={param.name}
          param={param}
          value={values[param.name] ?? param.default}
          onChange={(v) => onParamChange(param.name, v)}
        />
      ))}
    </div>
  );
}

function ParamControl({
  param,
  value,
  onChange,
}: {
  param: NodeParam;
  value: string | number | boolean;
  onChange: (value: string | number | boolean) => void;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-xs font-medium text-foreground">
        {param.name}
      </label>
      {param.type === "select" && (
        <Select
          value={String(value)}
          onValueChange={(v) => onChange(v)}
        >
          <SelectTrigger className="h-8 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {param.options?.map((opt) => (
              <SelectItem key={opt} value={opt}>
                {opt}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}
      {param.type === "slider" && (
        <div className="flex items-center gap-3">
          <Slider
            className="flex-1"
            min={param.min ?? 0}
            max={param.max ?? 100}
            step={param.step ?? 1}
            value={[Number(value)]}
            onValueChange={([v]) => onChange(v ?? 0)}
          />
          <span className="w-10 text-right text-xs tabular-nums text-muted-foreground">
            {value}
          </span>
        </div>
      )}
      {param.type === "text" && (
        <Input
          className="h-8 text-xs"
          value={String(value)}
          onChange={(e) => onChange(e.target.value)}
        />
      )}
      {param.type === "toggle" && (
        <Switch
          checked={Boolean(value)}
          onCheckedChange={(checked) => onChange(checked)}
        />
      )}
    </div>
  );
}
