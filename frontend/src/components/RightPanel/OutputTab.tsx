import { useEffect, useState, useMemo } from "react";
import type { OutputPreview } from "../../lib/types";
import { PORT_COLORS } from "../../lib/portColors";
import { useOutput, useRuns } from "../../hooks/useOutputs";
import { getOutputDownloadUrl } from "../../lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Download, AlertCircle, RefreshCw } from "lucide-react";
import { TablePreview } from "./TablePreview";
import { MetricsDisplay } from "./MetricsDisplay";
import { ErrorTraceback } from "./ErrorTraceback";

interface OutputTabProps {
  pipelineId: string;
  nodeId: string;
  requestedRunId?: string | null;
  onRequestedRunHandled?: () => void;
  onRunFrom?: (nodeId: string) => void;
}

export function OutputTab({ pipelineId, nodeId, requestedRunId, onRequestedRunHandled, onRunFrom }: OutputTabProps) {
  const [selectedRunId, setSelectedRunId] = useState<string | undefined>(
    undefined,
  );

  useEffect(() => {
    if (requestedRunId) {
      setSelectedRunId(requestedRunId);
      onRequestedRunHandled?.();
    }
  }, [requestedRunId, onRequestedRunHandled]);

  const { data: runs } = useRuns(pipelineId);
  const { data: output = null, isLoading: outputLoading, isError: outputError, refetch: refetchOutput } = useOutput(pipelineId, nodeId, selectedRunId);

  const downloadUrl = useMemo(
    () => getOutputDownloadUrl(pipelineId, nodeId, selectedRunId),
    [pipelineId, nodeId, selectedRunId],
  );

  return (
    <div className="flex flex-col gap-3 p-4">
      {/* Run selector */}
      {runs && runs.length > 0 && (
        <Select
          value={selectedRunId ?? "latest"}
          onValueChange={(v) =>
            setSelectedRunId(v === "latest" ? undefined : v)
          }
        >
          <SelectTrigger className="h-8 text-xs">
            <SelectValue placeholder="Latest Run" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="latest">Latest Run</SelectItem>
            {runs.map((run) => (
              <SelectItem key={run.id} value={run.id}>
                {run.id.slice(0, 8)} — {formatTimestamp(run.started_at)} (
                {run.status})
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}

      {/* Output content */}
      {outputError ? (
        <div
          className="flex flex-col h-32 items-center justify-center gap-2 text-sm"
          style={{ color: "var(--text-muted)" }}
        >
          <AlertCircle className="h-5 w-5" style={{ color: "var(--error-red)" }} />
          <span>Failed to load output</span>
          <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => refetchOutput()}>
            <RefreshCw className="h-3 w-3" />
            Retry
          </Button>
        </div>
      ) : outputLoading ? (
        <div className="flex flex-col gap-3 p-4">
          <div className="animate-pulse h-4 w-24 rounded" style={{ background: "var(--border-default)" }} />
          <div className="animate-pulse h-32 rounded" style={{ background: "var(--border-default)", opacity: 0.5 }} />
        </div>
      ) : (
        <OutputContent output={output} downloadUrl={downloadUrl} onRunFrom={onRunFrom ? () => onRunFrom(nodeId) : undefined} />
      )}
    </div>
  );
}

function OutputContent({
  output,
  downloadUrl,
  onRunFrom,
}: {
  output: OutputPreview | null;
  downloadUrl: string;
  onRunFrom?: () => void;
}) {
  if (!output) {
    return (
      <div
        className="flex flex-col h-32 items-center justify-center gap-2 text-sm"
        style={{ color: "var(--text-muted)" }}
      >
        No output yet. Run the pipeline to see results.
        {onRunFrom && (
          <Button variant="outline" size="sm" className="h-7 text-xs" onClick={onRunFrom}>
            ▶ Run from here
          </Button>
        )}
      </div>
    );
  }

  if (output.error) {
    return <ErrorTraceback error={output.error} />;
  }

  const typeBadgeColor =
    PORT_COLORS[output.type as keyof typeof PORT_COLORS] ?? "var(--text-muted)";

  return (
    <>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Badge
            className="rounded-sm px-1.5 py-0.5 text-[10px] font-semibold uppercase text-white border-0"
            style={{ backgroundColor: typeBadgeColor }}
          >
            {output.type}
          </Badge>
          <span className="text-xs" style={{ color: "var(--text-muted)" }}>
            {formatSize(output.size)}
          </span>
        </div>
        <Button variant="outline" size="sm" className="h-7 text-xs" asChild>
          <a href={downloadUrl} download>
            <Download className="h-3 w-3" />
            Download
          </a>
        </Button>
      </div>

      {renderPreview(output)}
    </>
  );
}

function renderPreview(output: OutputPreview) {
  const { type, preview } = output;

  if (!preview) {
    return (
      <div className="text-xs" style={{ color: "var(--text-muted)" }}>
        No preview available
      </div>
    );
  }

  switch (type) {
    case "TABLE":
      if (preview.columns && preview.rows) {
        return (
          <TablePreview
            columns={preview.columns}
            rows={preview.rows}
            totalRows={preview.total_rows ?? preview.rows.length}
          />
        );
      }
      return null;

    case "METRICS":
    case "VALUE":
      return <MetricsDisplay data={preview as unknown as Record<string, unknown>} />;

    case "MODEL":
      return (
        <div className="flex flex-col gap-2">
          <div
            className="rounded-md border border-border p-3 text-xs"
          >
            <pre
              className="whitespace-pre-wrap"
              style={{ color: "var(--text-primary)" }}
            >
              {JSON.stringify(preview, null, 2)}
            </pre>
          </div>
        </div>
      );

    case "ERROR":
      return <ErrorTraceback error={JSON.stringify(preview, null, 2)} />;

    default:
      return (
        <div
          className="rounded-md border border-border p-3 text-xs"
        >
          <pre
            className="whitespace-pre-wrap"
            style={{ color: "var(--text-primary)" }}
          >
            {JSON.stringify(preview, null, 2)}
          </pre>
        </div>
      );
  }
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTimestamp(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
