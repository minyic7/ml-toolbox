import { useEffect, useState, useMemo } from "react";
import type { OutputPreview } from "../../lib/types";
import { useOutput, useRuns } from "../../hooks/useOutputs";
import { getOutputDownloadUrl } from "../../lib/api";
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
    <div className="flex flex-col" style={{ fontFamily: "'Inter', sans-serif" }}>
      {/* Run selector bar — always visible when runs exist */}
      {runs && runs.length > 0 && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            padding: "6px 12px",
            borderBottom: "1px solid var(--output-summary-border)",
          }}
        >
          <Select
            value={selectedRunId ?? "latest"}
            onValueChange={(v) =>
              setSelectedRunId(v === "latest" ? undefined : v)
            }
          >
            <SelectTrigger
              className="h-6"
              style={{
                fontSize: 9,
                fontWeight: 700,
                fontFamily: "'Inter', sans-serif",
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                border: "1px solid var(--border-default)",
                borderRadius: 6,
                padding: "0 8px",
                minWidth: 100,
                background: "transparent",
              }}
            >
              <SelectValue placeholder="Latest Run" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="latest">Latest Run</SelectItem>
              {runs.map((run) => (
                <SelectItem key={run.id} value={run.id}>
                  {run.id.slice(0, 8)} — {formatTimestamp(run.started_at)} ({run.status})
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {/* Output content */}
      {outputError ? (
        <div
          className="flex flex-col items-center justify-center gap-2"
          style={{ color: "var(--text-muted)", padding: "32px 16px" }}
        >
          <AlertCircle style={{ width: 20, height: 20, color: "var(--error-red)" }} />
          <span style={{ fontSize: 11, fontWeight: 600 }}>Failed to load output</span>
          <button
            onClick={() => refetchOutput()}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              fontSize: 9,
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
              border: "1px solid var(--border-default)",
              borderRadius: 6,
              background: "transparent",
              color: "var(--text-muted)",
              padding: "4px 10px",
              cursor: "pointer",
            }}
          >
            <RefreshCw style={{ width: 10, height: 10 }} />
            Retry
          </button>
        </div>
      ) : outputLoading ? (
        <div className="flex flex-col gap-3" style={{ padding: 16 }}>
          <div className="animate-pulse" style={{ height: 16, width: 96, borderRadius: 4, background: "var(--border-default)" }} />
          <div className="animate-pulse" style={{ height: 128, borderRadius: 4, background: "var(--border-default)", opacity: 0.5 }} />
        </div>
      ) : (
        <OutputContent
          output={output}
          downloadUrl={downloadUrl}
          onRunFrom={onRunFrom ? () => onRunFrom(nodeId) : undefined}
        />
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
        className="flex flex-col items-center justify-center gap-1"
        style={{ padding: "32px 16px" }}
      >
        <span className="output-empty" style={{ padding: 0 }}>
          No output yet
        </span>
        <span style={{
          fontFamily: "'Inter', sans-serif",
          fontSize: 10,
          fontWeight: 400,
          color: "var(--text-muted)",
          opacity: 0.7,
        }}>
          Run the pipeline to see results
        </span>
        {onRunFrom && (
          <button
            onClick={onRunFrom}
            style={{
              marginTop: 8,
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              fontSize: 9,
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
              fontFamily: "'Inter', sans-serif",
              border: "1px solid var(--border-default)",
              borderRadius: 6,
              background: "transparent",
              color: "var(--text-muted)",
              padding: "4px 10px",
              cursor: "pointer",
            }}
          >
            ▶ Run from here
          </button>
        )}
      </div>
    );
  }

  if (output.type === "ERROR" || output.error) {
    return (
      <div style={{ padding: "8px 12px" }}>
        <ErrorTraceback error={output.error ?? "Unknown error"} />
      </div>
    );
  }

  // Build summary text
  const summaryParts: string[] = [output.type];
  if (output.preview?.total_rows != null) {
    summaryParts.push(`${output.preview.total_rows.toLocaleString()} rows`);
  }
  if (output.preview?.columns) {
    summaryParts.push(`${output.preview.columns.length} cols`);
  }
  const summaryText = summaryParts.join(" · ");

  return (
    <>
      {/* Summary bar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "8px 12px",
          borderBottom: "1px solid var(--output-summary-border)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{
            fontFamily: "'Inter', sans-serif",
            fontSize: 9,
            fontWeight: 700,
            textTransform: "uppercase",
            letterSpacing: "0.05em",
            color: "var(--text-muted)",
          }}>
            {summaryText}
          </span>
          {/* Healthy pill — shown for TABLE/METRICS/VALUE when no error */}
          {(output.type === "TABLE" || output.type === "METRICS" || output.type === "VALUE") && !output.error && (
            <span style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              padding: "1px 8px",
              borderRadius: 9999,
              background: "var(--output-healthy-bg)",
              color: "var(--output-healthy-text)",
              fontFamily: "'Inter', sans-serif",
              fontSize: 9,
              fontWeight: 700,
            }}>
              <span style={{
                width: 5,
                height: 5,
                borderRadius: "50%",
                background: "var(--output-healthy-dot)",
                display: "inline-block",
              }} />
              Healthy
            </span>
          )}
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {/* Export CSV / Download ghost button */}
          <a
            href={downloadUrl}
            download
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              fontSize: 9,
              fontWeight: 700,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
              fontFamily: "'Inter', sans-serif",
              border: "1px solid var(--border-default)",
              borderRadius: 6,
              background: "transparent",
              color: "var(--text-muted)",
              padding: "3px 10px",
              cursor: "pointer",
              textDecoration: "none",
              whiteSpace: "nowrap",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "var(--output-row-hover)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "transparent";
            }}
          >
            <Download style={{ width: 10, height: 10 }} />
            {output.type === "TABLE" ? "Export CSV" : "Download"}
          </a>
        </div>
      </div>

      {/* Preview content */}
      <div style={{ padding: "0 12px 8px" }}>
        {renderPreview(output)}
      </div>
    </>
  );
}

function renderPreview(output: OutputPreview) {
  const { type, preview } = output;

  if (!preview) {
    return (
      <div className="output-empty">
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
      return <MetricsDisplay data={preview as unknown as Record<string, unknown>} />;

    case "VALUE":
      return <ValueDisplay data={preview as unknown as Record<string, unknown>} />;

    case "MODEL":
      return <ModelDisplay preview={preview} size={output.size} />;

    case "ERROR":
      return <ErrorTraceback error={JSON.stringify(preview, null, 2)} />;

    default:
      return (
        <div
          style={{
            borderRadius: 6,
            border: "1px solid var(--border-default)",
            padding: 12,
            marginTop: 8,
          }}
        >
          <pre
            style={{
              whiteSpace: "pre-wrap",
              color: "var(--text-primary)",
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 10,
              margin: 0,
            }}
          >
            {JSON.stringify(preview, null, 2)}
          </pre>
        </div>
      );
  }
}

/** VALUE type: large centered mono value with type label */
function ValueDisplay({ data }: { data: Record<string, unknown> }) {
  const entries = Object.entries(data);
  // For VALUE type, show the first value prominently
  const [key, value] = entries[0] ?? ["value", "—"];

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      justifyContent: "center",
      padding: "20px 0",
      gap: 4,
    }}>
      <span style={{
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: 24,
        fontWeight: 600,
        color: "var(--text-primary)",
      }}>
        {typeof value === "number"
          ? Number.isInteger(value)
            ? value.toLocaleString()
            : value.toFixed(4)
          : String(value)}
      </span>
      <span style={{
        fontFamily: "'Inter', sans-serif",
        fontSize: 10,
        fontWeight: 400,
        color: "var(--text-muted)",
      }}>
        {key}
      </span>
    </div>
  );
}

/** MODEL type: class name + file size + download button */
function ModelDisplay({ preview, size }: { preview: Record<string, unknown>; size: number }) {
  const className = (preview.class_name as string) ?? (preview.model_class as string) ?? "Model";

  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      padding: "12px 0",
      borderRadius: 6,
      border: "1px solid var(--border-default)",
      marginTop: 8,
      paddingLeft: 12,
      paddingRight: 12,
    }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <span style={{
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 11,
          fontWeight: 600,
          color: "var(--text-primary)",
        }}>
          {className}
        </span>
        <span style={{
          fontFamily: "'Inter', sans-serif",
          fontSize: 10,
          fontWeight: 400,
          color: "var(--text-muted)",
        }}>
          {formatSize(size)}
        </span>
      </div>
    </div>
  );
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
