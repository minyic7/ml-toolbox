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
import { Download, FileText, BarChart3, Box, AlertCircle } from "lucide-react";

/**
 * Simplified output tab for the bottom drawer.
 * Shows only: status badge + list of output files with type/size/download.
 * Full output (table preview, metrics, error traceback) lives in CodePane.
 */

interface DrawerOutputTabProps {
  pipelineId: string;
  nodeId: string;
  requestedRunId?: string | null;
  onRequestedRunHandled?: () => void;
  onRunFrom?: () => void;
}

export function DrawerOutputTab({
  pipelineId,
  nodeId,
  requestedRunId,
  onRequestedRunHandled,
  onRunFrom,
}: DrawerOutputTabProps) {
  const [selectedRunId, setSelectedRunId] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (requestedRunId) {
      setSelectedRunId(requestedRunId);
      onRequestedRunHandled?.();
    }
  }, [requestedRunId, onRequestedRunHandled]);

  const { data: runs } = useRuns(pipelineId);
  const { data: output = null, isLoading: outputLoading } = useOutput(
    pipelineId,
    nodeId,
    selectedRunId,
  );

  const downloadUrl = useMemo(
    () => getOutputDownloadUrl(pipelineId, nodeId, selectedRunId),
    [pipelineId, nodeId, selectedRunId],
  );

  return (
    <div className="flex flex-col" style={{ fontFamily: "'Inter', sans-serif" }}>
      {/* Run selector */}
      {runs && runs.length > 0 && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            padding: "6px 12px",
            borderBottom: "1px solid var(--border-default)",
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
                  {run.id.slice(0, 8)} ({run.status})
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {outputLoading ? (
        <div className="flex items-center gap-2" style={{ padding: "12px 16px" }}>
          <div
            className="animate-pulse"
            style={{ height: 14, width: 80, borderRadius: 4, background: "var(--border-default)" }}
          />
        </div>
      ) : output ? (
        <OutputSummaryRow output={output} downloadUrl={downloadUrl} />
      ) : (
        <div
          className="flex flex-col items-center justify-center gap-1"
          style={{ padding: "16px" }}
        >
          <span style={{ fontSize: 11, fontWeight: 500, color: "var(--text-muted)" }}>
            No output yet
          </span>
          {onRunFrom && (
            <button
              onClick={onRunFrom}
              style={{
                marginTop: 4,
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
      )}
    </div>
  );
}

function OutputSummaryRow({
  output,
  downloadUrl,
}: {
  output: OutputPreview;
  downloadUrl: string;
}) {
  const isError = output.type === "ERROR" || !!output.error;
  const TypeIcon = getTypeIcon(output.type);

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "10px 12px",
      }}
    >
      {/* Type icon + badge */}
      <TypeIcon style={{ width: 14, height: 14, color: "var(--text-muted)", flexShrink: 0 }} />
      <span
        style={{
          fontSize: 10,
          fontWeight: 600,
          fontFamily: "'Inter', sans-serif",
          textTransform: "uppercase",
          letterSpacing: "0.04em",
          color: isError ? "var(--error-red)" : "var(--text-primary)",
        }}
      >
        {output.type}
      </span>

      {/* Status badge */}
      {!isError && (
        <span
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 4,
            padding: "1px 8px",
            borderRadius: 9999,
            background: "var(--output-healthy-bg)",
            color: "var(--output-healthy-text)",
            fontSize: 9,
            fontWeight: 700,
            fontFamily: "'Inter', sans-serif",
          }}
        >
          <span
            style={{
              width: 5,
              height: 5,
              borderRadius: "50%",
              background: "var(--output-healthy-dot)",
              display: "inline-block",
            }}
          />
          Healthy
        </span>
      )}
      {isError && (
        <span
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 4,
            padding: "1px 8px",
            borderRadius: 9999,
            background: "rgba(239,68,68,0.1)",
            color: "var(--error-red)",
            fontSize: 9,
            fontWeight: 700,
            fontFamily: "'Inter', sans-serif",
          }}
        >
          Error
        </span>
      )}

      {/* Size */}
      <span
        style={{
          fontSize: 10,
          color: "var(--text-muted)",
          fontFamily: "'JetBrains Mono', monospace",
          marginLeft: "auto",
        }}
      >
        {formatSize(output.size)}
      </span>

      {/* Download link */}
      {!isError && (
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
            padding: "3px 8px",
            cursor: "pointer",
            textDecoration: "none",
            whiteSpace: "nowrap",
          }}
        >
          <Download style={{ width: 10, height: 10 }} />
          {output.type === "TABLE" ? "CSV" : "Download"}
        </a>
      )}
    </div>
  );
}

function getTypeIcon(type: string) {
  switch (type) {
    case "TABLE":
      return FileText;
    case "METRICS":
      return BarChart3;
    case "MODEL":
      return Box;
    case "ERROR":
      return AlertCircle;
    default:
      return FileText;
  }
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
