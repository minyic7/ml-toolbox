import { useEffect, useState, useMemo } from "react";
import type { OutputPreview, OutputPortPreview } from "../../lib/types";
import { useOutput, useRuns, useAnalysis } from "../../hooks/useOutputs";
import { useExecutionStore } from "../../store/executionStore";
import { getOutputDownloadUrl } from "../../lib/api";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Download, AlertCircle, RefreshCw, Loader2 } from "lucide-react";
import { TablePreview } from "./TablePreview";
import { MetricsDisplay } from "./MetricsDisplay";
import { ErrorTraceback } from "./ErrorTraceback";
import { ProfileReport, ProfileReportBoundary } from "../ProfileReport/ProfileReport";
import { AnalysisPanel } from "../ProfileReport/AnalysisPanel";

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
  const [selectedPort, setSelectedPort] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (requestedRunId) {
      setSelectedRunId(requestedRunId);
      onRequestedRunHandled?.();
    }
  }, [requestedRunId, onRequestedRunHandled]);

  const { data: runs } = useRuns(pipelineId);
  const { data: output = null, isLoading: outputLoading, isError: outputError, refetch: refetchOutput } = useOutput(pipelineId, nodeId, selectedRunId);
  const { data: analysis = null, isFetching: analysisFetching } = useAnalysis(pipelineId, nodeId, selectedRunId);
  const nodeStatus = useExecutionStore((s) => s.nodeStatuses[nodeId]);

  // Show loading when node just finished and analysis hasn't arrived yet
  const analysisLoading = !analysis && analysisFetching || (!analysis && nodeStatus === "done");

  const isMultiOutput = !!(output?.outputs && output.outputs.length > 1);

  // Reset selectedPort when output changes
  useEffect(() => {
    if (isMultiOutput) {
      setSelectedPort((prev) => {
        // Keep current selection if it's still valid
        if (prev && output!.outputs!.some((o) => o.port === prev)) return prev;
        return output!.outputs![0].port;
      });
    } else {
      setSelectedPort(undefined);
    }
  }, [output, isMultiOutput]);

  const downloadUrl = useMemo(
    () => getOutputDownloadUrl(pipelineId, nodeId, selectedRunId, undefined, selectedPort),
    [pipelineId, nodeId, selectedRunId, selectedPort],
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

      {/* Port selector tabs — for multi-output nodes */}
      {isMultiOutput && (
        <div style={{
          display: "flex",
          gap: 4,
          padding: "6px 12px",
          borderBottom: "1px solid var(--border-default)",
        }}>
          {output!.outputs!.map((o) => (
            <button
              key={o.port}
              onClick={() => setSelectedPort(o.port)}
              style={{
                fontSize: 10,
                fontWeight: selectedPort === o.port ? 700 : 500,
                padding: "3px 10px",
                borderRadius: 6,
                border: selectedPort === o.port
                  ? "1px solid var(--accent-primary)"
                  : "1px solid var(--border-default)",
                background: selectedPort === o.port
                  ? "var(--ghost-hover-bg)"
                  : "transparent",
                color: selectedPort === o.port
                  ? "var(--accent-primary)"
                  : "var(--text-muted)",
                cursor: "pointer",
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                fontFamily: "'Inter', sans-serif",
              }}
            >
              {o.port}
            </button>
          ))}
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
          pipelineId={pipelineId}
          nodeId={nodeId}
          selectedRunId={selectedRunId}
          selectedPort={selectedPort}
          onRunFrom={onRunFrom ? () => onRunFrom(nodeId) : undefined}
          analysis={analysis}
          analysisLoading={analysisLoading}
        />
      )}
    </div>
  );
}

const ghostButtonStyle: React.CSSProperties = {
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
};

function hoverIn(e: React.MouseEvent<HTMLAnchorElement>) {
  e.currentTarget.style.background = "var(--output-row-hover)";
}

function hoverOut(e: React.MouseEvent<HTMLAnchorElement>) {
  e.currentTarget.style.background = "transparent";
}

function OutputContent({
  output,
  downloadUrl,
  pipelineId,
  nodeId,
  selectedRunId,
  selectedPort,
  onRunFrom,
  analysis,
  analysisLoading,
}: {
  output: OutputPreview | null;
  downloadUrl: string;
  pipelineId: string;
  nodeId: string;
  selectedRunId?: string;
  selectedPort?: string;
  onRunFrom?: () => void;
  analysis?: import("../../lib/types").CcAnalysis | null;
  analysisLoading?: boolean;
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
        {output.logs?.trim() && (
          <div style={{ marginTop: 8 }}>
            <div style={{
              fontFamily: "'Inter', sans-serif",
              fontSize: 9,
              fontWeight: 700,
              textTransform: "uppercase" as const,
              letterSpacing: "0.05em",
              color: "var(--text-muted)",
              marginBottom: 4,
            }}>
              Container logs
            </div>
            <pre style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: 10,
              color: "#E2E0F0",
              whiteSpace: "pre-wrap",
              margin: 0,
              padding: 8,
              borderRadius: 6,
              background: "#1A1625",
              maxHeight: 200,
              overflowY: "auto",
            }}>
              {output.logs}
            </pre>
          </div>
        )}
      </div>
    );
  }

  // Resolve active output for multi-output nodes
  const isMultiOutput = !!(output.outputs && output.outputs.length > 1);
  const activePort: OutputPortPreview | undefined = isMultiOutput
    ? output.outputs!.find((o) => o.port === selectedPort) ?? output.outputs![0]
    : undefined;

  // Use active port's data when available, otherwise fall back to primary
  const displayType = activePort ? activePort.type : output.type;
  const displayPreview = activePort ? activePort.preview : output.preview;
  const displaySize = activePort ? activePort.size : output.size;

  const type = normalizeType(displayType);

  // Build summary text
  const summaryParts: string[] = [type];
  if (displayPreview?.total_rows != null) {
    summaryParts.push(`${displayPreview.total_rows.toLocaleString()} rows`);
  }
  if (displayPreview?.columns) {
    summaryParts.push(`${displayPreview.columns.length} cols`);
  }
  const summaryText = summaryParts.join(" · ");

  const portParam = activePort?.port;

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
          {(type === "TABLE" || type === "METRICS" || type === "VALUE") && !output.error && (
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

        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          {type === "TABLE" ? (
            <>
              <a
                href={getOutputDownloadUrl(pipelineId, nodeId, selectedRunId, "csv", portParam)}
                download
                style={ghostButtonStyle}
                onMouseEnter={hoverIn}
                onMouseLeave={hoverOut}
              >
                <Download style={{ width: 10, height: 10 }} />
                CSV
              </a>
              <a
                href={getOutputDownloadUrl(pipelineId, nodeId, selectedRunId, undefined, portParam)}
                download
                style={ghostButtonStyle}
                onMouseEnter={hoverIn}
                onMouseLeave={hoverOut}
              >
                <Download style={{ width: 10, height: 10 }} />
                Parquet
              </a>
            </>
          ) : (
            <a
              href={downloadUrl}
              download
              style={ghostButtonStyle}
              onMouseEnter={hoverIn}
              onMouseLeave={hoverOut}
            >
              <Download style={{ width: 10, height: 10 }} />
              Download
            </a>
          )}
        </div>
      </div>

      {/* Transform summary card */}
      {output.transform_summary && (
        <TransformSummaryCard summary={output.transform_summary} />
      )}

      {/* Preview content */}
      <div style={{ padding: "0 12px 8px" }}>
        {renderPreview({ ...output, type: displayType, preview: displayPreview, size: displaySize }, analysis)}
      </div>

      {/* AI Analysis — shown at the bottom, after report data */}
      {analysis && (
        <div style={{ padding: "0 12px 8px" }}>
          <AnalysisPanel analysis={analysis} />
        </div>
      )}

      {/* Loading indicator while analysis is pending */}
      {!analysis && analysisLoading && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            padding: "8px 12px",
            color: "var(--text-muted)",
          }}
        >
          <Loader2 style={{ width: 12, height: 12, animation: "spin 1s linear infinite" }} />
          <span
            style={{
              fontSize: 11,
              fontFamily: "'Inter', sans-serif",
              fontWeight: 500,
            }}
          >
            AI analyzing output...
          </span>
        </div>
      )}
    </>
  );
}

/** Map file-extension types returned by the backend to PortType enum values. */
const EXT_TO_PORT_TYPE: Record<string, string> = {
  parquet: "TABLE",
  csv: "TABLE",
  json: "METRICS",
  joblib: "MODEL",
  pkl: "MODEL",
  npy: "ARRAY",
  pt: "TENSOR",
};

function normalizeType(type: string): string {
  return EXT_TO_PORT_TYPE[type] ?? type;
}

function renderPreview(output: OutputPreview, analysis?: import("../../lib/types").CcAnalysis | null) {
  const { preview } = output;
  const type = normalizeType(output.type);

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
            dtypes={preview.dtypes}
            hasMetadata={!!output.column_metadata}
          />
        );
      }
      return null;

    case "METRICS":
      if (preview && typeof preview === "object" && "report_type" in preview) {
        const reportData = preview as unknown as Record<string, unknown>;
        return (
          <ProfileReportBoundary data={reportData}>
            <ProfileReport data={reportData} analysis={analysis} />
          </ProfileReportBoundary>
        );
      }
      return <MetricsDisplay data={preview as unknown as Record<string, unknown>} />;

    case "VALUE":
      return <ValueDisplay data={preview as unknown as Record<string, unknown>} />;

    case "MODEL":
      return <ModelDisplay preview={preview} size={output.size} />;

    case "ARRAY":
      if (preview.shape) {
        return (
          <div style={{ padding: 12 }}>
            <div className="text-xs font-mono" style={{ color: "var(--text-muted)" }}>
              shape: [{preview.shape.join(", ")}] · dtype: {preview.dtype}
            </div>
            {preview.values && (
              <div className="text-xs font-mono mt-2" style={{ color: "var(--text-primary)" }}>
                [{preview.values.join(", ")}{(preview.total_elements ?? 0) > (preview.values?.length ?? 0) ? ", ..." : ""}]
              </div>
            )}
            <div className="output-table-footer mt-1">
              {preview.total_elements?.toLocaleString()} elements
            </div>
          </div>
        );
      }
      return null;

    case "TENSOR":
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

function TransformSummaryCard({ summary }: {
  summary: NonNullable<OutputPreview["transform_summary"]>;
}) {
  const { method, transformed_columns, skipped_columns, target_column } = summary;

  return (
    <div
      style={{
        margin: "8px 12px 0",
        padding: "8px 10px",
        borderRadius: 6,
        border: "1px solid var(--border-default)",
        background: "var(--ghost-hover-bg)",
      }}
    >
      <div style={{
        fontFamily: "'Inter', sans-serif",
        fontSize: 9,
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        color: "var(--text-muted)",
        marginBottom: 6,
      }}>
        {method} Transform Applied
      </div>

      {transformed_columns.length > 0 && (
        <div style={{ marginBottom: 4 }}>
          <span style={{
            fontFamily: "'Inter', sans-serif",
            fontSize: 10,
            fontWeight: 600,
            color: "var(--output-healthy-text)",
          }}>
            {"+ "}
          </span>
          <span style={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10,
            color: "var(--text-primary)",
          }}>
            {transformed_columns.join(", ")}
          </span>
          <span style={{
            fontFamily: "'Inter', sans-serif",
            fontSize: 10,
            color: "var(--text-muted)",
            marginLeft: 4,
          }}>
            ({transformed_columns.length} column{transformed_columns.length !== 1 ? "s" : ""})
          </span>
        </div>
      )}

      {skipped_columns.length > 0 && (
        <div style={{ marginBottom: target_column ? 4 : 0 }}>
          <span style={{
            fontFamily: "'Inter', sans-serif",
            fontSize: 10,
            color: "var(--text-muted)",
          }}>
            {"- Unchanged: "}
          </span>
          <span style={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10,
            color: "var(--text-muted)",
          }}>
            {skipped_columns.join(", ")}
          </span>
        </div>
      )}

      {target_column && (
        <div>
          <span style={{
            fontFamily: "'Inter', sans-serif",
            fontSize: 10,
            color: "var(--text-muted)",
          }}>
            {"Target (protected): "}
          </span>
          <span style={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10,
            fontWeight: 600,
            color: "var(--text-muted)",
          }}>
            {target_column}
          </span>
        </div>
      )}
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
