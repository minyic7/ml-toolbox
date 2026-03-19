import type { OutputPreview } from "../../lib/types";
import { PORT_COLORS } from "../../lib/portColors";
import { TablePreview } from "./TablePreview";
import { MetricsDisplay } from "./MetricsDisplay";
import { ErrorTraceback } from "./ErrorTraceback";

interface OutputTabProps {
  output: OutputPreview | null;
  downloadUrl: string | null;
}

export function OutputTab({ output, downloadUrl }: OutputTabProps) {
  if (!output) {
    return (
      <div
        className="flex h-32 items-center justify-center text-sm"
        style={{ color: "var(--text-muted)" }}
      >
        No output yet. Run the pipeline to see results.
      </div>
    );
  }

  if (output.error) {
    return (
      <div className="p-4">
        <ErrorTraceback error={output.error} />
      </div>
    );
  }

  const typeBadgeColor =
    PORT_COLORS[output.type as keyof typeof PORT_COLORS] ?? "var(--text-muted)";

  return (
    <div className="flex flex-col gap-3 p-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span
            className="rounded-sm px-1.5 py-0.5 text-[10px] font-semibold uppercase text-white"
            style={{ backgroundColor: typeBadgeColor }}
          >
            {output.type}
          </span>
          <span className="text-xs" style={{ color: "var(--text-muted)" }}>
            {formatSize(output.size)}
          </span>
        </div>
        {downloadUrl && (
          <a
            href={downloadUrl}
            download
            className="rounded-md px-2.5 py-1 text-xs font-medium transition-colors"
            style={{
              color: "var(--accent-blue)",
              border: "1px solid var(--accent-blue)",
            }}
          >
            Download
          </a>
        )}
      </div>

      {renderPreview(output)}
    </div>
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
            className="rounded-md border p-3 text-xs"
            style={{ borderColor: "var(--border-default)" }}
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
          className="rounded-md border p-3 text-xs"
          style={{ borderColor: "var(--border-default)" }}
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
