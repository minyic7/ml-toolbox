import type { NodeOutputState } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

export interface OutputTabProps {
  outputState?: NodeOutputState;
  downloadUrl?: string;
}

export function OutputTab({ outputState, downloadUrl }: OutputTabProps) {
  if (!outputState) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <p className="text-xs text-muted-foreground">
          Run node to see output
        </p>
      </div>
    );
  }

  if (outputState.error) {
    return (
      <div className="flex flex-col gap-2">
        <div className="rounded-md bg-red-950/50 p-3">
          <pre className="whitespace-pre-wrap font-mono text-xs text-red-400">
            {outputState.error}
          </pre>
        </div>
      </div>
    );
  }

  const { output } = outputState;
  if (!output) {
    return (
      <div className="flex flex-1 items-center justify-center">
        <p className="text-xs text-muted-foreground">
          Run node to see output
        </p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {downloadUrl && (
        <div className="flex justify-end">
          <Button variant="outline" size="xs" asChild>
            <a href={downloadUrl} download>
              <Download className="size-3" />
              Download
            </a>
          </Button>
        </div>
      )}

      {output.type === "TABLE" && <TableView output={output} />}
      {output.type === "METRICS" && <JsonView data={output.data} />}
      {output.type === "VALUE" && <JsonView data={output.data} />}
      {output.type === "MODEL" && <ModelView output={output} />}
    </div>
  );
}

function TableView({ output }: { output: { columns: string[]; row_count: number; rows: Record<string, unknown>[] } }) {
  const displayRows = output.rows.slice(0, 10);

  return (
    <div className="flex flex-col gap-2">
      <p className="text-xs text-muted-foreground">
        {output.row_count} rows, {output.columns.length} columns
      </p>
      <div className="overflow-x-auto rounded-md border border-border">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border bg-secondary/50">
              {output.columns.map((col) => (
                <th key={col} className="px-2 py-1 text-left font-medium">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayRows.map((row, i) => (
              <tr key={i} className="border-b border-border last:border-0">
                {output.columns.map((col) => (
                  <td key={col} className="px-2 py-1 text-muted-foreground">
                    {String(row[col] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {output.row_count > 10 && (
        <p className="text-[10px] text-muted-foreground">
          Showing 10 of {output.row_count} rows
        </p>
      )}
    </div>
  );
}

function JsonView({ data }: { data: unknown }) {
  return (
    <pre className="overflow-x-auto rounded-md border border-border bg-secondary/30 p-3 font-mono text-xs text-foreground">
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

function ModelView({ output }: { output: { file_size?: number; model_type?: string } }) {
  return (
    <div className="flex flex-col gap-2 rounded-md border border-border p-3 text-xs">
      {output.model_type && (
        <div className="flex justify-between">
          <span className="text-muted-foreground">Type</span>
          <span className="text-foreground">{output.model_type}</span>
        </div>
      )}
      {output.file_size != null && (
        <div className="flex justify-between">
          <span className="text-muted-foreground">Size</span>
          <span className="text-foreground">{formatBytes(output.file_size)}</span>
        </div>
      )}
      {!output.model_type && output.file_size == null && (
        <p className="text-muted-foreground">Model output available</p>
      )}
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
