import { useQuery } from "@tanstack/react-query";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { getEdaContext } from "../../lib/api";

interface EdaContextModalProps {
  open: boolean;
  onClose: () => void;
  pipelineId: string;
  nodeId: string;
}

interface ColumnStats {
  skewness?: number;
  kurtosis?: number;
  outlier_pct?: number;
  z_max?: number;
  missing_pct?: number;
  severity?: string;
  [key: string]: unknown;
}

interface CorrelationPair {
  col1?: string;
  col2?: string;
  correlation?: number;
  [key: string]: unknown;
}

interface EdaData {
  distribution?: Record<string, ColumnStats>;
  outliers?: Record<string, ColumnStats>;
  missing?: Record<string, ColumnStats>;
  correlation?: {
    high_pairs?: CorrelationPair[];
    target_correlations?: Record<string, number>;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

export default function EdaContextModal({
  open,
  onClose,
  pipelineId,
  nodeId,
}: EdaContextModalProps) {
  const { data, isLoading } = useQuery({
    queryKey: ["eda-context", pipelineId, nodeId],
    queryFn: () => getEdaContext(pipelineId, nodeId),
    enabled: open && !!pipelineId && !!nodeId,
  });

  const eda = data?.eda_context as EdaData | null | undefined;

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-[80vw] max-h-[80vh] overflow-auto">
        <DialogHeader>
          <DialogTitle>EDA Context</DialogTitle>
        </DialogHeader>
        {isLoading ? (
          <div style={mutedStyle}>Loading EDA context...</div>
        ) : !eda ? (
          <div style={mutedStyle}>
            No EDA context — run EDA nodes upstream first
          </div>
        ) : (
          <div style={{ fontSize: 12, fontFamily: "'Inter', sans-serif" }}>
            {eda.distribution && (
              <Section title="Distribution">
                <StatsTable
                  data={eda.distribution}
                  columns={[
                    { key: "skewness", label: "Skewness" },
                    { key: "kurtosis", label: "Kurtosis" },
                  ]}
                />
              </Section>
            )}
            {eda.outliers && (
              <Section title="Outliers">
                <StatsTable
                  data={eda.outliers}
                  columns={[
                    { key: "outlier_pct", label: "Outlier %", fmt: pct },
                    { key: "z_max", label: "Z-max", fmt: num },
                  ]}
                />
              </Section>
            )}
            {eda.missing && (
              <Section title="Missing Values">
                <StatsTable
                  data={eda.missing}
                  columns={[
                    { key: "missing_pct", label: "Missing %", fmt: pct },
                    { key: "severity", label: "Severity" },
                  ]}
                />
              </Section>
            )}
            {eda.correlation && (
              <Section title="Correlation">
                {eda.correlation.high_pairs &&
                  eda.correlation.high_pairs.length > 0 && (
                    <div style={{ marginBottom: 8 }}>
                      <div style={subHeadingStyle}>High Correlation Pairs</div>
                      <table style={tableStyle}>
                        <thead>
                          <tr style={thRowStyle}>
                            <th style={thStyle}>Column 1</th>
                            <th style={thStyle}>Column 2</th>
                            <th style={thStyle}>Correlation</th>
                          </tr>
                        </thead>
                        <tbody>
                          {eda.correlation.high_pairs.map((p, i) => (
                            <tr key={i} style={trStyle}>
                              <td style={tdStyle}>{p.col1 ?? "—"}</td>
                              <td style={tdStyle}>{p.col2 ?? "—"}</td>
                              <td style={tdStyle}>
                                {num(p.correlation)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                {eda.correlation.target_correlations &&
                  Object.keys(eda.correlation.target_correlations).length >
                    0 && (
                    <div>
                      <div style={subHeadingStyle}>Target Correlations</div>
                      <table style={tableStyle}>
                        <thead>
                          <tr style={thRowStyle}>
                            <th style={thStyle}>Column</th>
                            <th style={thStyle}>Correlation</th>
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(
                            eda.correlation.target_correlations,
                          ).map(([col, val]) => (
                            <tr key={col} style={trStyle}>
                              <td style={tdStyle}>{col}</td>
                              <td style={tdStyle}>{num(val)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                {!eda.correlation.high_pairs &&
                  !eda.correlation.target_correlations && (
                    <RawJson data={eda.correlation} />
                  )}
              </Section>
            )}
            {/* Render any extra top-level keys not covered above */}
            {Object.entries(eda)
              .filter(
                ([k]) =>
                  !["distribution", "outliers", "missing", "correlation"].includes(k),
              )
              .map(([key, val]) => (
                <Section key={key} title={key}>
                  <RawJson data={val} />
                </Section>
              ))}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

// ── Helpers ──────────────────────────────────────────────────────

function pct(v: unknown): string {
  if (typeof v !== "number") return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function num(v: unknown): string {
  if (typeof v !== "number") return "—";
  return v.toFixed(3);
}

// ── Sub-components ──────────────────────────────────────────────

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          color: "var(--text-muted)",
          marginBottom: 6,
          padding: "0 4px",
        }}
      >
        {title}
      </div>
      {children}
    </div>
  );
}

interface ColDef {
  key: string;
  label: string;
  fmt?: (v: unknown) => string;
}

function StatsTable({
  data,
  columns,
}: {
  data: Record<string, ColumnStats>;
  columns: ColDef[];
}) {
  const entries = Object.entries(data);
  if (entries.length === 0) return <div style={mutedStyle}>No data</div>;

  return (
    <table style={tableStyle}>
      <thead>
        <tr style={thRowStyle}>
          <th style={thStyle}>Column</th>
          {columns.map((c) => (
            <th key={c.key} style={thStyle}>
              {c.label}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {entries.map(([col, stats]) => (
          <tr key={col} style={trStyle}>
            <td style={{ ...tdStyle, fontWeight: 600 }}>{col}</td>
            {columns.map((c) => (
              <td key={c.key} style={tdStyle}>
                {c.fmt ? c.fmt(stats[c.key]) : String(stats[c.key] ?? "—")}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RawJson({ data }: { data: unknown }) {
  return (
    <pre
      style={{
        fontSize: 11,
        fontFamily: "'JetBrains Mono', monospace",
        background: "var(--ghost-hover-bg)",
        padding: "8px 12px",
        borderRadius: 6,
        overflow: "auto",
        maxHeight: 300,
        margin: 0,
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
      }}
    >
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

// ── Styles ───────────────────────────────────────────────────────

const mutedStyle: React.CSSProperties = {
  padding: "12px 16px",
  color: "var(--text-muted)",
  fontSize: 12,
  fontFamily: "'Inter', sans-serif",
  lineHeight: 1.5,
};

const tableStyle: React.CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
  tableLayout: "auto",
};

const thRowStyle: React.CSSProperties = {
  borderBottom: "1px solid var(--border-default)",
  color: "var(--text-muted)",
  fontSize: 10,
  fontWeight: 600,
  textTransform: "uppercase",
  letterSpacing: "0.05em",
};

const thStyle: React.CSSProperties = {
  padding: "6px 8px 6px 4px",
  textAlign: "left",
  fontWeight: 600,
};

const trStyle: React.CSSProperties = {
  borderBottom: "1px solid var(--border-default)",
};

const tdStyle: React.CSSProperties = {
  padding: "4px 8px 4px 4px",
  verticalAlign: "middle",
  fontSize: 11,
};

const subHeadingStyle: React.CSSProperties = {
  fontSize: 10,
  fontWeight: 600,
  color: "var(--text-secondary)",
  marginBottom: 4,
  padding: "0 4px",
};
