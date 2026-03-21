import { useState, useCallback } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getMetadata, updateMetadata } from "../../lib/api";
import { AlertTriangle, ChevronRight, ChevronDown, Sparkles, Check } from "lucide-react";

// ── Types ────────────────────────────────────────────────────────

interface ColumnMeta {
  dtype: string;
  semantic_type: string;
  role: string;
  nullable?: boolean;
  unique_count?: number;
  unique_ratio?: number;
  null_pct?: number;
  sample_values?: unknown[];
  reasoning?: string;
  confidence?: number;
  decision?: string;
  evidence?: string[];
  alternatives_considered?: string[];
  suggested_action?: string;
}

type MetadataPayload = {
  columns: Record<string, ColumnMeta>;
  row_count?: number;
  generated_by?: string;
  node_id?: string;
  [key: string]: unknown;
};

const SEMANTIC_TYPES = [
  "continuous",
  "categorical",
  "ordinal",
  "binary",
  "identifier",
  "datetime",
  "text",
  "target",
] as const;

const ROLES = ["feature", "target", "identifier", "ignore"] as const;

// ── Component ────────────────────────────────────────────────────

interface SchemaEditorProps {
  pipelineId: string;
  nodeId: string;
  onOpenTerminal?: (nodeId: string) => void;
}

export default function SchemaEditor({
  pipelineId,
  nodeId,
  onOpenTerminal,
}: SchemaEditorProps) {
  const qc = useQueryClient();
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [savedCol, setSavedCol] = useState<string | null>(null);

  const { data: metadata, isLoading: loading } = useQuery({
    queryKey: ["metadata", pipelineId, nodeId],
    queryFn: async () => {
      const res = await getMetadata(pipelineId, nodeId);
      if (res.metadata && typeof res.metadata === "object" && "columns" in res.metadata) {
        return res.metadata as MetadataPayload;
      }
      return null;
    },
    enabled: !!pipelineId && !!nodeId,
  });

  const noData = !loading && !metadata;

  const handleChange = useCallback(
    async (colName: string, field: "semantic_type" | "role", value: string) => {
      if (!metadata) return;
      const updated: MetadataPayload = {
        ...metadata,
        columns: {
          ...metadata.columns,
          [colName]: { ...metadata.columns[colName], [field]: value },
        },
      };
      // Optimistic update
      qc.setQueryData(["metadata", pipelineId, nodeId], updated);
      try {
        await updateMetadata(pipelineId, nodeId, updated);
        setSavedCol(colName);
        setTimeout(() => setSavedCol(null), 1500);
      } catch {
        // Revert on failure — refetch
        qc.invalidateQueries({ queryKey: ["metadata", pipelineId, nodeId] });
      }
    },
    [metadata, pipelineId, nodeId, qc],
  );

  // ── Loading state ──
  if (loading) {
    return (
      <div style={{ padding: "12px 16px", color: "var(--text-muted)", fontSize: 12 }}>
        Loading schema...
      </div>
    );
  }

  // ── No metadata state ──
  if (noData || !metadata) {
    return (
      <div
        style={{
          padding: "12px 16px",
          color: "var(--text-muted)",
          fontSize: 12,
          fontFamily: "'Inter', sans-serif",
          lineHeight: 1.5,
        }}
      >
        Run this node, then use Pipeline CC{" "}
        <code
          style={{
            background: "var(--ghost-hover-bg)",
            padding: "1px 5px",
            borderRadius: 3,
            fontSize: 11,
          }}
        >
          /infer-schema
        </code>{" "}
        to generate metadata.
      </div>
    );
  }

  // ── Table ──
  const columns = Object.entries(metadata.columns);

  return (
    <div style={{ fontSize: 12, fontFamily: "'Inter', sans-serif" }}>
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          tableLayout: "fixed",
        }}
      >
        <thead>
          <tr
            style={{
              borderBottom: "1px solid var(--border-default)",
              color: "var(--text-muted)",
              fontSize: 10,
              fontWeight: 600,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
            }}
          >
            <th style={{ ...thStyle, width: "22%" }}>Column</th>
            <th style={{ ...thStyle, width: "12%" }}>Dtype</th>
            <th style={{ ...thStyle, width: "24%" }}>Semantic Type</th>
            <th style={{ ...thStyle, width: "20%" }}>Role</th>
            <th style={{ ...thStyle, width: "12%" }}>Reasoning</th>
            <th style={{ ...thStyle, width: "10%", textAlign: "center" }}></th>
          </tr>
        </thead>
        <tbody>
          {columns.map(([colName, col]) => {
            const isExpanded = expandedRow === colName;
            const isLowConfidence =
              typeof col.confidence === "number" && col.confidence < 0.7;
            const isSaved = savedCol === colName;

            return (
              <ColumnRow
                key={colName}
                colName={colName}
                col={col}
                isExpanded={isExpanded}
                isLowConfidence={isLowConfidence}
                isSaved={isSaved}
                onToggleExpand={() =>
                  setExpandedRow(isExpanded ? null : colName)
                }
                onSemanticChange={(v) =>
                  handleChange(colName, "semantic_type", v)
                }
                onRoleChange={(v) => handleChange(colName, "role", v)}
                onCCClick={
                  onOpenTerminal ? () => onOpenTerminal(nodeId) : undefined
                }
              />
            );
          })}
        </tbody>
      </table>
      {metadata.row_count != null && (
        <div
          style={{
            padding: "6px 16px 8px",
            color: "var(--text-muted)",
            fontSize: 10,
          }}
        >
          {metadata.row_count.toLocaleString()} rows
          {metadata.generated_by ? ` · generated by ${metadata.generated_by}` : ""}
        </div>
      )}
    </div>
  );
}

// ── Sub-components ───────────────────────────────────────────────

interface ColumnRowProps {
  colName: string;
  col: ColumnMeta;
  isExpanded: boolean;
  isLowConfidence: boolean;
  isSaved: boolean;
  onToggleExpand: () => void;
  onSemanticChange: (v: string) => void;
  onRoleChange: (v: string) => void;
  onCCClick?: () => void;
}

function ColumnRow({
  colName,
  col,
  isExpanded,
  isLowConfidence,
  isSaved,
  onToggleExpand,
  onSemanticChange,
  onRoleChange,
  onCCClick,
}: ColumnRowProps) {
  const hasReasoning =
    col.reasoning || col.decision || col.evidence?.length;

  return (
    <>
      <tr
        style={{
          borderBottom: isExpanded ? "none" : "1px solid var(--border-default)",
          background: isLowConfidence
            ? "rgba(234, 179, 8, 0.06)"
            : "transparent",
        }}
      >
        {/* Column name */}
        <td style={tdStyle}>
          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
            {isLowConfidence && (
              <span title="Low confidence — please verify this classification">
                <AlertTriangle
                  size={12}
                  style={{ color: "var(--warning-amber)", flexShrink: 0 }}
                />
              </span>
            )}
            <span
              style={{
                fontWeight: 600,
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: 11,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
              title={colName}
            >
              {colName}
            </span>
            {isSaved && (
              <Check
                size={12}
                style={{ color: "var(--success-green)", flexShrink: 0 }}
              />
            )}
          </div>
        </td>

        {/* Dtype */}
        <td style={{ ...tdStyle, color: "var(--text-muted)", fontSize: 11 }}>
          {col.dtype}
        </td>

        {/* Semantic type dropdown */}
        <td style={tdStyle}>
          <select
            value={col.semantic_type}
            onChange={(e) => onSemanticChange(e.target.value)}
            style={selectStyle}
          >
            {SEMANTIC_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </td>

        {/* Role dropdown */}
        <td style={tdStyle}>
          <select
            value={col.role}
            onChange={(e) => onRoleChange(e.target.value)}
            style={selectStyle}
          >
            {ROLES.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </td>

        {/* Reasoning expand toggle */}
        <td style={tdStyle}>
          {hasReasoning ? (
            <button
              onClick={onToggleExpand}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                color: "var(--text-muted)",
                display: "flex",
                alignItems: "center",
                gap: 2,
                padding: 0,
                fontSize: 11,
              }}
            >
              {isExpanded ? (
                <ChevronDown size={12} />
              ) : (
                <ChevronRight size={12} />
              )}
              <span>{isExpanded ? "hide" : "show"}</span>
            </button>
          ) : (
            <span style={{ color: "var(--text-muted)", fontSize: 10 }}>—</span>
          )}
        </td>

        {/* CC button */}
        <td style={{ ...tdStyle, textAlign: "center" }}>
          {onCCClick && (
            <button
              onClick={onCCClick}
              title="Ask Pipeline CC about this column"
              style={{
                background: "none",
                border: "1px solid var(--border-default)",
                borderRadius: 4,
                cursor: "pointer",
                color: "var(--text-muted)",
                width: 24,
                height: 24,
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                padding: 0,
              }}
              onMouseEnter={(e) => {
                (e.currentTarget as HTMLElement).style.background =
                  "var(--ghost-hover-bg)";
              }}
              onMouseLeave={(e) => {
                (e.currentTarget as HTMLElement).style.background = "transparent";
              }}
            >
              <Sparkles size={12} />
            </button>
          )}
        </td>
      </tr>

      {/* Expanded reasoning sub-row */}
      {isExpanded && hasReasoning && (
        <tr>
          <td
            colSpan={6}
            style={{
              padding: "8px 16px 10px",
              background: "var(--ghost-hover-bg)",
              borderBottom: "1px solid var(--border-default)",
              fontSize: 11,
              lineHeight: 1.6,
              color: "var(--text-secondary)",
            }}
          >
            {col.decision && (
              <div>
                <strong style={{ color: "var(--text-primary)" }}>Decision:</strong>{" "}
                {col.decision}
              </div>
            )}
            {col.reasoning && !col.decision && (
              <div>
                <strong style={{ color: "var(--text-primary)" }}>Reasoning:</strong>{" "}
                {col.reasoning}
              </div>
            )}
            {col.evidence && col.evidence.length > 0 && (
              <div style={{ marginTop: 2 }}>
                <strong style={{ color: "var(--text-primary)" }}>Evidence:</strong>{" "}
                {col.evidence.join("; ")}
              </div>
            )}
            {col.alternatives_considered &&
              col.alternatives_considered.length > 0 && (
                <div style={{ marginTop: 2 }}>
                  <strong style={{ color: "var(--text-primary)" }}>
                    Alternatives:
                  </strong>{" "}
                  {col.alternatives_considered.join(", ")}
                </div>
              )}
            {col.suggested_action && (
              <div style={{ marginTop: 2 }}>
                <strong style={{ color: "var(--text-primary)" }}>
                  Suggested action:
                </strong>{" "}
                {col.suggested_action}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

// ── Shared styles ────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  padding: "6px 8px 6px 16px",
  textAlign: "left",
  fontWeight: 600,
};

const tdStyle: React.CSSProperties = {
  padding: "5px 8px 5px 16px",
  verticalAlign: "middle",
};

const selectStyle: React.CSSProperties = {
  fontFamily: "'Inter', sans-serif",
  fontSize: 11,
  padding: "3px 6px",
  borderRadius: 4,
  border: "1px solid var(--border-default)",
  background: "var(--node-bg)",
  color: "var(--text-primary)",
  cursor: "pointer",
  width: "100%",
  maxWidth: 140,
};
