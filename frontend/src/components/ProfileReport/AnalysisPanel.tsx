import type { CcAnalysis } from "../../lib/types";
import { WarningList } from "./WarningList";

interface AnalysisPanelProps {
  analysis: CcAnalysis;
}

const SECTION_HEADER: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 600,
  fontFamily: "'Inter', sans-serif",
  textTransform: "uppercase",
  color: "var(--text-secondary)",
  borderBottom: "1px solid var(--border-default)",
  paddingBottom: 6,
  marginBottom: 8,
};

const BULLET_STYLE: React.CSSProperties = {
  fontSize: 11,
  fontFamily: "'Inter', sans-serif",
  color: "var(--text-primary)",
  lineHeight: 1.5,
  paddingLeft: 4,
};

export function AnalysisPanel({ analysis }: AnalysisPanelProps) {
  const { findings, warnings, suggestions } = analysis;
  const hasFindings = findings && findings.length > 0;
  const hasWarnings = warnings && warnings.length > 0;
  const hasSuggestions = suggestions && suggestions.length > 0;

  if (!hasFindings && !hasWarnings && !hasSuggestions) return null;

  return (
    <div
      style={{
        padding: 12,
        borderTop: "1px solid var(--border-default)",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          marginBottom: 12,
        }}
      >
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            fontFamily: "'Inter', sans-serif",
            textTransform: "uppercase",
            letterSpacing: "0.05em",
            padding: "2px 8px",
            borderRadius: 9999,
            background: "var(--ghost-hover-bg, rgba(0,0,0,0.04))",
            color: "var(--text-muted)",
          }}
        >
          AI Analysis
        </span>
      </div>

      {hasFindings && (
        <div style={{ marginBottom: 12 }}>
          <div style={SECTION_HEADER}>Key Findings</div>
          <ul style={{ margin: 0, paddingLeft: 16 }}>
            {findings.map((f, i) => (
              <li key={i} style={BULLET_STYLE}>
                {f}
              </li>
            ))}
          </ul>
        </div>
      )}

      {hasWarnings && (
        <WarningList
          warnings={warnings.map((w) => ({
            type: w.type,
            column: w.column ?? undefined,
            message: w.message,
          }))}
          source="ai"
        />
      )}

      {hasSuggestions && (
        <div style={{ marginTop: 12 }}>
          <div style={SECTION_HEADER}>Suggested Next Steps</div>
          <ul style={{ margin: 0, paddingLeft: 16 }}>
            {suggestions.map((s, i) => (
              <li key={i} style={BULLET_STYLE}>
                {s}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
