import { useState } from "react";
import type { CcAnalysis } from "../../lib/types";

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
  const [collapsed, setCollapsed] = useState(false);
  const { summary, findings, suggestions } = analysis;
  const hasFindings = findings && findings.length > 0;
  const hasSuggestions = suggestions && suggestions.length > 0;

  if (!summary && !hasFindings && !hasSuggestions) return null;

  return (
    <div
      style={{
        marginTop: 16,
        border: "1px solid var(--border-default)",
        borderRadius: 6,
        overflow: "hidden",
      }}
    >
      <button
        onClick={() => setCollapsed((c) => !c)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          width: "100%",
          padding: "8px 12px",
          border: "none",
          cursor: "pointer",
          backgroundColor: "var(--output-thead-bg, #f5f5f5)",
          textAlign: "left",
        }}
      >
        <span style={{ fontSize: 8, color: "var(--text-secondary)" }}>
          {collapsed ? "\u25b6" : "\u25bc"}
        </span>
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
      </button>

      {!collapsed && (
        <div style={{ padding: 12 }}>
          {summary && (
            <p
              style={{
                fontSize: 13,
                fontFamily: "'Inter', sans-serif",
                color: "var(--text-primary)",
                marginTop: 0,
                marginBottom: 8,
                lineHeight: 1.4,
              }}
            >
              {summary}
            </p>
          )}

          {hasFindings && (
            <div style={{ marginBottom: 8 }}>
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

          {hasSuggestions && (
            <div style={{ marginBottom: 0 }}>
              <div style={SECTION_HEADER}>Next Steps</div>
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
      )}
    </div>
  );
}
