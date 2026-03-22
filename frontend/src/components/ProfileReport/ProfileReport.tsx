import React from "react";
import type { CcAnalysis } from "../../lib/types";
import { MetricsDisplay } from "../RightPanel/MetricsDisplay";
import { DistributionReport } from "./DistributionReport";
import { MissingReport } from "./MissingReport";
import { CorrelationReport } from "./CorrelationReport";
import { OutlierReport } from "./OutlierReport";
import { RocPrReport } from "./RocPrReport";

interface ProfileReportProps {
  data: Record<string, unknown>;
  analysis?: CcAnalysis | null;
}

export function ProfileReportFallback({ data, error }: { data: Record<string, unknown>; error: Error }) {
  return (
    <div style={{ padding: 12 }}>
      <div style={{
        padding: '8px 12px',
        borderRadius: 6,
        background: 'var(--error-bg, #fef2f2)',
        border: '1px solid var(--error-red)',
        marginBottom: 12,
        fontSize: 12,
        color: 'var(--error-red)',
      }}>
        Report rendering failed: {error.message}
      </div>
      <pre style={{
        fontSize: 11,
        fontFamily: "'JetBrains Mono', monospace",
        background: 'var(--ghost-hover-bg)',
        padding: 12,
        borderRadius: 6,
        overflow: 'auto',
        maxHeight: 400,
        whiteSpace: 'pre-wrap',
        color: 'var(--text-secondary)',
      }}>
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}

export class ProfileReportBoundary extends React.Component<
  { data: Record<string, unknown>; children: React.ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  render() {
    if (this.state.error) {
      return <ProfileReportFallback data={this.props.data} error={this.state.error} />;
    }
    return this.props.children;
  }
}

export function ProfileReport({ data, analysis }: ProfileReportProps) {
  switch (data.report_type) {
    case "distribution_profile":
      return <DistributionReport data={data} analysis={analysis} />;
    case "missing_analysis":
      return <MissingReport data={data} analysis={analysis} />;
    case "correlation_matrix":
      return <CorrelationReport data={data} analysis={analysis} />;
    case "outlier_detection":
      return <OutlierReport data={data} analysis={analysis} />;
    case "roc_pr_curves":
      return <RocPrReport data={data} analysis={analysis} />;
    default:
      return <MetricsDisplay data={data} />;
  }
}
