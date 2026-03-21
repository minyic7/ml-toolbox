import { MetricsDisplay } from "../RightPanel/MetricsDisplay";
import { DistributionReport } from "./DistributionReport";
import { MissingReport } from "./MissingReport";
import { CorrelationReport } from "./CorrelationReport";
import { OutlierReport } from "./OutlierReport";

interface ProfileReportProps {
  data: Record<string, unknown>;
}

export function ProfileReport({ data }: ProfileReportProps) {
  switch (data.report_type) {
    case "distribution_profile":
      return <DistributionReport data={data} />;
    case "missing_analysis":
      return <MissingReport data={data} />;
    case "correlation_matrix":
      return <CorrelationReport data={data} />;
    case "outlier_detection":
      return <OutlierReport data={data} />;
    default:
      return <MetricsDisplay data={data} />;
  }
}
