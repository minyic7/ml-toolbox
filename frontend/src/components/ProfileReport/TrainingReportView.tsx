import { SummaryCards } from "./SummaryCards";
import { TrainingCurve } from "./TrainingCurve";

interface TrainingReportViewProps {
  data: Record<string, unknown>;
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
  marginTop: 16,
};

const BAR_COLOR = "rgba(180, 160, 130, 0.7)";
const MAX_FEATURES = 15;

export function TrainingReportView({ data }: TrainingReportViewProps) {
  const modelType = (data.model_type as string) ?? "—";
  const taskType = (data.task_type as string) ?? "—";
  const targetCol = (data.target_column as string) ?? "—";
  const featureCols = (data.feature_columns as string[]) ?? [];
  const sampleCounts = (data.sample_counts as Record<string, number>) ?? {};
  const params = (data.params as Record<string, unknown>) ?? {};
  const featureImportances = (data.feature_importances as { feature: string; importance: number }[]) ?? [];
  const importanceType = (data.importance_type as string) ?? "";
  const xgbVersion = data.xgb_version as string | undefined;

  // Early stopping info
  const bestIteration = data.best_iteration as number | undefined;
  const nEstimatorsUsed = data.n_estimators_used as number | undefined;
  const bestScore = data.best_score as number | undefined;
  const evalMetric = (data.eval_metric as string) ?? "";
  const evalMetricDirection = (data.eval_metric_direction as string) ?? "minimize";
  const evalHistory = data.eval_history as Record<string, number[]> | undefined;

  // Classification info
  const classes = data.classes as unknown[] | undefined;
  const targetDist = data.target_distribution as Record<string, number> | undefined;

  // Summary cards
  const totalSamples = Object.values(sampleCounts).reduce((a, b) => a + b, 0);
  const splitStr = Object.entries(sampleCounts).map(([, n]) => n.toLocaleString()).join(" / ");

  const summaryItems: { label: string; value: string }[] = [
    {
      label: `${modelType} · ${taskType}`,
      value: `${featureCols.length} features`,
    },
    {
      label: `samples (${Object.keys(sampleCounts).join("/")})`,
      value: splitStr,
    },
  ];

  if (nEstimatorsUsed != null) {
    const maxEst = (params.n_estimators as number) ?? 0;
    summaryItems.push({
      label: bestScore != null
        ? `best ${evalMetric}: ${bestScore.toFixed(4)}`
        : "early stopping",
      value: `${nEstimatorsUsed} / ${maxEst} rounds`,
    });
  }

  if (xgbVersion) {
    summaryItems.push({
      label: "xgboost version",
      value: xgbVersion,
    });
  }

  // Params to display (filter out uninteresting defaults)
  const paramEntries = Object.entries(params).filter(
    ([, v]) => v !== "" && v !== null && v !== undefined,
  );

  // Feature importances
  const visibleFeatures = featureImportances.slice(0, MAX_FEATURES);
  const hiddenFeatures = featureImportances.slice(MAX_FEATURES);
  const hiddenShare = hiddenFeatures.reduce((s, f) => s + f.importance, 0);
  const maxImp = visibleFeatures.length > 0 ? visibleFeatures[0].importance : 1;

  return (
    <div style={{ padding: 12 }}>
      <SummaryCards items={summaryItems} />

      {/* Training curve */}
      {evalHistory && Object.keys(evalHistory).length > 0 && (
        <>
          <div style={SECTION_HEADER}>
            Training Curve · {evalMetric}
          </div>
          <TrainingCurve
            evalHistory={evalHistory}
            evalMetric={evalMetric}
            evalMetricDirection={evalMetricDirection}
            bestIteration={bestIteration}
          />
        </>
      )}

      {/* Hyperparameters */}
      {paramEntries.length > 0 && (
        <>
          <div style={SECTION_HEADER}>Hyperparameters</div>
          <div style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "1px",
            border: "1px solid var(--border-default)",
            borderRadius: 6,
            overflow: "hidden",
            marginBottom: 12,
            fontSize: 11,
          }}>
            {paramEntries.map(([key, val]) => (
              <div key={key} style={{
                display: "flex",
                justifyContent: "space-between",
                padding: "5px 10px",
                background: "var(--ghost-hover-bg, rgba(0,0,0,0.02))",
              }}>
                <span style={{
                  fontFamily: "'Inter', sans-serif",
                  color: "var(--text-secondary)",
                  fontSize: 10,
                }}>
                  {key}
                </span>
                <span style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  color: "var(--text-primary)",
                  fontWeight: 500,
                }}>
                  {typeof val === "boolean" ? (val ? "on" : "off") : String(val)}
                </span>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Feature importances */}
      {visibleFeatures.length > 0 && (
        <>
          <div style={SECTION_HEADER}>
            Feature Importances{importanceType ? ` · ${importanceType}` : ""}
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2, marginBottom: 4 }}>
            {visibleFeatures.map((f) => {
              const barWidth = maxImp > 0 ? (f.importance / maxImp) * 100 : 0;
              const pct = (f.importance * 100).toFixed(1);
              return (
                <div
                  key={f.feature}
                  style={{ display: "flex", alignItems: "center", gap: 6, height: 20 }}
                  title={`${f.feature}: ${pct}%`}
                >
                  <span style={{
                    width: 100,
                    fontSize: 10,
                    fontFamily: "'JetBrains Mono', monospace",
                    color: "var(--text-primary)",
                    textAlign: "right",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    flexShrink: 0,
                  }}>
                    {f.feature}
                  </span>
                  <div style={{ flex: 1, height: 14 }}>
                    <div style={{
                      width: `${barWidth}%`,
                      height: "100%",
                      backgroundColor: BAR_COLOR,
                      borderRadius: 2,
                      minWidth: barWidth > 0 ? 2 : 0,
                    }} />
                  </div>
                  <span style={{
                    width: 40,
                    fontSize: 10,
                    fontFamily: "'JetBrains Mono', monospace",
                    color: "var(--text-muted)",
                    textAlign: "right",
                    flexShrink: 0,
                  }}>
                    {pct}%
                  </span>
                </div>
              );
            })}
          </div>
          {hiddenFeatures.length > 0 && (
            <div style={{
              fontSize: 10,
              fontFamily: "'Inter', sans-serif",
              color: "var(--text-muted)",
              textAlign: "center",
              padding: "4px 0 8px",
            }}>
              … and {hiddenFeatures.length} more (total {(hiddenShare * 100).toFixed(1)}%)
            </div>
          )}
        </>
      )}

      {/* Class distribution */}
      {targetDist && classes && (
        <>
          <div style={SECTION_HEADER}>Target Distribution</div>
          <div style={{
            display: "flex",
            gap: 8,
            marginBottom: 12,
            fontSize: 11,
            fontFamily: "'JetBrains Mono', monospace",
          }}>
            {Object.entries(targetDist).map(([cls, count]) => {
              const pct = totalSamples > 0 ? ((count / (sampleCounts.train ?? totalSamples)) * 100).toFixed(1) : "0";
              return (
                <div key={cls} style={{
                  flex: 1,
                  padding: "6px 10px",
                  border: "1px solid var(--border-default)",
                  borderRadius: 6,
                  textAlign: "center",
                }}>
                  <div style={{ fontSize: 14, fontWeight: 600 }}>{pct}%</div>
                  <div style={{ fontSize: 9, color: "var(--text-muted)", fontFamily: "'Inter', sans-serif" }}>
                    label {cls} · n={count.toLocaleString()}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Target column + feature count footer */}
      <div style={{
        fontSize: 9,
        fontFamily: "'Inter', sans-serif",
        color: "var(--text-muted)",
        marginTop: 8,
      }}>
        target: {targetCol} · {featureCols.length} features
      </div>
    </div>
  );
}
