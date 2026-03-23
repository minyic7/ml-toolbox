import { useMemo, useState } from "react";

interface TrainingCurveProps {
  evalHistory: Record<string, number[]>;
  evalMetric: string;
  evalMetricDirection: string;
  bestIteration?: number; // 0-indexed
}

const CHART_W = 460;
const CHART_H = 180;
const PAD = { top: 10, right: 14, bottom: 28, left: 50 };
const INNER_W = CHART_W - PAD.left - PAD.right;
const INNER_H = CHART_H - PAD.top - PAD.bottom;

const COLORS: Record<string, string> = {
  train: "#6b7280",
  val: "#d97706",
};

const MAX_POINTS = 200;

/** Downsample an array to at most maxPts points, keeping first/last. */
function downsample(arr: number[], maxPts: number): { values: number[]; indices: number[] } {
  if (arr.length <= maxPts) {
    return { values: arr, indices: arr.map((_, i) => i) };
  }
  const step = (arr.length - 1) / (maxPts - 1);
  const values: number[] = [];
  const indices: number[] = [];
  for (let i = 0; i < maxPts; i++) {
    const idx = Math.round(i * step);
    values.push(arr[idx]);
    indices.push(idx);
  }
  return { values, indices };
}

export function TrainingCurve({
  evalHistory,
  evalMetric,
  evalMetricDirection,
  bestIteration,
}: TrainingCurveProps) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const { series, totalRounds, yMin, yMax, yTicks, xTicks } = useMemo(() => {
    const entries = Object.entries(evalHistory);
    if (entries.length === 0) return { series: [], totalRounds: 0, yMin: 0, yMax: 1, yTicks: [], xTicks: [] };

    const maxLen = Math.max(...entries.map(([, v]) => v.length));

    // Downsample all series
    const sampled = entries.map(([name, values]) => {
      const { values: dv, indices: di } = downsample(values, MAX_POINTS);
      return { name, values: dv, indices: di };
    });

    // Y range
    const allVals = sampled.flatMap((s) => s.values);
    let min = Math.min(...allVals);
    let max = Math.max(...allVals);
    const margin = (max - min) * 0.08 || 0.01;
    min -= margin;
    max += margin;

    // Y ticks (5 ticks)
    const yStep = (max - min) / 4;
    const yt = Array.from({ length: 5 }, (_, i) => min + i * yStep);

    // X ticks (up to 6 ticks)
    const xtCount = Math.min(6, maxLen);
    const xStep = Math.max(1, Math.floor((maxLen - 1) / (xtCount - 1)));
    const xt = Array.from({ length: xtCount }, (_, i) => Math.min(i * xStep, maxLen - 1));

    return { series: sampled, totalRounds: maxLen, yMin: min, yMax: max, yTicks: yt, xTicks: xt };
  }, [evalHistory]);

  if (series.length === 0 || totalRounds === 0) return null;

  const xScale = (round: number) => PAD.left + (round / Math.max(totalRounds - 1, 1)) * INNER_W;
  const yScale = (val: number) => PAD.top + INNER_H - ((val - yMin) / (yMax - yMin)) * INNER_H;

  // Build SVG paths
  const paths = series.map((s) => {
    const d = s.indices
      .map((origIdx, i) => `${i === 0 ? "M" : "L"}${xScale(origIdx).toFixed(1)},${yScale(s.values[i]).toFixed(1)}`)
      .join(" ");
    return { name: s.name, d, color: COLORS[s.name] ?? "#3b82f6" };
  });

  // Best iteration line
  const bestX = bestIteration != null ? xScale(bestIteration) : null;

  // Hover: find closest index from mouse position
  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const round = Math.round(((mouseX - PAD.left) / INNER_W) * (totalRounds - 1));
    setHoverIdx(Math.max(0, Math.min(totalRounds - 1, round)));
  };

  // Hover values
  const hoverValues: { name: string; value: number; color: string }[] = [];
  if (hoverIdx != null) {
    for (const [name, values] of Object.entries(evalHistory)) {
      if (hoverIdx < values.length) {
        hoverValues.push({ name, value: values[hoverIdx], color: COLORS[name] ?? "#3b82f6" });
      }
    }
  }

  return (
    <div style={{ marginBottom: 12 }}>
      {/* Legend */}
      <div style={{
        display: "flex",
        gap: 14,
        marginBottom: 4,
        fontSize: 10,
        fontFamily: "'Inter', sans-serif",
        color: "var(--text-secondary)",
      }}>
        {paths.map((p) => (
          <span key={p.name} style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
            <span style={{ width: 12, height: 2, backgroundColor: p.color, display: "inline-block" }} />
            {p.name}
          </span>
        ))}
        {bestIteration != null && (
          <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
            <span style={{ width: 12, height: 0, borderTop: "1.5px dashed #dc2626", display: "inline-block" }} />
            best ({bestIteration + 1})
          </span>
        )}
      </div>

      <svg
        width={CHART_W}
        height={CHART_H}
        style={{ display: "block", maxWidth: "100%" }}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {/* Grid lines */}
        {yTicks.map((y, i) => (
          <g key={i}>
            <line
              x1={PAD.left} x2={CHART_W - PAD.right}
              y1={yScale(y)} y2={yScale(y)}
              stroke="var(--border-default)" strokeWidth={0.5}
            />
            <text
              x={PAD.left - 6} y={yScale(y) + 3}
              textAnchor="end"
              fontSize={9}
              fontFamily="'JetBrains Mono', monospace"
              fill="var(--text-muted)"
            >
              {y.toFixed(3)}
            </text>
          </g>
        ))}

        {/* X axis labels */}
        {xTicks.map((round, i) => (
          <text
            key={i}
            x={xScale(round)} y={CHART_H - 4}
            textAnchor="middle"
            fontSize={9}
            fontFamily="'JetBrains Mono', monospace"
            fill="var(--text-muted)"
          >
            {round + 1}
          </text>
        ))}

        {/* Data lines */}
        {paths.map((p) => (
          <path
            key={p.name}
            d={p.d}
            fill="none"
            stroke={p.color}
            strokeWidth={1.5}
            strokeLinejoin="round"
          />
        ))}

        {/* Best iteration line */}
        {bestX != null && (
          <line
            x1={bestX} x2={bestX}
            y1={PAD.top} y2={PAD.top + INNER_H}
            stroke="#dc2626"
            strokeWidth={1}
            strokeDasharray="4,3"
          />
        )}

        {/* Hover crosshair */}
        {hoverIdx != null && (
          <>
            <line
              x1={xScale(hoverIdx)} x2={xScale(hoverIdx)}
              y1={PAD.top} y2={PAD.top + INNER_H}
              stroke="var(--text-muted)"
              strokeWidth={0.5}
              strokeDasharray="2,2"
            />
            {hoverValues.map((hv) => (
              <circle
                key={hv.name}
                cx={xScale(hoverIdx)}
                cy={yScale(hv.value)}
                r={3}
                fill={hv.color}
                stroke="var(--node-bg)"
                strokeWidth={1.5}
              />
            ))}
          </>
        )}
      </svg>

      {/* Hover tooltip */}
      {hoverIdx != null && hoverValues.length > 0 && (
        <div style={{
          fontSize: 10,
          fontFamily: "'JetBrains Mono', monospace",
          color: "var(--text-secondary)",
          marginTop: 2,
          display: "flex",
          gap: 12,
        }}>
          <span>round {hoverIdx + 1}</span>
          {hoverValues.map((hv) => (
            <span key={hv.name} style={{ color: hv.color }}>
              {hv.name}: {hv.value.toFixed(4)}
            </span>
          ))}
        </div>
      )}

      {/* X axis label */}
      <div style={{
        textAlign: "center",
        fontSize: 9,
        fontFamily: "'Inter', sans-serif",
        color: "var(--text-muted)",
        marginTop: 2,
      }}>
        {evalMetric} ({evalMetricDirection === "minimize" ? "lower is better" : "higher is better"})
      </div>
    </div>
  );
}
