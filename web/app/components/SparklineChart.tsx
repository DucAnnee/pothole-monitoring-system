interface Props {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
}

const VB_W = 400;

export function SparklineChart({
  data,
  width,
  height = 48,
  color = "#1488DB",
}: Props) {
  if (!data.length) return null;

  const w = VB_W;
  const max = Math.max(...data, 1);
  const min = Math.min(...data);
  const range = max - min || 1;
  const pad = 4;

  const toX = (i: number) =>
    pad + (i / (data.length - 1)) * (w - pad * 2);
  const toY = (v: number) =>
    pad + ((max - v) / range) * (height - pad * 2);

  const points = data.map((v, i) => `${toX(i)},${toY(v)}`).join(" ");
  const areaPoints = `${toX(0)},${height} ${points} ${toX(data.length - 1)},${height}`;

  const lastX = toX(data.length - 1);
  const lastY = toY(data[data.length - 1]);

  return (
    <svg
      width={width ?? "100%"}
      height={height}
      viewBox={`0 0 ${w} ${height}`}
      preserveAspectRatio="none"
      style={{ display: "block" }}
    >
      <defs>
        <linearGradient id="spark-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity={0.3} />
          <stop offset="100%" stopColor={color} stopOpacity={0.02} />
        </linearGradient>
      </defs>
      <polygon points={areaPoints} fill="url(#spark-fill)" />
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      <circle cx={lastX} cy={lastY} r={3} fill={color} />
    </svg>
  );
}
