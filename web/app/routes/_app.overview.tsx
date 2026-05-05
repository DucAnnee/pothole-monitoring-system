import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import LinearProgress from "@mui/material/LinearProgress";
import Typography from "@mui/material/Typography";
import { TrendingDown, TrendingUp } from "lucide-react";
import { useEffect } from "react";
import { useLoaderData, useNavigate, useRevalidator } from "react-router";
import type { Route } from "./+types/_app.overview";
import { SeverityBadge } from "~/components/SeverityBadge";
import { SparklineChart } from "~/components/SparklineChart";
import { SEVERITY } from "~/constants/severity";
import { cached } from "~/lib/redis.server";
import { requireAuth } from "~/lib/session.server";
import { emptySummaryData, querySummary } from "~/lib/postgis.server";

export const handle = { title: "Executive Overview" };

export async function loader({ request }: Route.LoaderArgs) {
  await requireAuth(request);
  return cached("web:summary:v1", 60, querySummary).catch((error) => {
    console.warn("[PostGIS] summary unavailable:", error instanceof Error ? error.message : error);
    return emptySummaryData();
  });
}

const SEVERITY_COLORS: Record<string, string> = {
  CRITICAL: SEVERITY.critical.color,
  HIGH: SEVERITY.high.color,
  MODERATE: SEVERITY.moderate.color,
  MINOR: SEVERITY.minor.color,
};

function SeverityDonut({ dist }: { dist: Record<string, number> }) {
  const total = Object.values(dist).reduce((s, v) => s + v, 0) || 1;
  const entries = Object.entries(dist).filter(([, v]) => v > 0);

  const r = 60;
  const cx = 80;
  const cy = 80;
  const circumference = 2 * Math.PI * r;

  let offset = 0;
  const slices = entries.map(([key, val]) => {
    const pct = val / total;
    const dash = pct * circumference;
    const gap = circumference - dash;
    const slice = { key, val, pct, dash, gap, offset, color: SEVERITY_COLORS[key] ?? "#ccc" };
    offset += dash;
    return slice;
  });

  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 3 }}>
      <svg width={160} height={160} viewBox="0 0 160 160">
        {slices.map((s) => (
          <circle
            key={s.key}
            cx={cx}
            cy={cy}
            r={r}
            fill="none"
            stroke={s.color}
            strokeWidth={20}
            strokeDasharray={`${s.dash} ${s.gap}`}
            strokeDashoffset={-s.offset + circumference / 4}
            style={{ transform: "rotate(-90deg)", transformOrigin: `${cx}px ${cy}px` }}
          />
        ))}
        <text x={cx} y={cy - 6} textAnchor="middle" fontSize="22" fontWeight="700" fill="#1A2332">{total}</text>
        <text x={cx} y={cy + 14} textAnchor="middle" fontSize="11" fill="#5A6B7F">Active</text>
      </svg>
      <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
        {entries.map(([key, val]) => (
          <Box key={key} sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Box sx={{ width: 10, height: 10, borderRadius: "50%", bgcolor: SEVERITY_COLORS[key] }} />
            <Typography variant="caption" color="text.secondary">{key}</Typography>
            <Typography variant="caption" fontWeight={600}>{val}</Typography>
          </Box>
        ))}
      </Box>
    </Box>
  );
}

export default function OverviewPage() {
  const data = useLoaderData<typeof loader>();
  const { revalidate } = useRevalidator();
  const navigate = useNavigate();

  useEffect(() => {
    const id = setInterval(revalidate, 60_000);
    return () => clearInterval(id);
  }, [revalidate]);

  const sparkData = data.activePotholesLast30Days.map((d) => d.count);
  const todayDiff = data.activePotholes.trend.today.count - (data.activePotholes.trend.today.count > 0 ? 1 : 0);

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      {/* KPI row */}
      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 2 }}>
        {[
          {
            label: "Total Active",
            value: data.activePotholes.count,
            sub: data.activePotholes.trend.today.comparison,
            up: true,
          },
          {
            label: "New Today",
            value: data.activePotholes.trend.today.count,
            sub: data.activePotholes.trend.today.comparison,
            up: todayDiff >= 0,
          },
          {
            label: "Resolved This Week",
            value: data.statusChanges.inProgressToFixed.thisWeek,
            sub: data.statusChanges.inProgressToFixed.comparison,
            up: data.statusChanges.inProgressToFixed.comparison.startsWith("+"),
          },
          {
            label: "Avg Severity",
            value: data.averageSeverity.toFixed(1),
            sub: "out of 10",
            up: null,
          },
          {
            label: "In Progress",
            value: data.inProgress,
            sub: data.statusChanges.reportedToInProgress.comparison,
            up: true,
          },
        ].map(({ label, value, sub, up }) => (
          <Card key={label}>
            <CardContent>
              <Typography variant="caption" color="text.secondary">{label}</Typography>
              <Typography variant="h4" fontWeight={700} mt={0.5}>{value}</Typography>
              <Box sx={{ display: "flex", alignItems: "center", gap: 0.5, mt: 0.5 }}>
                {up !== null && (
                  up
                    ? <TrendingUp size={13} color="#16A34A" />
                    : <TrendingDown size={13} color="#DC2626" />
                )}
                <Typography variant="caption" color="text.secondary">{sub}</Typography>
              </Box>
            </CardContent>
          </Card>
        ))}
      </Box>

      {/* Sparkline + donut row */}
      <Box sx={{ display: "grid", gridTemplateColumns: "1fr auto", gap: 2 }}>
        <Card>
          <CardContent>
            <Typography variant="subtitle2" mb={1}>Active Potholes — Last 30 Days</Typography>
            <SparklineChart data={sparkData} width={undefined as unknown as number} height={72} color="#1488DB" />
          </CardContent>
        </Card>
        <Card>
          <CardContent>
            <Typography variant="subtitle2" mb={1}>Severity Distribution</Typography>
            <SeverityDonut dist={data.severityDistribution} />
          </CardContent>
        </Card>
      </Box>

      {/* Bottom 3-column row */}
      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 2 }}>
        {/* Recent Critical */}
        <Card>
          <CardContent>
            <Typography variant="subtitle2" mb={1.5}>Recent Critical Detections</Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {data.recentCritical.length === 0 && (
                <Typography variant="body2" color="text.secondary">No critical potholes</Typography>
              )}
              {data.recentCritical.map((p) => (
                <Box
                  key={p.pothole_id}
                  onClick={() => navigate("/map")}
                  sx={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    p: 1,
                    borderRadius: 1,
                    border: "1px solid #E2E8F0",
                    cursor: "pointer",
                    "&:hover": { bgcolor: "#F5F7FA" },
                  }}
                >
                  <Box>
                    <Typography variant="caption" fontWeight={600}>{p.pothole_id.slice(0, 12)}…</Typography>
                    <Typography variant="caption" color="text.secondary" display="block">{p.district}</Typography>
                  </Box>
                  <SeverityBadge level={p.severity_level} />
                </Box>
              ))}
            </Box>
          </CardContent>
        </Card>

        {/* Top Districts */}
        <Card>
          <CardContent>
            <Typography variant="subtitle2" mb={1.5}>Top Affected Districts</Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
              {data.topDistricts.map(({ district, count }) => {
                const max = data.topDistricts[0]?.count ?? 1;
                return (
                  <Box key={district}>
                    <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.25 }}>
                      <Typography variant="caption">{district}</Typography>
                      <Typography variant="caption" fontWeight={600}>{count}</Typography>
                    </Box>
                    <LinearProgress
                      variant="determinate"
                      value={(count / max) * 100}
                      sx={{ height: 6, borderRadius: 3 }}
                    />
                  </Box>
                );
              })}
            </Box>
          </CardContent>
        </Card>

        {/* Operator Queue */}
        <Card>
          <CardContent>
            <Typography variant="subtitle2" mb={1.5}>Operator Action Queue</Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {data.recentCritical.slice(0, 3).map((p) => (
                <Box
                  key={p.pothole_id}
                  sx={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    p: 1,
                    borderRadius: 1,
                    border: "1px solid #E2E8F0",
                  }}
                >
                  <Box>
                    <Typography variant="caption" fontWeight={600}>{p.pothole_id.slice(0, 12)}…</Typography>
                    <Typography variant="caption" color="text.secondary" display="block">{p.district}</Typography>
                  </Box>
                  <Button size="small" variant="outlined" sx={{ minWidth: 40, px: 1, py: 0.25, fontSize: 11 }}>
                    Act
                  </Button>
                </Box>
              ))}
              {data.recentCritical.length === 0 && (
                <Typography variant="body2" color="text.secondary">Queue empty</Typography>
              )}
            </Box>
          </CardContent>
        </Card>
      </Box>
    </Box>
  );
}
