import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import LinearProgress from "@mui/material/LinearProgress";
import Typography from "@mui/material/Typography";
import { useEffect } from "react";
import { useLoaderData, useRevalidator } from "react-router";
import type { Route } from "./+types/_app.health";
import { EdgeDevicesTable } from "~/components/EdgeDevicesTable";
import { StatusDot } from "~/components/StatusDot";
import { cached } from "~/lib/redis.server";
import { requireAuth } from "~/lib/session.server";
import { fetchHealthData } from "~/lib/health.server";

export const handle = { title: "System Health" };

export async function loader({ request }: Route.LoaderArgs) {
  await requireAuth(request);
  return cached("web:health:v1", 10, fetchHealthData);
}

export default function HealthPage() {
  const data = useLoaderData<typeof loader>();
  const { revalidate } = useRevalidator();

  useEffect(() => {
    const id = setInterval(revalidate, 30_000);
    return () => clearInterval(id);
  }, [revalidate]);

  const minioUsedPct = (data.minio.used_gb / data.minio.total_gb) * 100;

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      {/* Top 2-col row */}
      <Box sx={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 2 }}>
        {/* Kafka */}
        <Card>
          <CardContent>
            <Typography variant="subtitle2" mb={1.5}>Kafka Cluster</Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {data.kafka.brokers.map((b) => (
                <Box
                  key={b.name}
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    py: 0.5,
                    borderBottom: "1px solid #F0F0F0",
                  }}
                >
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <StatusDot color={b.active ? "#16A34A" : "#DC2626"} />
                    <Box>
                      <Typography variant="caption" fontWeight={600}>{b.name}</Typography>
                      <Typography variant="caption" color="text.secondary" display="block">{b.ip}</Typography>
                    </Box>
                  </Box>
                  <Box sx={{ textAlign: "right" }}>
                    <Chip label="Active" size="small" sx={{ bgcolor: "#F0FDF4", color: "#16A34A", fontSize: 10 }} />
                    <Typography variant="caption" color="text.disabled" display="block">
                      {new Date(b.last_heartbeat).toLocaleTimeString()}
                    </Typography>
                  </Box>
                </Box>
              ))}
            </Box>
          </CardContent>
        </Card>

        {/* MinIO + Polaris */}
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Card>
            <CardContent>
              <Typography variant="subtitle2" mb={1}>MinIO Object Storage</Typography>
              <Box sx={{ mb: 1 }}>
                <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.25 }}>
                  <Typography variant="caption" color="text.secondary">Storage Usage</Typography>
                  <Typography variant="caption" fontWeight={600}>
                    {data.minio.used_gb.toFixed(1)} / {data.minio.total_gb} GB
                  </Typography>
                </Box>
                <LinearProgress variant="determinate" value={minioUsedPct} sx={{ height: 6, borderRadius: 3 }} />
              </Box>
              <Box sx={{ display: "flex", gap: 1.5 }}>
                {Object.entries(data.minio.type_dist).map(([k, pct]) => (
                  <Box key={k} sx={{ flex: 1 }}>
                    <Typography variant="caption" color="text.secondary">{k.replace("_", " ")}</Typography>
                    <LinearProgress
                      variant="determinate"
                      value={pct}
                      sx={{ height: 4, borderRadius: 2, mt: 0.25 }}
                    />
                  </Box>
                ))}
              </Box>
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="subtitle2" mb={1}>Polaris Catalog</Typography>
              <Box sx={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 1, textAlign: "center" }}>
                {[
                  { label: "Catalogs", value: data.polaris.catalogs },
                  { label: "Tables", value: data.polaris.tables },
                  { label: "Q/min", value: data.polaris.queries_per_min },
                ].map(({ label, value }) => (
                  <Box key={label} sx={{ border: "1px solid #E2E8F0", borderRadius: 1.5, p: 1 }}>
                    <Typography variant="caption" color="text.secondary">{label}</Typography>
                    <Typography variant="body1" fontWeight={700}>{value}</Typography>
                  </Box>
                ))}
              </Box>
            </CardContent>
          </Card>
        </Box>
      </Box>

      {/* Microservices */}
      <Box>
        <Typography variant="subtitle2" mb={1.5}>Microservices</Typography>
        <Box sx={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 2 }}>
          {data.microservices.map((svc) => {
            const statusColor = svc.status === "healthy" ? "#16A34A" : svc.status === "warning" ? "#D97706" : "#8895A7";
            return (
              <Card key={svc.name}>
                <CardContent>
                  <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 1 }}>
                    <Typography variant="body2" fontWeight={600}>{svc.name}</Typography>
                    <Chip
                      label={svc.status}
                      size="small"
                      sx={{ color: statusColor, bgcolor: statusColor + "20", fontSize: 10, textTransform: "capitalize" }}
                    />
                  </Box>
                  <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                    <Box>
                      <Typography variant="caption" color="text.secondary">Uptime</Typography>
                      <Typography variant="caption" fontWeight={600} display="block">{svc.uptime}%</Typography>
                    </Box>
                    <Box>
                      <Typography variant="caption" color="text.secondary">Latency</Typography>
                      <Typography variant="caption" fontWeight={600} display="block">{svc.latency_ms}ms</Typography>
                    </Box>
                    <Box>
                      <Typography variant="caption" color="text.secondary">Last check</Typography>
                      <Typography variant="caption" fontWeight={600} display="block">
                        {new Date(svc.last_check).toLocaleTimeString()}
                      </Typography>
                    </Box>
                  </Box>
                </CardContent>
              </Card>
            );
          })}
        </Box>
      </Box>

      {/* Edge Devices */}
      <Card>
        <CardContent>
          <Typography variant="subtitle2" mb={1.5}>Edge Devices</Typography>
          <EdgeDevicesTable devices={data.edge_devices} />
        </CardContent>
      </Card>
    </Box>
  );
}
