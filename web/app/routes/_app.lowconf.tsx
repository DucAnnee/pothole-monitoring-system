import type { Route } from "./+types/_app.lowconf";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import Slider from "@mui/material/Slider";
import Typography from "@mui/material/Typography";
import { Link, useLoaderData } from "react-router";
import { useState } from "react";
import { SEVERITY } from "~/constants/severity";
import { queryReviewQueue } from "~/lib/postgis.server";

export const handle = { title: "Low Confidence" };

export async function loader(_args: Route.LoaderArgs) {
  const items = await queryReviewQueue().catch((error) => {
    console.warn(
      "[PostGIS] review queue unavailable:",
      error instanceof Error ? error.message : error,
    );
    return [];
  });
  return { items };
}

const SEVERITY_KEYS = ["critical", "high", "moderate", "minor"] as const;
const TASK_STATUSES = ["pending", "in_progress"] as const;

export default function LowConfPage() {
  const { items } = useLoaderData<typeof loader>();
  const [confRange, setConfRange] = useState<number[]>([0, 60]);
  const [activeSeverities, setActiveSeverities] = useState<Set<string>>(new Set());
  const [activeStatuses, setActiveStatuses] = useState<Set<string>>(new Set());

  function toggleSeverity(k: string) {
    setActiveSeverities((prev) => {
      const next = new Set(prev);
      next.has(k) ? next.delete(k) : next.add(k);
      return next;
    });
  }

  function toggleStatus(k: string) {
    setActiveStatuses((prev) => {
      const next = new Set(prev);
      next.has(k) ? next.delete(k) : next.add(k);
      return next;
    });
  }

  const filtered = items.filter(
    (d) =>
      d.confidencePercent >= confRange[0] &&
      d.confidencePercent <= confRange[1] &&
      (activeSeverities.size === 0 || activeSeverities.has(d.severity)) &&
      (activeStatuses.size === 0 || activeStatuses.has(d.taskStatus)),
  );

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <Box
        sx={{
          p: 2,
          border: "1px solid #E2E8F0",
          borderRadius: 2,
          bgcolor: "background.paper",
          display: "flex",
          alignItems: "center",
          gap: 3,
          flexWrap: "wrap",
        }}
      >
        <Box sx={{ width: 240 }}>
          <Typography variant="caption" color="text.secondary">
            Confidence: {confRange[0]}% - {confRange[1]}%
          </Typography>
          <Slider
            value={confRange}
            onChange={(_, v) => setConfRange(v as number[])}
            min={0}
            max={100}
            size="small"
          />
        </Box>
        <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
          {SEVERITY_KEYS.map((k) => (
            <Chip
              key={k}
              label={SEVERITY[k].label}
              size="small"
              onClick={() => toggleSeverity(k)}
              sx={{
                color: activeSeverities.has(k) ? SEVERITY[k].color : "text.secondary",
                bgcolor: activeSeverities.has(k) ? SEVERITY[k].bg : undefined,
                border: `1px solid ${activeSeverities.has(k) ? SEVERITY[k].color : "#E2E8F0"}`,
              }}
            />
          ))}
        </Box>
        <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
          {TASK_STATUSES.map((status) => (
            <Chip
              key={status}
              label={status.replace("_", " ")}
              size="small"
              onClick={() => toggleStatus(status)}
              sx={{
                textTransform: "capitalize",
                border: `1px solid ${activeStatuses.has(status) ? "#1488DB" : "#E2E8F0"}`,
                color: activeStatuses.has(status) ? "#1488DB" : "text.secondary",
              }}
            />
          ))}
        </Box>
        <Typography variant="caption" color="text.secondary">
          {filtered.length} results
        </Typography>
      </Box>

      {filtered.length === 0 && (
        <Box sx={{ p: 4, border: "1px solid #E2E8F0", borderRadius: 2, bgcolor: "background.paper" }}>
          <Typography variant="body2" color="text.secondary">
            No low-confidence detections match the current filters.
          </Typography>
        </Box>
      )}

      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px,1fr))", gap: 2 }}>
        {filtered.map((d) => (
          <Card key={d.reviewTaskId}>
            <CardContent>
              <Box sx={{ display: "flex", gap: 1, mb: 1.5 }}>
                {d.rawImageObjectKey ? (
                  <Box
                    component="img"
                    src={`/api/image/proxy?path=${encodeURIComponent(d.rawImageObjectKey)}`}
                    alt={`Raw evidence for ${d.defectId}`}
                    sx={{
                      flex: 1,
                      width: "100%",
                      height: 120,
                      objectFit: "cover",
                      bgcolor: "#F0F0F0",
                      borderRadius: 1,
                    }}
                  />
                ) : (
                  <Box
                    sx={{
                      flex: 1,
                      height: 120,
                      bgcolor: "#F0F0F0",
                      borderRadius: 1,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                    }}
                  >
                    <Typography variant="caption" color="text.disabled">
                      Raw
                    </Typography>
                  </Box>
                )}
              </Box>

              <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 0.5 }}>
                <Box>
                  <Typography variant="body2" fontWeight={600}>
                    {d.confidencePercent}% confidence
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {d.location}
                  </Typography>
                </Box>
                <Chip
                  label={SEVERITY[d.severity].label}
                  size="small"
                  sx={{ color: SEVERITY[d.severity].color, bgcolor: SEVERITY[d.severity].bg }}
                />
              </Box>

              <Typography variant="caption" color="text.secondary" display="block">
                {d.lastSeenAt ?? "Unknown time"}
              </Typography>
              <Typography variant="caption" color="warning.main" display="block" mt={0.5}>
                Reason: {d.reason}
              </Typography>
              <Chip label={d.taskStatus.replace("_", " ")} size="small" sx={{ mt: 1, mr: 1, textTransform: "capitalize" }} />
              <Button
                component={Link}
                to={`/annotation?task=${encodeURIComponent(d.reviewTaskId)}`}
                size="small"
                variant="outlined"
                sx={{ mt: 1 }}
              >
                Review
              </Button>
            </CardContent>
          </Card>
        ))}
      </Box>
    </Box>
  );
}
