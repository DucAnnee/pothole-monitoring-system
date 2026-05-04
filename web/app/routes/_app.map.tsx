import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import CircularProgress from "@mui/material/CircularProgress";
import Dialog from "@mui/material/Dialog";
import DialogContent from "@mui/material/DialogContent";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import LinearProgress from "@mui/material/LinearProgress";
import Typography from "@mui/material/Typography";
import { CheckCircle, Clock, MapPin, X } from "lucide-react";
import { lazy, Suspense, useState } from "react";
import { useLoaderData, useNavigate } from "react-router";
import type { Route } from "./+types/_app.map";
import { ClientOnly } from "~/components/ClientOnly";

const PotholeMap = lazy(() =>
  import("~/components/PotholeMap").then((m) => ({ default: m.PotholeMap }))
);
import { SeverityBadge } from "~/components/SeverityBadge";
import { StatusDot } from "~/components/StatusDot";
import { SEVERITY, normalizeSeverity } from "~/constants/severity";
import { cached } from "~/lib/redis.server";
import { requireAuth } from "~/lib/session.server";
import type { PotholeDetail, PotholeMarker } from "~/lib/trino.server";
import { queryMapPotholes } from "~/lib/trino.server";

export const handle = { title: "Pothole Map" };

export async function loader({ request }: Route.LoaderArgs) {
  await requireAuth(request);
  const markers = await cached("web:mapdata:recent", 30, queryMapPotholes);
  return { markers };
}

const STATUS_COLORS: Record<string, string> = {
  reported: "#D97706",
  in_progress: "#1488DB",
  fixed: "#16A34A",
};

const FILTER_KEYS = ["critical", "high", "moderate", "minor"] as const;

function DetailPanel({
  pothole,
  detail,
  imageUrls,
  onClose,
}: {
  pothole: PotholeMarker;
  detail: PotholeDetail | null;
  imageUrls: string[];
  onClose: () => void;
}) {
  const navigate = useNavigate();
  const [lightbox, setLightbox] = useState<string | null>(null);
  const key = normalizeSeverity(pothole.severity_level);
  const statusColor = STATUS_COLORS[detail?.status ?? pothole.status] ?? "#8895A7";

  return (
    <Box
      sx={{
        width: 340,
        flexShrink: 0,
        height: "100%",
        bgcolor: "background.paper",
        borderLeft: "1px solid #E2E8F0",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <Box
        sx={{
          p: 2,
          borderBottom: "1px solid #E2E8F0",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
        }}
      >
        <Box>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 0.5 }}>
            <Typography variant="h6" fontWeight={700}>
              {Math.round((detail?.severity_score ?? 0) * 10) / 10}
            </Typography>
            <SeverityBadge level={pothole.severity_level} />
          </Box>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <StatusDot color={statusColor} />
            <Typography variant="caption" color="text.secondary" sx={{ textTransform: "capitalize" }}>
              {detail?.status ?? pothole.status}
            </Typography>
          </Box>
        </Box>
        <IconButton size="small" onClick={onClose}>
          <X size={16} />
        </IconButton>
      </Box>

      <Box sx={{ flex: 1, overflowY: "auto", p: 2, display: "flex", flexDirection: "column", gap: 2 }}>
        {/* Location */}
        <Box sx={{ border: "1px solid #E2E8F0", borderRadius: 2, p: 1.5 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.75, mb: 1 }}>
            <MapPin size={14} color="#1488DB" />
            <Typography variant="caption" fontWeight={600} color="primary.main">Location</Typography>
          </Box>
          <Typography variant="body2">{detail?.street_name ?? "Unknown street"}</Typography>
          <Typography variant="caption" color="text.secondary">
            {detail?.ward}, {detail?.district}, {detail?.city}
          </Typography>
          <Typography variant="caption" color="text.disabled" display="block" mt={0.5}>
            {pothole.gps_lat.toFixed(5)}, {pothole.gps_lon.toFixed(5)}
            {detail?.road_id && ` · ${detail.road_id}`}
          </Typography>
        </Box>

        {/* Measurements */}
        {detail && (
          <Box sx={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 1 }}>
            {[
              { label: "Depth", value: `${detail.depth_cm?.toFixed(1) ?? "—"} cm` },
              { label: "Area", value: `${detail.surface_area_cm2?.toFixed(0) ?? "—"} cm²` },
              { label: "Observations", value: detail.observation_count },
            ].map(({ label, value }) => (
              <Box key={label} sx={{ border: "1px solid #E2E8F0", borderRadius: 1.5, p: 1, textAlign: "center" }}>
                <Typography variant="caption" color="text.secondary" display="block">{label}</Typography>
                <Typography variant="body2" fontWeight={600}>{value}</Typography>
              </Box>
            ))}
          </Box>
        )}

        {/* Timeline */}
        <Box>
          <Typography variant="caption" fontWeight={600} color="text.secondary">Timeline</Typography>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1, mt: 0.75 }}>
            <Clock size={13} color="#8895A7" />
            <Typography variant="caption" color="text.secondary">
              Detected {new Date(detail?.detected_at ?? pothole.reported_at ?? "").toLocaleDateString()}
            </Typography>
          </Box>
          {detail?.in_progress_at && (
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, mt: 0.5 }}>
              <Clock size={13} color="#1488DB" />
              <Typography variant="caption" color="text.secondary">
                In progress {new Date(detail.in_progress_at).toLocaleDateString()}
              </Typography>
            </Box>
          )}
          {detail?.fixed_at && (
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, mt: 0.5 }}>
              <CheckCircle size={13} color="#16A34A" />
              <Typography variant="caption" color="text.secondary">
                Fixed {new Date(detail.fixed_at).toLocaleDateString()}
              </Typography>
            </Box>
          )}
        </Box>

        {/* Images */}
        {imageUrls.length > 0 && (
          <Box>
            <Typography variant="caption" fontWeight={600} color="text.secondary">Images</Typography>
            <Box sx={{ display: "flex", gap: 1, mt: 0.75, flexWrap: "wrap" }}>
              {imageUrls.map((url, i) => (
                <Box
                  key={i}
                  component="img"
                  src={url}
                  onClick={() => setLightbox(url)}
                  sx={{
                    width: 80,
                    height: 60,
                    objectFit: "cover",
                    borderRadius: 1,
                    cursor: "pointer",
                    border: "1px solid #E2E8F0",
                    "&:hover": { opacity: 0.85 },
                  }}
                />
              ))}
            </Box>
          </Box>
        )}

        {/* Progress bar for severity score */}
        {detail && (
          <Box>
            <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.25 }}>
              <Typography variant="caption" color="text.secondary">Severity Score</Typography>
              <Typography variant="caption" fontWeight={600}>{detail.severity_score?.toFixed(1)}/10</Typography>
            </Box>
            <LinearProgress
              variant="determinate"
              value={(detail.severity_score / 10) * 100}
              sx={{
                height: 6,
                borderRadius: 3,
                "& .MuiLinearProgress-bar": { bgcolor: SEVERITY[key].color },
              }}
            />
          </Box>
        )}
      </Box>

      {/* Actions */}
      <Divider />
      <Box sx={{ p: 2, display: "flex", gap: 1 }}>
        <Button variant="contained" size="small" fullWidth onClick={() => {}}>
          Assign Repair
        </Button>
        <Button variant="outlined" size="small" onClick={() => {}}>Review</Button>
        <Button variant="text" size="small" onClick={() => navigate("/annotation")}>Annotate</Button>
      </Box>

      {/* Lightbox */}
      <Dialog open={!!lightbox} onClose={() => setLightbox(null)} maxWidth="md">
        <DialogContent sx={{ p: 0 }}>
          {lightbox && <Box component="img" src={lightbox} sx={{ maxWidth: "100%", display: "block" }} />}
        </DialogContent>
      </Dialog>
    </Box>
  );
}

export default function MapPage() {
  const { markers } = useLoaderData<typeof loader>();
  const [selected, setSelected] = useState<PotholeMarker | null>(null);
  const [detail, setDetail] = useState<PotholeDetail | null>(null);
  const [imageUrls, setImageUrls] = useState<string[]>([]);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [activeFilters, setActiveFilters] = useState<Set<string>>(new Set());

  const counts = FILTER_KEYS.reduce<Record<string, number>>((acc, k) => {
    acc[k] = markers.filter((m) => normalizeSeverity(m.severity_level) === k).length;
    return acc;
  }, {});

  async function handleSelect(p: PotholeMarker) {
    setSelected(p);
    setDetail(null);
    setImageUrls([]);
    setLoadingDetail(true);
    try {
      const res = await fetch(`/api/pothole/${encodeURIComponent(p.pothole_id)}`);
      if (res.ok) {
        const d = await res.json() as PotholeDetail;
        setDetail(d);
        const urls: string[] = [];
        if (d.raw_image_path) {
          const r = await fetch(`/api/image/proxy?path=${encodeURIComponent(d.raw_image_path)}`);
          if (r.ok) urls.push(URL.createObjectURL(await r.blob()));
        }
        if (d.bev_image_path) {
          const r = await fetch(`/api/image/proxy?path=${encodeURIComponent(d.bev_image_path)}`);
          if (r.ok) urls.push(URL.createObjectURL(await r.blob()));
        }
        setImageUrls(urls);
      }
    } finally {
      setLoadingDetail(false);
    }
  }

  function toggleFilter(k: string) {
    setActiveFilters((prev) => {
      const next = new Set(prev);
      next.has(k) ? next.delete(k) : next.add(k);
      return next;
    });
  }

  return (
    <Box
      sx={{
        position: "absolute",
        inset: 0,
        top: 56,
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Filter row */}
      <Box
        sx={{
          display: "flex",
          gap: 1,
          px: 2,
          py: 1,
          bgcolor: "background.paper",
          borderBottom: "1px solid #E2E8F0",
          alignItems: "center",
        }}
      >
        {FILTER_KEYS.map((k) => (
          <Chip
            key={k}
            label={`${SEVERITY[k].label} (${counts[k] ?? 0})`}
            size="small"
            onClick={() => toggleFilter(k)}
            sx={{
              bgcolor: activeFilters.has(k) ? SEVERITY[k].bg : undefined,
              color: activeFilters.has(k) ? SEVERITY[k].color : "text.secondary",
              border: `1px solid ${activeFilters.has(k) ? SEVERITY[k].color : "#E2E8F0"}`,
              fontWeight: activeFilters.has(k) ? 600 : 400,
            }}
          />
        ))}
        <Typography variant="caption" color="text.secondary" ml={1}>
          {markers.length} potholes loaded
        </Typography>
      </Box>

      {/* Map + panel */}
      <Box sx={{ flex: 1, display: "flex", overflow: "hidden" }}>
          <ClientOnly
            fallback={
              <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
                <CircularProgress />
              </Box>
            }
          >
            <Suspense
              fallback={
                <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
                  <CircularProgress />
                </Box>
              }
            >
              <PotholeMap
                initialMarkers={markers}
                onSelect={handleSelect}
                selected={selected}
                activeFilters={activeFilters}
              />
            </Suspense>
          </ClientOnly>

        {selected && (
          <DetailPanel
            pothole={selected}
            detail={loadingDetail ? null : detail}
            imageUrls={imageUrls}
            onClose={() => { setSelected(null); setDetail(null); setImageUrls([]); }}
          />
        )}
      </Box>
    </Box>
  );
}
