import type { Route } from "./+types/_app.dataset";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useLoaderData } from "react-router";
import { queryDatasetImages, queryDatasetStats } from "~/lib/dataset.server";

export const handle = { title: "Dataset Management" };

export async function loader(_args: Route.LoaderArgs) {
  const [stats, images] = await Promise.all([
    queryDatasetStats().catch(() => ({
      totalImages: 0,
      annotations: 0,
      train: 0,
      valid: 0,
      test: 0,
      version: "—",
    })),
    queryDatasetImages(48).catch(() => []),
  ]);
  return { stats, images };
}

const TABS = ["Images", "Annotations", "Versions", "Low Confidence"];

const SEVERITY_COLOR: Record<string, string> = {
  critical: "#DC2626",
  high: "#EA580C",
  moderate: "#D97706",
  minor: "#16A34A",
};

const FOLDERS = ["All Images", "Labeled", "Unlabeled", "Critical", "High", "Moderate", "Minor"];

export default function DatasetPage() {
  const { stats, images } = useLoaderData<typeof loader>();
  const [tab, setTab] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const [folderFilter, setFolderFilter] = useState("All Images");

  const STATS = [
    { label: "Total Images", value: stats.totalImages.toLocaleString() },
    { label: "Annotations", value: stats.annotations.toLocaleString() },
    { label: "Train", value: stats.train.toLocaleString() },
    { label: "Valid", value: stats.valid.toLocaleString() },
    { label: "Test", value: stats.test.toLocaleString() },
    { label: "Version", value: stats.version },
  ];

  const folderFiltered = images.filter((img) => {
    if (folderFilter === "All Images") return true;
    if (folderFilter === "Labeled") return img.defectId !== null;
    if (folderFilter === "Unlabeled") return img.defectId === null;
    return img.severity === folderFilter.toLowerCase();
  });

  const selectedImage = images.find((img) => img.objectKey === selected) ?? null;

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
      {/* Stats row */}
      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 1.5 }}>
        {STATS.map(({ label, value }) => (
          <Card key={label}>
            <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
              <Typography variant="caption" color="text.secondary">{label}</Typography>
              <Typography variant="h6" fontWeight={700}>{value}</Typography>
            </CardContent>
          </Card>
        ))}
      </Box>

      {/* Tabs */}
      <Tabs
        value={tab}
        onChange={(_, v) => setTab(v as number)}
        sx={{ borderBottom: "1px solid #E2E8F0" }}
      >
        {TABS.map((t) => <Tab key={t} label={t} />)}
      </Tabs>

      {/* Content split */}
      <Box sx={{ display: "flex", gap: 2, minHeight: 400 }}>
        {/* Folder sidebar */}
        <Box
          sx={{
            width: 200,
            flexShrink: 0,
            border: "1px solid #E2E8F0",
            borderRadius: 2,
            p: 1.5,
            bgcolor: "background.paper",
          }}
        >
          <Typography variant="caption" fontWeight={600} color="text.secondary">
            FOLDERS
          </Typography>
          {FOLDERS.map((f) => (
            <Box
              key={f}
              onClick={() => setFolderFilter(f)}
              sx={{
                py: 0.5,
                px: 1,
                borderRadius: 1,
                cursor: "pointer",
                mt: 0.5,
                bgcolor: folderFilter === f ? "#EFF6FF" : undefined,
                "&:hover": { bgcolor: "#F5F7FA" },
              }}
            >
              <Typography
                variant="caption"
                fontWeight={folderFilter === f ? 600 : 400}
                color={folderFilter === f ? "primary" : "text.primary"}
              >
                {f}
              </Typography>
            </Box>
          ))}
        </Box>

        {/* Image grid */}
        <Box
          sx={{
            flex: 1,
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(110px, 1fr))",
            gap: 1,
            alignContent: "start",
            overflowY: "auto",
            maxHeight: 560,
          }}
        >
          {folderFiltered.length === 0 && (
            <Box sx={{ gridColumn: "1 / -1", p: 4 }}>
              <Typography variant="body2" color="text.secondary">
                No images in this folder.
              </Typography>
            </Box>
          )}
          {folderFiltered.map((img) => (
            <Tooltip key={img.objectKey} title={img.name} placement="top">
              <Box
                onClick={() => setSelected(img.objectKey)}
                sx={{
                  border: `2px solid ${selected === img.objectKey ? "#1488DB" : "#E2E8F0"}`,
                  borderRadius: 1.5,
                  overflow: "hidden",
                  cursor: "pointer",
                  "&:hover": { borderColor: "#1488DB" },
                }}
              >
                <Box
                  component="img"
                  src={`/api/image/proxy?path=${encodeURIComponent(img.objectKey)}`}
                  alt={img.name}
                  loading="lazy"
                  sx={{
                    width: "100%",
                    height: 80,
                    objectFit: "cover",
                    display: "block",
                    bgcolor: "#F0F0F0",
                  }}
                  onError={(e) => {
                    (e.currentTarget as HTMLImageElement).style.display = "none";
                    const parent = e.currentTarget.parentElement;
                    if (parent) {
                      const fallback = document.createElement("div");
                      fallback.style.cssText =
                        "height:80px;display:flex;align-items:center;justify-content:center;background:#F0F0F0";
                      fallback.textContent = "—";
                      parent.prepend(fallback);
                    }
                  }}
                />
                <Box sx={{ p: 0.5, display: "flex", alignItems: "center", gap: 0.5 }}>
                  <Box
                    sx={{
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      bgcolor: SEVERITY_COLOR[img.severity] ?? "#94A3B8",
                      flexShrink: 0,
                    }}
                  />
                  <Typography variant="caption" fontSize={10} noWrap>
                    {img.name}
                  </Typography>
                </Box>
              </Box>
            </Tooltip>
          ))}
        </Box>

        {/* Detail panel */}
        {selectedImage && (
          <Box
            sx={{
              width: 220,
              flexShrink: 0,
              border: "1px solid #E2E8F0",
              borderRadius: 2,
              p: 1.5,
              bgcolor: "background.paper",
              display: "flex",
              flexDirection: "column",
              gap: 1,
            }}
          >
            <Typography variant="caption" fontWeight={600} color="text.secondary">
              DETAILS
            </Typography>
            <Box
              component="img"
              src={`/api/image/proxy?path=${encodeURIComponent(selectedImage.objectKey)}`}
              alt={selectedImage.name}
              sx={{ width: "100%", borderRadius: 1, objectFit: "cover", maxHeight: 120 }}
            />
            <Typography variant="caption" display="block" sx={{ wordBreak: "break-all" }}>
              {selectedImage.name}
            </Typography>
            <Chip
              label={selectedImage.severity}
              size="small"
              sx={{
                color: SEVERITY_COLOR[selectedImage.severity] ?? "#94A3B8",
                borderColor: SEVERITY_COLOR[selectedImage.severity] ?? "#94A3B8",
                border: "1px solid",
                textTransform: "capitalize",
                width: "fit-content",
              }}
            />
            {selectedImage.defectId && (
              <Typography variant="caption" color="text.secondary" display="block">
                Defect: {selectedImage.defectId}
              </Typography>
            )}
          </Box>
        )}
      </Box>
    </Box>
  );
}
