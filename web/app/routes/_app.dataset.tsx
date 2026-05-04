import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import Typography from "@mui/material/Typography";
import { useState } from "react";

export const handle = { title: "Dataset Management" };

const STATS = [
  { label: "Total Images", value: "24,183" },
  { label: "Annotations", value: "19,204" },
  { label: "Train", value: "15,360" },
  { label: "Valid", value: "2,560" },
  { label: "Test", value: "1,284" },
  { label: "Version", value: "v2.4" },
];

const TABS = ["Images", "Annotations", "Versions", "Low Confidence"];

const MOCK_IMAGES = Array.from({ length: 24 }, (_, i) => ({
  id: `img-${i + 1}`,
  name: `frame_${String(i + 1).padStart(4, "0")}.jpg`,
  label: i % 4 === 0 ? "critical" : i % 3 === 0 ? "high" : i % 2 === 0 ? "moderate" : "minor",
}));

const LABEL_COLORS: Record<string, string> = {
  critical: "#DC2626",
  high: "#EA580C",
  moderate: "#D97706",
  minor: "#16A34A",
};

export default function DatasetPage() {
  const [tab, setTab] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);

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
      <Tabs value={tab} onChange={(_, v) => setTab(v as number)} sx={{ borderBottom: "1px solid #E2E8F0" }}>
        {TABS.map((t) => <Tab key={t} label={t} />)}
      </Tabs>

      {/* Content split */}
      <Box sx={{ display: "flex", gap: 2, minHeight: 400 }}>
        {/* Tree sidebar */}
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
          <Typography variant="caption" fontWeight={600} color="text.secondary">FOLDERS</Typography>
          {["All Images", "Labeled", "Unlabeled", "Critical", "High", "Moderate", "Minor"].map((f) => (
            <Box
              key={f}
              sx={{
                py: 0.5,
                px: 1,
                borderRadius: 1,
                cursor: "pointer",
                "&:hover": { bgcolor: "#F5F7FA" },
                mt: 0.5,
              }}
            >
              <Typography variant="caption">{f}</Typography>
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
          }}
        >
          {MOCK_IMAGES.map((img) => (
            <Box
              key={img.id}
              onClick={() => setSelected(img.id)}
              sx={{
                border: `2px solid ${selected === img.id ? "#1488DB" : "#E2E8F0"}`,
                borderRadius: 1.5,
                overflow: "hidden",
                cursor: "pointer",
                "&:hover": { borderColor: "#1488DB" },
              }}
            >
              <Box
                sx={{
                  height: 80,
                  bgcolor: "#F0F0F0",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                }}
              >
                <Typography variant="caption" color="text.disabled" fontSize={10}>{img.id}</Typography>
              </Box>
              <Box sx={{ p: 0.5, display: "flex", alignItems: "center", gap: 0.5 }}>
                <Box
                  sx={{ width: 6, height: 6, borderRadius: "50%", bgcolor: LABEL_COLORS[img.label] }}
                />
                <Typography variant="caption" fontSize={10} noWrap>{img.name}</Typography>
              </Box>
            </Box>
          ))}
        </Box>

        {/* Detail panel */}
        {selected && (
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
            <Typography variant="caption" fontWeight={600} color="text.secondary">DETAILS</Typography>
            <Typography variant="caption" display="block" mt={1}>{selected}</Typography>
            <Typography variant="caption" color="text.secondary" display="block">960 × 540 px</Typography>
            <Typography variant="caption" color="text.secondary" display="block">JPEG • 428 KB</Typography>
          </Box>
        )}
      </Box>
    </Box>
  );
}
