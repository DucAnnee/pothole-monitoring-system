import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import Slider from "@mui/material/Slider";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { SEVERITY } from "~/constants/severity";

export const handle = { title: "Low Confidence" };

const SEVERITY_KEYS = ["critical", "high", "moderate", "minor"] as const;

const MOCK_DETECTIONS = Array.from({ length: 8 }, (_, i) => ({
  id: `det-${i + 1}`,
  confidence: Math.round((0.3 + Math.random() * 0.25) * 100),
  severity: SEVERITY_KEYS[i % 4],
  location: `Nguyen Thi Minh Khai, District ${i + 1}`,
  date: new Date(Date.now() - i * 86400000).toLocaleDateString(),
  reason: i % 2 === 0 ? "Occlusion by vehicle" : "Poor lighting conditions",
}));

export default function LowConfPage() {
  const [confRange, setConfRange] = useState<number[]>([30, 55]);
  const [activeSeverities, setActiveSeverities] = useState<Set<string>>(new Set());

  function toggleSeverity(k: string) {
    setActiveSeverities((prev) => {
      const next = new Set(prev);
      next.has(k) ? next.delete(k) : next.add(k);
      return next;
    });
  }

  const filtered = MOCK_DETECTIONS.filter(
    (d) =>
      d.confidence >= confRange[0] &&
      d.confidence <= confRange[1] &&
      (activeSeverities.size === 0 || activeSeverities.has(d.severity))
  );

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
      {/* Filter bar */}
      <Box
        sx={{
          p: 2,
          border: "1px solid #E2E8F0",
          borderRadius: 2,
          bgcolor: "background.paper",
          display: "flex",
          alignItems: "center",
          gap: 3,
        }}
      >
        <Box sx={{ width: 240 }}>
          <Typography variant="caption" color="text.secondary">
            Confidence: {confRange[0]}% – {confRange[1]}%
          </Typography>
          <Slider
            value={confRange}
            onChange={(_, v) => setConfRange(v as number[])}
            min={0}
            max={100}
            size="small"
          />
        </Box>
        <Box sx={{ display: "flex", gap: 1 }}>
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
        <Typography variant="caption" color="text.secondary">{filtered.length} results</Typography>
      </Box>

      {/* Card grid */}
      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px,1fr))", gap: 2 }}>
        {filtered.map((d) => (
          <Card key={d.id}>
            <CardContent>
              {/* Dual image panes */}
              <Box sx={{ display: "flex", gap: 1, mb: 1.5 }}>
                {["Raw", "Segmentation"].map((label) => (
                  <Box
                    key={label}
                    sx={{
                      flex: 1,
                      height: 100,
                      bgcolor: "#F0F0F0",
                      borderRadius: 1,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                    }}
                  >
                    <Typography variant="caption" color="text.disabled">{label}</Typography>
                  </Box>
                ))}
              </Box>

              <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 0.5 }}>
                <Box>
                  <Typography variant="body2" fontWeight={600}>{d.confidence}% confidence</Typography>
                  <Typography variant="caption" color="text.secondary">{d.location}</Typography>
                </Box>
                <Chip
                  label={SEVERITY[d.severity].label}
                  size="small"
                  sx={{ color: SEVERITY[d.severity].color, bgcolor: SEVERITY[d.severity].bg }}
                />
              </Box>

              <Typography variant="caption" color="text.secondary" display="block">{d.date}</Typography>
              <Typography variant="caption" color="warning.main" display="block" mt={0.5}>
                Reason: {d.reason}
              </Typography>

              <Button size="small" variant="outlined" sx={{ mt: 1 }}>Review</Button>
            </CardContent>
          </Card>
        ))}
      </Box>
    </Box>
  );
}
