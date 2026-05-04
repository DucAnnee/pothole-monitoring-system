import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { SeverityBadge } from "~/components/SeverityBadge";
import { SparklineChart } from "~/components/SparklineChart";
import { StatusDot } from "~/components/StatusDot";

const mockData = [4, 7, 3, 9, 5, 12, 8, 15, 11, 6, 9, 14, 10, 7, 13, 8, 11, 16, 9, 12];

export default function DevUI() {
  return (
    <Box sx={{ p: 4, display: "flex", flexDirection: "column", gap: 3 }}>
      <Typography variant="h5">Design System Preview</Typography>

      <Box>
        <Typography variant="subtitle2" gutterBottom>SeverityBadge</Typography>
        <Box sx={{ display: "flex", gap: 1 }}>
          {["critical", "high", "moderate", "minor"].map((l) => (
            <SeverityBadge key={l} level={l} />
          ))}
        </Box>
      </Box>

      <Box>
        <Typography variant="subtitle2" gutterBottom>StatusDot</Typography>
        <Box sx={{ display: "flex", gap: 2, alignItems: "center" }}>
          <StatusDot color="#16A34A" title="Online" />
          <StatusDot color="#DC2626" title="Offline" />
          <StatusDot color="#D97706" title="Warning" />
        </Box>
      </Box>

      <Box>
        <Typography variant="subtitle2" gutterBottom>SparklineChart</Typography>
        <SparklineChart data={mockData} width={300} height={60} />
      </Box>
    </Box>
  );
}
