import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import LinearProgress from "@mui/material/LinearProgress";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";
import { StatusDot } from "./StatusDot";
import type { EdgeDevice } from "~/lib/health.server";

const CONNECTION_COLOR: Record<string, string> = {
  online: "#16A34A",
  degraded: "#D97706",
  offline: "#DC2626",
};

const HEALTH_COLOR: Record<string, string> = {
  healthy: "#16A34A",
  warning: "#D97706",
  critical: "#DC2626",
};

export function EdgeDevicesTable({ devices }: { devices: EdgeDevice[] }) {
  return (
    <Table size="small">
      <TableHead>
        <TableRow sx={{ "& th": { fontWeight: 600, fontSize: 12, color: "text.secondary" } }}>
          <TableCell>Vehicle</TableCell>
          <TableCell>Device</TableCell>
          <TableCell>GPS</TableCell>
          <TableCell>Camera</TableCell>
          <TableCell>Model</TableCell>
          <TableCell>Last Upload</TableCell>
          <TableCell>Battery</TableCell>
          <TableCell>Storage</TableCell>
          <TableCell>Connection</TableCell>
          <TableCell>Pending</TableCell>
          <TableCell>Health</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {devices.map((d) => (
          <TableRow key={d.device_id} sx={{ "&:hover": { bgcolor: "#F5F7FA" } }}>
            <TableCell><Typography variant="caption" fontWeight={600}>{d.vehicle_id}</Typography></TableCell>
            <TableCell><Typography variant="caption">{d.device_id}</Typography></TableCell>
            <TableCell>
              <StatusDot color={d.gps_ok === null ? "#9CA3AF" : d.gps_ok ? "#16A34A" : "#DC2626"} title={d.gps_ok === null ? "n/a" : d.gps_ok ? "OK" : "Fault"} />
            </TableCell>
            <TableCell>
              <StatusDot color={d.camera_ok === null ? "#9CA3AF" : d.camera_ok ? "#16A34A" : "#DC2626"} title={d.camera_ok === null ? "n/a" : d.camera_ok ? "OK" : "Fault"} />
            </TableCell>
            <TableCell><Typography variant="caption">{d.model_version}</Typography></TableCell>
            <TableCell>
              <Typography variant="caption">
                {new Date(d.last_upload).toLocaleTimeString()}
              </Typography>
            </TableCell>
            <TableCell>
              {d.battery_pct == null ? (
                <Typography variant="caption" color="text.disabled">n/a</Typography>
              ) : (
                <Box sx={{ width: 80 }}>
                  <Typography variant="caption">{d.battery_pct}%</Typography>
                  <LinearProgress
                    variant="determinate"
                    value={d.battery_pct}
                    sx={{
                      height: 4,
                      borderRadius: 2,
                      "& .MuiLinearProgress-bar": {
                        bgcolor: d.battery_pct < 20 ? "#DC2626" : d.battery_pct < 50 ? "#D97706" : "#16A34A",
                      },
                    }}
                  />
                </Box>
              )}
            </TableCell>
            <TableCell>
              <Typography variant="caption">{d.storage_pct != null ? `${d.storage_pct}%` : "n/a"}</Typography>
            </TableCell>
            <TableCell>
              <Chip
                label={d.connection}
                size="small"
                sx={{
                  color: CONNECTION_COLOR[d.connection],
                  bgcolor: CONNECTION_COLOR[d.connection] + "20",
                  fontSize: 11,
                  textTransform: "capitalize",
                }}
              />
            </TableCell>
            <TableCell>
              <Typography variant="caption">{d.pending_count != null ? d.pending_count : "n/a"}</Typography>
            </TableCell>
            <TableCell>
              <Chip
                label={d.health}
                size="small"
                sx={{
                  color: HEALTH_COLOR[d.health],
                  bgcolor: HEALTH_COLOR[d.health] + "20",
                  fontSize: 11,
                  textTransform: "capitalize",
                }}
              />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
