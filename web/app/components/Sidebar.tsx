import Box from "@mui/material/Box";
import ButtonBase from "@mui/material/ButtonBase";
import Divider from "@mui/material/Divider";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import {
  Activity,
  Cpu,
  Database,
  Grid2X2,
  Map,
  Pencil,
  Settings,
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { NavLink } from "react-router";
import { useTheme } from "@mui/material/styles";

const NAV_ITEMS = [
  { to: "/", label: "Overview", icon: Grid2X2, end: true },
  { to: "/map", label: "Map", icon: Map, end: false },
  { to: "/health", label: "System Health", icon: Activity, end: false },
  { to: "/devices", label: "Edge Devices", icon: Cpu, end: false },
  { to: "/dataset", label: "Dataset", icon: Database, end: false },
  { to: "/lowconf", label: "Low Confidence", icon: AlertTriangle, end: false },
  { to: "/annotation", label: "Annotation", icon: Pencil, end: false },
  { to: "/settings", label: "Settings", icon: Settings, end: false },
];

interface Props {
  collapsed: boolean;
  onToggle: () => void;
}

export function Sidebar({ collapsed, onToggle }: Props) {
  const theme = useTheme();
  const w = collapsed ? 56 : 220;

  return (
    <Box
      component="nav"
      sx={{
        width: w,
        flexShrink: 0,
        height: "100vh",
        bgcolor: "background.paper",
        borderRight: "1px solid #E2E8F0",
        display: "flex",
        flexDirection: "column",
        transition: "width 0.2s ease",
        overflow: "hidden",
      }}
    >
      {/* Logo */}
      <Box
        sx={{
          height: 56,
          display: "flex",
          alignItems: "center",
          px: collapsed ? 1.5 : 2,
          borderBottom: "1px solid #E2E8F0",
          gap: 1.5,
          flexShrink: 0,
        }}
      >
        <Box
          sx={{
            width: 32,
            height: 32,
            borderRadius: "8px",
            background: `linear-gradient(135deg, ${theme.palette.primary.main}, ${theme.palette.primary.dark})`,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexShrink: 0,
          }}
        >
          <Typography sx={{ color: "#fff", fontWeight: 700, fontSize: 12 }}>
            PM
          </Typography>
        </Box>
        {!collapsed && (
          <Box>
            <Typography variant="subtitle2" fontWeight={700} noWrap>
              Pothole Monitor
            </Typography>
            <Typography variant="caption" color="text.secondary" noWrap>
              HCMUT • MoC&T
            </Typography>
          </Box>
        )}
      </Box>

      {/* Nav items */}
      <Box sx={{ flex: 1, overflowY: "auto", overflowX: "hidden", py: 1 }}>
        {NAV_ITEMS.map(({ to, label, icon: Icon, end }) => (
          <Tooltip key={to} title={collapsed ? label : ""} placement="right">
            <NavLink to={to} end={end} style={{ textDecoration: "none" }}>
              {({ isActive }) => (
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 1.5,
                    px: collapsed ? 1.5 : 2,
                    py: 1,
                    mx: 1,
                    my: 0.25,
                    borderRadius: 1.5,
                    bgcolor: isActive ? "primary.light" : "transparent",
                    color: isActive ? "primary.main" : "text.secondary",
                    "&:hover": {
                      bgcolor: isActive ? "primary.light" : "#F5F7FA",
                    },
                    transition: "background-color 0.15s",
                    cursor: "pointer",
                  }}
                >
                  <Icon size={18} strokeWidth={isActive ? 2.5 : 2} />
                  {!collapsed && (
                    <Typography
                      variant="body2"
                      fontWeight={isActive ? 600 : 400}
                      noWrap
                    >
                      {label}
                    </Typography>
                  )}
                </Box>
              )}
            </NavLink>
          </Tooltip>
        ))}
      </Box>

      <Divider />

      {/* Collapse toggle */}
      <ButtonBase
        onClick={onToggle}
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: collapsed ? "center" : "flex-end",
          px: 2,
          py: 1.5,
          color: "text.secondary",
          "&:hover": { color: "text.primary" },
        }}
      >
        {collapsed ? <ChevronRight size={18} /> : (
          <>
            <Typography variant="caption" mr={0.5}>Collapse</Typography>
            <ChevronLeft size={18} />
          </>
        )}
      </ButtonBase>
    </Box>
  );
}
