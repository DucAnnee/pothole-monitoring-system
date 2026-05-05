import Badge from "@mui/material/Badge";
import Box from "@mui/material/Box";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import InputBase from "@mui/material/InputBase";
import Typography from "@mui/material/Typography";
import { Bell, LogOut, Search } from "lucide-react";
import { useFetcher, useMatches } from "react-router";

interface Props {
  lastSync?: string;
}

export function Header({ lastSync }: Props) {
  const matches = useMatches();
  const lastMatch = matches[matches.length - 1];
  const title =
    (lastMatch?.handle as { title?: string } | undefined)?.title ?? "Dashboard";

  const fetcher = useFetcher();

  return (
    <Box
      component="header"
      sx={{
        height: 56,
        flexShrink: 0,
        bgcolor: "background.paper",
        borderBottom: "1px solid #E2E8F0",
        display: "flex",
        alignItems: "center",
        px: 3,
        gap: 2,
      }}
    >
      {/* Left */}
      <Box sx={{ flex: "0 0 auto" }}>
        <Typography variant="subtitle1" fontWeight={600}>
          {title}
        </Typography>
        {lastSync && (
          <Typography variant="caption" color="text.secondary">
            Last sync: {lastSync}
          </Typography>
        )}
      </Box>

      {/* Center search */}
      <Box sx={{ flex: 1, display: "flex", justifyContent: "center" }}>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            bgcolor: "#F5F7FA",
            border: "1px solid #E2E8F0",
            borderRadius: 2,
            px: 1.5,
            py: 0.5,
            width: 280,
            gap: 1,
          }}
        >
          <Search size={16} color="#8895A7" />
          <InputBase
            placeholder="Search potholes…"
            sx={{ fontSize: 13, color: "text.secondary", flex: 1 }}
            readOnly
          />
        </Box>
      </Box>

      {/* Right */}
      <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
        <IconButton size="small">
          <Badge variant="dot" color="error">
            <Bell size={18} />
          </Badge>
        </IconButton>
        <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <Box
            sx={{
              width: 32,
              height: 32,
              borderRadius: "50%",
              bgcolor: "primary.light",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <Typography variant="caption" color="primary.main" fontWeight={700}>
              OP
            </Typography>
          </Box>
          <Box>
            <Typography variant="caption" fontWeight={600} display="block">
              Operator
            </Typography>
            <Typography variant="caption" color="text.secondary" display="block">
              admin
            </Typography>
          </Box>
          <fetcher.Form method="post" action="/logout">
            <IconButton type="submit" size="small" title="Logout">
              <LogOut size={16} />
            </IconButton>
          </fetcher.Form>
        </Box>
      </Box>
    </Box>
  );
}
