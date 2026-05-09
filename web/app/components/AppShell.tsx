import Box from "@mui/material/Box";
import LinearProgress from "@mui/material/LinearProgress";
import { useState } from "react";
import { Outlet, useNavigation } from "react-router";
import { Header } from "./Header";
import { Sidebar } from "./Sidebar";

export function AppShell() {
  const [collapsed, setCollapsed] = useState(false);
  const navigation = useNavigation();

  return (
    <Box sx={{ display: "flex", height: "100vh", overflow: "hidden" }}>
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed((c) => !c)} />
      {/* position:relative makes this the containing block for absolutely-positioned children (e.g. map page) */}
      <Box sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", position: "relative" }}>
        <Header lastSync={new Date().toLocaleTimeString()} />
        {navigation.state !== "idle" && (
          <LinearProgress sx={{ position: "absolute", top: 56, left: 0, right: 0, zIndex: 10, height: 2 }} />
        )}
        <Box
          component="main"
          sx={{ flex: 1, overflow: "auto", p: 3, bgcolor: "background.default" }}
        >
          <Outlet />
        </Box>
      </Box>
    </Box>
  );
}
