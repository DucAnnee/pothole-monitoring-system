import Box from "@mui/material/Box";
import { useState } from "react";
import { Outlet } from "react-router";
import { Header } from "./Header";
import { Sidebar } from "./Sidebar";

export function AppShell() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <Box sx={{ display: "flex", height: "100vh", overflow: "hidden" }}>
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed((c) => !c)} />
      <Box sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <Header lastSync={new Date().toLocaleTimeString()} />
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
