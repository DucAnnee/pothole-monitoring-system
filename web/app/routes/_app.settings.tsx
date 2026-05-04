import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { Settings } from "lucide-react";

export const handle = { title: "Settings" };

export default function SettingsPage() {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        height: "60vh",
        gap: 2,
        color: "text.disabled",
      }}
    >
      <Settings size={48} strokeWidth={1} />
      <Typography variant="h6" color="text.disabled">
        Configuration panel coming soon.
      </Typography>
    </Box>
  );
}
