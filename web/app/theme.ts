import { createTheme } from "@mui/material/styles";

export const theme = createTheme({
  palette: {
    primary: { main: "#1488DB", dark: "#030391", light: "#E8F4FD" },
    error: { main: "#DC2626" },
    warning: { main: "#D97706" },
    success: { main: "#16A34A" },
    background: { default: "#F5F7FA", paper: "#FFFFFF" },
    text: { primary: "#1A2332", secondary: "#5A6B7F", disabled: "#8895A7" },
  },
  typography: { fontFamily: '"Inter", "Arial", sans-serif' },
  shape: { borderRadius: 8 },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          boxShadow: "none",
          border: "1px solid #E2E8F0",
        },
      },
    },
  },
});
