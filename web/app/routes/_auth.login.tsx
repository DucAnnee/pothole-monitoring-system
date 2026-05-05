import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import CircularProgress from "@mui/material/CircularProgress";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { redirect, useActionData, useNavigation } from "react-router";
import type { Route } from "./+types/_auth.login";
import { commitSession, getSession, requireAuth } from "~/lib/session.server";

export async function loader({ request }: Route.LoaderArgs) {
  try {
    await requireAuth(request);
    return redirect("/");
  } catch {
    return null;
  }
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const username = form.get("username");
  const password = form.get("password");

  const validUser = process.env.AUTH_USERNAME ?? "admin";
  const validPass = process.env.AUTH_PASSWORD ?? "admin123";

  if (username !== validUser || password !== validPass) {
    return { error: "Invalid credentials" };
  }

  const session = await getSession(request.headers.get("Cookie"));
  session.set("userId", username);

  return redirect("/", {
    headers: { "Set-Cookie": await commitSession(session) },
  });
}

const HexLogo = () => (
  <svg width="64" height="72" viewBox="0 0 64 72" fill="none">
    <defs>
      <linearGradient id="hex-grad" x1="0" y1="0" x2="64" y2="72" gradientUnits="userSpaceOnUse">
        <stop stopColor="#1488DB" />
        <stop offset="1" stopColor="#030391" />
      </linearGradient>
    </defs>
    <polygon
      points="32,2 60,18 60,54 32,70 4,54 4,18"
      fill="url(#hex-grad)"
    />
    <text x="32" y="42" textAnchor="middle" fill="white" fontSize="16" fontWeight="700" fontFamily="Inter,Arial,sans-serif">
      PMS
    </text>
  </svg>
);

const HexBg = () => (
  <svg
    style={{ position: "fixed", inset: 0, width: "100%", height: "100%", zIndex: 0, opacity: 0.04 }}
    xmlns="http://www.w3.org/2000/svg"
  >
    {Array.from({ length: 8 }, (_, row) =>
      Array.from({ length: 10 }, (_, col) => {
        const x = col * 90 + (row % 2) * 45;
        const y = row * 78;
        const pts = [
          [x + 30, y], [x + 60, y + 15], [x + 60, y + 45],
          [x + 30, y + 60], [x, y + 45], [x, y + 15],
        ]
          .map(([px, py]) => `${px},${py}`)
          .join(" ");
        return <polygon key={`${row}-${col}`} points={pts} fill="none" stroke="#1488DB" strokeWidth="1" />;
      })
    )}
  </svg>
);

export default function LoginPage() {
  const actionData = useActionData<typeof action>();
  const nav = useNavigation();
  const submitting = nav.state === "submitting";

  return (
    <Box
      sx={{
        minHeight: "100vh",
        bgcolor: "background.default",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        position: "relative",
        overflow: "hidden",
      }}
    >
      <HexBg />
      <Box
        sx={{
          position: "relative",
          zIndex: 1,
          bgcolor: "background.paper",
          borderRadius: 3,
          border: "1px solid #E2E8F0",
          p: 5,
          width: 400,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: 3,
        }}
      >
        <HexLogo />
        <Box textAlign="center">
          <Typography variant="h5" fontWeight={700} color="text.primary">
            Pothole Monitor
          </Typography>
          <Typography variant="body2" color="text.secondary" mt={0.5}>
            HCMUT • MoC&T Operations Dashboard
          </Typography>
        </Box>

        {actionData?.error && (
          <Alert severity="error" sx={{ width: "100%" }}>
            {actionData.error}
          </Alert>
        )}

        <Box
          component="form"
          method="post"
          sx={{ width: "100%", display: "flex", flexDirection: "column", gap: 2 }}
        >
          <TextField label="Username" name="username" autoComplete="username" fullWidth size="small" />
          <TextField
            label="Password"
            name="password"
            type="password"
            autoComplete="current-password"
            fullWidth
            size="small"
          />
          <Button
            type="submit"
            variant="contained"
            fullWidth
            disabled={submitting}
            sx={{ mt: 1, py: 1.2 }}
          >
            {submitting ? <CircularProgress size={20} color="inherit" /> : "Sign In"}
          </Button>
        </Box>

        <Typography variant="caption" color="text.disabled" textAlign="center">
          Demo: admin / admin123
        </Typography>
      </Box>
    </Box>
  );
}
