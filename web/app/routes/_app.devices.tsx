import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import { useEffect } from "react";
import { useLoaderData, useRevalidator } from "react-router";
import type { Route } from "./+types/_app.devices";
import { EdgeDevicesTable } from "~/components/EdgeDevicesTable";
import { cached } from "~/lib/redis.server";
import { requireAuth } from "~/lib/session.server";
import { fetchHealthData } from "~/lib/health.server";

export const handle = { title: "Edge Devices" };

export async function loader({ request }: Route.LoaderArgs) {
  await requireAuth(request);
  const data = await cached("web:health:v1", 10, fetchHealthData);
  return { edge_devices: data.edge_devices };
}

export default function DevicesPage() {
  const { edge_devices } = useLoaderData<typeof loader>();
  const { revalidate } = useRevalidator();

  useEffect(() => {
    const id = setInterval(revalidate, 30_000);
    return () => clearInterval(id);
  }, [revalidate]);

  return (
    <Box>
      <Typography variant="h6" fontWeight={700} mb={2}>Edge Devices</Typography>
      <Card>
        <CardContent>
          <EdgeDevicesTable devices={edge_devices} />
        </CardContent>
      </Card>
    </Box>
  );
}
