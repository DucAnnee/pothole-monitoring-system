import type { Route } from "./+types/api.map-data";
import { cached } from "~/lib/redis.server";
import { queryMapPotholes } from "~/lib/trino.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const lat = parseFloat(url.searchParams.get("lat") ?? "");
  const lon = parseFloat(url.searchParams.get("lon") ?? "");

  const hasCoords = !isNaN(lat) && !isNaN(lon);
  const key = hasCoords
    ? `web:mapdata:${Math.round(lat * 1000)}:${Math.round(lon * 1000)}`
    : "web:mapdata:recent";

  const markers = await cached(key, 30, () =>
    hasCoords ? queryMapPotholes(lat, lon, 1) : queryMapPotholes()
  ).catch((error) => {
    console.warn("[Trino] map data API unavailable:", error instanceof Error ? error.message : error);
    return [];
  });

  return Response.json({ markers });
}
