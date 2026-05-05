import type { Route } from "./+types/api.pothole.$id";
import { queryPotholeDetail } from "~/lib/postgis.server";

export async function loader({ params }: Route.LoaderArgs) {
  const { id } = params;
  if (!id) return new Response("Missing id", { status: 400 });
  const detail = await queryPotholeDetail(id).catch((error) => {
    console.warn("[PostGIS] pothole detail unavailable:", error instanceof Error ? error.message : error);
    return null;
  });
  if (!detail) return new Response("Not found", { status: 404 });
  return Response.json(detail);
}
