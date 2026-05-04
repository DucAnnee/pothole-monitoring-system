import type { Route } from "./+types/api.pothole.$id";
import { queryPotholeDetail } from "~/lib/trino.server";

export async function loader({ params }: Route.LoaderArgs) {
  const { id } = params;
  if (!id) return new Response("Missing id", { status: 400 });
  const detail = await queryPotholeDetail(id);
  if (!detail) return new Response("Not found", { status: 404 });
  return Response.json(detail);
}
