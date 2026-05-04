import type { Route } from "./+types/api.image.proxy";
import { proxyStream } from "~/lib/minio.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const path = url.searchParams.get("path");
  if (!path) return new Response("Missing path", { status: 400 });
  try {
    return await proxyStream(path);
  } catch {
    return new Response("Not found", { status: 404 });
  }
}
