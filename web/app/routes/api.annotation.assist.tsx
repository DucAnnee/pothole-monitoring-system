import type { Route } from "./+types/api.annotation.assist";
import { buildSam3AssistResponse } from "~/lib/annotation-contract";

export async function action({ request }: Route.ActionArgs) {
  if (request.method !== "POST") {
    return Response.json({ error: "Method not allowed" }, { status: 405 });
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "Request body must be JSON." }, { status: 400 });
  }

  const response = buildSam3AssistResponse(
    typeof body === "object" && body !== null ? body : {},
  );
  return Response.json(response);
}
