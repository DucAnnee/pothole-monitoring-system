import type { Route } from "./+types/api.annotation.assist";
import { validatePolygon, type Sam3AssistRequest } from "~/lib/annotation-contract";
import { callSam3Assist } from "~/lib/triton-sam3.server";

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

  const req = (typeof body === "object" && body !== null ? body : {}) as Sam3AssistRequest;

  const imageKey = req.image_object_key;
  if (!imageKey || typeof imageKey !== "string") {
    return Response.json({ error: "image_object_key required." }, { status: 400 });
  }

  const polygonResult = validatePolygon(req.current_polygon);
  const currentPolygon = polygonResult.ok
    ? polygonResult.polygon
    : ([[150, 100], [450, 100], [450, 300], [150, 300]] as [number, number][]);

  const result = await callSam3Assist(imageKey, currentPolygon);

  return Response.json({
    polygon: result.polygon,
    confidence: result.confidence,
    source: result.source,
    model: {
      name: "sam3_assist",
      version: "2.1-hiera-small",
    },
  });
}
