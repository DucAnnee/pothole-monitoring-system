import type { Route } from "./+types/api.v1.collections.road-defects.items.$defect_id";
import { queryRoadDefectById, toRoadDefectFeature } from "~/lib/postgis.server";

export async function loader({ params }: Route.LoaderArgs) {
  const defectId = params.defect_id;
  if (!defectId) return Response.json({ error: "Missing defect_id." }, { status: 400 });

  try {
    const row = await queryRoadDefectById(defectId);
    if (!row) return Response.json({ error: "Road defect not found." }, { status: 404 });
    return Response.json(toRoadDefectFeature(row), {
      headers: { "Content-Type": "application/geo+json" },
    });
  } catch (error) {
    console.warn("[PostGIS] OGC item unavailable:", error instanceof Error ? error.message : error);
    return Response.json({ error: "Road defect collection is unavailable." }, { status: 503 });
  }
}
