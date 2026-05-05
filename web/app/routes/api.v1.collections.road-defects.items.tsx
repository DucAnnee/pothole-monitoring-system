import type { Route } from "./+types/api.v1.collections.road-defects.items";
import { queryRoadDefectItems, toRoadDefectFeature } from "~/lib/postgis.server";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);

  try {
    const rows = await queryRoadDefectItems({
      bbox: url.searchParams.get("bbox"),
      datetime: url.searchParams.get("datetime"),
      district: url.searchParams.get("district"),
      ward: url.searchParams.get("ward"),
      roadSegmentId: url.searchParams.get("road_segment_id"),
      severityLevel: url.searchParams.get("severity_level"),
      status: url.searchParams.get("status"),
      limit: url.searchParams.get("limit"),
      offset: url.searchParams.get("offset"),
    });

    return Response.json(
      {
        type: "FeatureCollection",
        timeStamp: new Date().toISOString(),
        numberReturned: rows.length,
        links: [
          { href: url.toString(), rel: "self", type: "application/geo+json" },
          { href: `${url.origin}/api/v1/collections/road-defects`, rel: "collection", type: "application/json" },
        ],
        features: rows.map(toRoadDefectFeature),
      },
      { headers: { "Content-Type": "application/geo+json" } }
    );
  } catch (error) {
    if (error instanceof Response) return error;
    console.warn("[PostGIS] OGC items unavailable:", error instanceof Error ? error.message : error);
    return Response.json({ error: "Road defects collection is unavailable." }, { status: 503 });
  }
}
