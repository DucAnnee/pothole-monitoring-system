import type { Route } from "./+types/api.v1.collections";

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const base = `${url.origin}/api/v1`;

  return Response.json({
    links: [
      { href: `${base}/collections`, rel: "self", type: "application/json", title: "Collections" },
    ],
    collections: [
      {
        id: "road-defects",
        title: "Road defects",
        description: "Current pothole and road-defect projection backed by PostGIS.",
        itemType: "feature",
        crs: ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"],
        links: [
          {
            href: `${base}/collections/road-defects/items`,
            rel: "items",
            type: "application/geo+json",
            title: "Road defect features",
          },
        ],
      },
    ],
  });
}
