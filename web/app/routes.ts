import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
  layout("routes/_auth.tsx", [
    route("login", "routes/_auth.login.tsx"),
  ]),
  route("logout", "routes/logout.tsx"),
  layout("routes/_app.tsx", [
    index("routes/_app.overview.tsx"),
    route("map", "routes/_app.map.tsx"),
    route("health", "routes/_app.health.tsx"),
    route("devices", "routes/_app.devices.tsx"),
    route("dataset", "routes/_app.dataset.tsx"),
    route("lowconf", "routes/_app.lowconf.tsx"),
    route("annotation", "routes/_app.annotation.tsx"),
    route("settings", "routes/_app.settings.tsx"),
  ]),
  route("api/map-data", "routes/api.map-data.tsx"),
  route("api/annotation/assist", "routes/api.annotation.assist.tsx"),
  route("api/image/proxy", "routes/api.image.proxy.tsx"),
  route("api/pothole/:id", "routes/api.pothole.$id.tsx"),
  route("api/v1/collections", "routes/api.v1.collections.tsx"),
  route("api/v1/collections/road-defects/items", "routes/api.v1.collections.road-defects.items.tsx"),
  route("api/v1/collections/road-defects/items/:defect_id", "routes/api.v1.collections.road-defects.items.$defect_id.tsx"),
  route("dev-ui", "routes/dev-ui.tsx"),
] satisfies RouteConfig;
