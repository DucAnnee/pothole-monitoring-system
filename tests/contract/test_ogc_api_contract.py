from pathlib import Path


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_ogc_routes_are_registered(repo_root: Path):
    routes = read(repo_root / "web" / "app" / "routes.ts")
    assert 'route("api/v1/collections"' in routes
    assert 'route("api/v1/collections/road-defects/items"' in routes
    assert 'route("api/v1/collections/road-defects/items/:defect_id"' in routes


def test_ogc_items_endpoint_returns_geojson_feature_collection(repo_root: Path):
    route = read(repo_root / "web" / "app" / "routes" / "api.v1.collections.road-defects.items.tsx")
    assert "FeatureCollection" in route
    assert "application/geo+json" in route
    for param in [
        "bbox",
        "datetime",
        "district",
        "ward",
        "road_segment_id",
        "severity_level",
        "status",
        "limit",
        "offset",
    ]:
        assert f'get("{param}")' in route


def test_postgis_server_uses_parameterized_filters(repo_root: Path):
    server = read(repo_root / "web" / "app" / "lib" / "postgis.server.ts")
    assert "ST_MakeEnvelope" in server
    assert "last_seen_at >= $1::timestamptz" in server
    assert "queryPostgis<RoadDefectFeatureRow>" in server
    assert "serving.current_road_defects" in server
    assert "toRoadDefectFeature" in server
