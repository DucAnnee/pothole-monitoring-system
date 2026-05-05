from pathlib import Path


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_overview_uses_postgis_summary(repo_root: Path):
    route = read(repo_root / "web" / "app" / "routes" / "_app.overview.tsx")
    assert 'from "~/lib/postgis.server"' in route
    assert "querySummary" in route
    assert 'from "~/lib/trino.server"' not in route


def test_postgis_server_exposes_summary_helpers(repo_root: Path):
    server = read(repo_root / "web" / "app" / "lib" / "postgis.server.ts")
    assert "export async function querySummary" in server
    assert "export function emptySummaryData" in server
    assert "serving.current_road_defects" in server
    assert "severity_distribution" in server


def test_health_server_uses_real_probe_helpers(repo_root: Path):
    health = read(repo_root / "web" / "app" / "lib" / "health.server.ts")
    assert "probeKafka" in health
    assert "probeMinio" in health
    assert "probePostgis" in health
    assert "BUS-001" not in health
    assert "10.0.0.1" not in health


def test_demo_standardization_script_checks_runtime_surfaces(repo_root: Path):
    script = read(repo_root / "scripts" / "check-demo-standardization.ps1")
    for text in [
        "pothole.raw.events.v2",
        "pothole.surface.area.v2",
        "pothole.depth.v1",
        "pothole.severity.score.v1",
        "http://localhost:8084/jobs",
        "serving.current_road_defects",
        "raw_images/",
        "bev_images/",
    ]:
        assert text in script
