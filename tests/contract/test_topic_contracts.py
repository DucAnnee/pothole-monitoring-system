import pytest
import yaml


REQUIRED_TOPICS = {
    "pothole.raw.events.v2",
    "pothole.surface.area.v2",
    "pothole.depth.v1",
    "pothole.severity.score.v1",
    "pothole.raw.events.dlq.v1",
    "pothole.surface.area.dlq.v1",
    "pothole.depth.dlq.v1",
    "pothole.severity.score.dlq.v1",
}


@pytest.mark.contract
def test_docker_compose_creates_current_pipeline_topics(repo_root):
    compose = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")

    for topic in REQUIRED_TOPICS:
        assert f"--topic {topic}" in compose
    assert "--topic pothole.raw.events.v1" not in compose
    assert "--topic pothole.surface.area.v1" not in compose


@pytest.mark.contract
def test_configured_topics_match_producer_consumer_flow(repo_root):
    edge_config = yaml.safe_load((repo_root / "edge" / "config.yaml").read_text())
    bev_config = yaml.safe_load(
        (repo_root / "cloud" / "bev_surface_service" / "config.yaml").read_text()
    )
    depth_config = yaml.safe_load(
        (repo_root / "cloud" / "depth_estimation_model" / "config.yaml").read_text()
    )
    severity_config = yaml.safe_load(
        (repo_root / "cloud" / "severity_calculation_service" / "config.yaml").read_text()
    )
    final_config = yaml.safe_load(
        (repo_root / "cloud" / "final_enrichment_service" / "config.yaml").read_text()
    )

    assert edge_config["kafka"]["topic"] == bev_config["kafka"]["source_topic"]
    assert bev_config["kafka"]["output_topic"] == depth_config["kafka"]["source_topic"]
    assert depth_config["kafka"]["output_topic"] == severity_config["kafka"]["depth_topic"]
    assert severity_config["kafka"]["output_topic"] == final_config["kafka"]["severity_score_topic"]
    assert edge_config["kafka"]["topic"] == final_config["kafka"]["raw_events_topic"]
