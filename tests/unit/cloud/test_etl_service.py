from datetime import datetime

import pytest

from tests.fixtures.sample_records import RAW_EVENT_V2, SEVERITY_SCORE_V1, SURFACE_AREA_V2


@pytest.mark.unit
def test_etl_arrow_timestamps_use_microseconds(repo_root):
    source = (repo_root / "cloud" / "etl_service" / "etl_microservice.py").read_text(
        encoding="utf-8"
    )

    assert 'pa.timestamp("us")' in source
    assert 'pa.timestamp("ms")' not in source


@pytest.mark.unit
def test_etl_topic_table_mappings_are_current(repo_root):
    source = (repo_root / "cloud" / "etl_service" / "etl_microservice.py").read_text(
        encoding="utf-8"
    )

    assert 'kafka_topic="pothole.raw.events.v2"' in source
    assert 'kafka_topic="pothole.surface.area.v2"' in source
    assert 'kafka_topic="pothole.severity.score.v1"' in source
    assert 'kafka_topic="pothole.raw.events.v1"' not in source
    assert 'kafka_topic="pothole.surface.area.v1"' not in source


@pytest.mark.unit
def test_etl_transform_functions_match_new_columns(repo_root, load_module):
    pytest.importorskip("pyarrow")
    pytest.importorskip("pyiceberg")
    pytest.importorskip("confluent_kafka")
    pytest.importorskip("trino")
    module = load_module(
        "etl_microservice_unit",
        repo_root / "cloud" / "etl_service" / "etl_microservice.py",
        repo_root / "cloud" / "etl_service",
    )

    raw = module.transform_raw_event(RAW_EVENT_V2)
    surface = module.transform_surface_area(SURFACE_AREA_V2)
    severity = module.transform_severity_score(SEVERITY_SCORE_V1)

    assert raw["raw_image_object_key"].endswith("/raw_images/evt-test-001.jpg")
    assert "raw_image_path" not in raw
    assert surface["bev_object_key"] == "bev_images/evt-test-001.jpg"
    assert surface["bev_mask"].startswith("[[")
    assert severity["surface_area_cm2"] == SURFACE_AREA_V2["surface_area_cm2"]
    assert isinstance(raw["created_at"], datetime)
