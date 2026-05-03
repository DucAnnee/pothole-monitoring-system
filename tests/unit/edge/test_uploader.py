from datetime import datetime, timezone

import numpy as np
import pytest


class DummyConfig:
    config = {
        "kafka": {"topic": "pothole.raw.events.v2"},
        "minio": {"bucket": "warehouse"},
        "gps": {"lat_min": 10.7, "lat_max": 10.8, "lon_min": 106.6, "lon_max": 106.7},
    }


@pytest.mark.unit
def test_uploader_raw_event_schema_is_v2(repo_root, schema_constant):
    schema = schema_constant(repo_root / "edge" / "uploader.py", "RAW_EVENT_SCHEMA_STR")

    assert '"namespace": "pothole.raw.v2"' in schema
    assert "raw_image_object_key" in schema
    assert "original_mask" in schema
    assert "raw_image_path" not in schema


@pytest.mark.unit
def test_process_detection_creates_one_event_per_mask(repo_root, load_module, monkeypatch):
    pytest.importorskip("confluent_kafka")
    module = load_module("edge_uploader_unit", repo_root / "edge" / "uploader.py", repo_root / "edge")
    data_models = load_module(
        "edge_data_models_unit",
        repo_root / "edge" / "data_models.py",
        repo_root / "edge",
    )
    monkeypatch.setattr(module.Uploader, "_initialize_connections", lambda self: None)
    uploader = module.Uploader(DummyConfig(), "vehicle-test")
    detection = data_models.DetectionData(
        frame_id="frame_000001",
        timestamp=datetime.now(timezone.utc),
        frame=np.zeros((8, 8, 3), dtype=np.uint8),
        masks=[
            data_models.DetectionMask(conf=0.8, coordinates=[[1, 1], [2, 1], [2, 2]]),
            data_models.DetectionMask(conf=0.9, coordinates=[[3, 3], [4, 3], [4, 4]]),
        ],
    )

    bundled = uploader.process_detection(detection)

    assert len(bundled) == 2
    assert {item.frame_id for item in bundled} == {"frame_000001"}
    assert bundled[0].event_id != bundled[1].event_id


@pytest.mark.unit
def test_upload_image_returns_s3_object_key(repo_root, load_module, monkeypatch):
    pytest.importorskip("confluent_kafka")
    module = load_module("edge_uploader_minio_unit", repo_root / "edge" / "uploader.py", repo_root / "edge")
    monkeypatch.setattr(module.Uploader, "_initialize_connections", lambda self: None)
    uploader = module.Uploader(DummyConfig(), "vehicle-test")

    class FakeMinio:
        def __init__(self):
            self.puts = []

        def put_object(self, **kwargs):
            self.puts.append(kwargs)

    fake_minio = FakeMinio()
    uploader.minio_client = fake_minio
    image = np.zeros((8, 8, 3), dtype=np.uint8)

    key = uploader._upload_image_to_minio(image, "evt-1", "raw_images")

    assert key == "s3://warehouse/raw_images/evt-1.jpg"
    assert fake_minio.puts[0]["object_name"] == "raw_images/evt-1.jpg"
