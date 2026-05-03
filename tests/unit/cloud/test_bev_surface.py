from io import BytesIO

import cv2
import numpy as np
import pytest


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def close(self):
        pass

    def release_conn(self):
        pass


class FakeMinio:
    def __init__(self, image_bytes):
        self.image_bytes = image_bytes
        self.uploads = []

    def get_object(self, bucket, key):
        self.last_get = (bucket, key)
        return FakeResponse(self.image_bytes)

    def put_object(self, **kwargs):
        self.uploads.append(kwargs)


class FakeEstimator:
    def compute_pothole_area(self, image_rgb, mask_np):
        bev_img = np.zeros_like(image_rgb)
        h_matrix = np.eye(3, dtype=np.float32)
        pothole_bev = np.array([[1, 1], [3, 1], [3, 3], [1, 3]], dtype=np.float32)
        return 42.5, bev_img, h_matrix, pothole_bev


@pytest.mark.unit
def test_bev_processor_downloads_raw_uploads_bev_and_returns_json_mask(repo_root, load_module):
    pytest.importorskip("minio")
    module = load_module(
        "bev_processor_unit",
        repo_root / "cloud" / "bev_surface_service" / "bev_processor.py",
        repo_root / "cloud" / "bev_surface_service",
    )
    image = np.zeros((16, 16, 3), dtype=np.uint8)
    _, encoded = cv2.imencode(".jpg", image)
    minio = FakeMinio(encoded.tobytes())
    processor = module.BEVProcessor(minio, "warehouse", FakeEstimator())

    bev_key, bev_mask, area_cm2, confidence = processor.process(
        "evt-1",
        "s3://warehouse/raw_images/evt-1.jpg",
        [[1, 1], [3, 1], [3, 3], [1, 3]],
    )

    assert minio.last_get == ("warehouse", "raw_images/evt-1.jpg")
    assert bev_key == "bev_images/evt-1.jpg"
    assert bev_mask == "[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0]]"
    assert area_cm2 == 42.5
    assert confidence == 1.0
    assert minio.uploads[0]["object_name"] == "bev_images/evt-1.jpg"


@pytest.mark.unit
def test_surface_area_schema_keeps_bev_mask_as_string(repo_root, schema_constant):
    schema = schema_constant(
        repo_root / "cloud" / "bev_surface_service" / "bev_surface_service.py",
        "SURFACE_AREA_SCHEMA_STR",
    )

    assert '"name": "bev_mask", "type": "string"' in schema
