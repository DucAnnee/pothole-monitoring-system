import pytest


@pytest.mark.unit
def test_depth_schema_preserves_surface_area(repo_root, schema_constant):
    schema = schema_constant(
        repo_root / "cloud" / "depth_estimation_model" / "cloud_pipeline.py",
        "DEPTH_ESTIMATE_SCHEMA_STR",
    )

    assert '"name": "surface_area_cm2", "type": "double"' in schema


@pytest.mark.unit
def test_depth_input_schema_uses_surface_area_v2(repo_root, schema_constant):
    schema = schema_constant(
        repo_root / "cloud" / "depth_estimation_model" / "cloud_pipeline.py",
        "SURFACE_AREA_SCHEMA_STR",
    )

    assert '"namespace": "pothole.surface.v2"' in schema
    assert '"name": "bev_object_key", "type": "string"' in schema
    assert '"name": "raw_image_object_key", "type": "string"' in schema


@pytest.mark.unit
def test_download_image_parses_s3_and_plain_keys(repo_root, load_module):
    pytest.importorskip("tritonclient")
    module = load_module(
        "depth_pipeline_unit",
        repo_root / "cloud" / "depth_estimation_model" / "cloud_pipeline.py",
        repo_root / "cloud" / "depth_estimation_model",
    )

    class Response:
        def read(self):
            return b"image"

        def close(self):
            pass

        def release_conn(self):
            pass

    class Client:
        def __init__(self):
            self.calls = []

        def get_object(self, bucket, key):
            self.calls.append((bucket, key))
            return Response()

    client = Client()

    assert module.download_image_from_minio(client, "s3://warehouse/bev_images/a.jpg", "warehouse") == b"image"
    assert module.download_image_from_minio(client, "raw_images/a.jpg", "warehouse") == b"image"
    assert client.calls == [
        ("warehouse", "bev_images/a.jpg"),
        ("warehouse", "raw_images/a.jpg"),
    ]
