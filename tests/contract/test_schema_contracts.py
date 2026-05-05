import json

import pytest


def fields(schema_str: str) -> dict:
    return {field["name"]: field for field in json.loads(schema_str)["fields"]}


@pytest.mark.contract
def test_raw_event_v2_schema_matches_edge_bev_etl_and_final(repo_root, schema_constant):
    schema_locations = [
        (repo_root / "edge" / "uploader.py", "RAW_EVENT_SCHEMA_STR"),
        (
            repo_root / "cloud" / "bev_surface_service" / "bev_surface_service.py",
            "RAW_EVENT_SCHEMA_STR",
        ),
        (
            repo_root / "cloud" / "etl_service" / "etl_microservice.py",
            "RAW_EVENT_AVRO_SCHEMA",
        ),
        (
            repo_root / "cloud" / "final_enrichment_service" / "final_enrichment_service.py",
            "RAW_EVENT_AVRO_SCHEMA",
        ),
    ]
    schemas = [json.loads(schema_constant(path, constant)) for path, constant in schema_locations]

    assert {schema["namespace"] for schema in schemas} == {"pothole.raw.v2"}
    assert all("raw_image_object_key" in fields(json.dumps(schema)) for schema in schemas)
    assert all("raw_image_path" not in fields(json.dumps(schema)) for schema in schemas)
    assert len({json.dumps(schema, sort_keys=True) for schema in schemas}) == 1


@pytest.mark.contract
def test_surface_area_v2_schema_matches_bev_depth_and_etl(repo_root, schema_constant):
    schema_locations = [
        (
            repo_root / "cloud" / "bev_surface_service" / "bev_surface_service.py",
            "SURFACE_AREA_SCHEMA_STR",
        ),
        (
            repo_root / "cloud" / "depth_estimation_model" / "cloud_pipeline.py",
            "SURFACE_AREA_SCHEMA_STR",
        ),
        (
            repo_root / "cloud" / "etl_service" / "etl_microservice.py",
            "SURFACE_AREA_AVRO_SCHEMA",
        ),
    ]
    schemas = [json.loads(schema_constant(path, constant)) for path, constant in schema_locations]

    assert {schema["namespace"] for schema in schemas} == {"pothole.surface.v2"}
    assert all(fields(json.dumps(schema))["bev_mask"]["type"] == "string" for schema in schemas)
    assert len({json.dumps(schema, sort_keys=True) for schema in schemas}) == 1


@pytest.mark.contract
def test_depth_and_severity_surface_area_passthrough_contract(repo_root, schema_constant):
    depth_schema = schema_constant(
        repo_root / "cloud" / "depth_estimation_model" / "cloud_pipeline.py",
        "DEPTH_ESTIMATE_SCHEMA_STR",
    )
    severity_input_schema = schema_constant(
        repo_root / "cloud" / "severity_calculation_service" / "severity_aggregator.py",
        "DEPTH_ESTIMATE_SCHEMA_STR",
    )
    severity_output_schema = schema_constant(
        repo_root / "cloud" / "severity_calculation_service" / "severity_aggregator.py",
        "SEVERITY_SCORE_SCHEMA_STR",
    )

    assert fields(depth_schema)["surface_area_cm2"]["type"] == "double"
    assert json.loads(depth_schema) == json.loads(severity_input_schema)
    assert fields(severity_output_schema)["surface_area_cm2"]["type"] == "double"
