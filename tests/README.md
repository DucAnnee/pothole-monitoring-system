# Test Suite

Automated tests for the pothole monitoring pipeline. Three tiers: **unit**, **contract**, and **end-to-end**.

## Quick Start

```powershell
# From repo root
python -m pytest -m "not e2e" -v
```

All non-E2E tests pass with no external services. Three tests skip if `pyarrow` or `h3` are not installed in the local Python env — those packages are only required for cloud services, not for running the test runner itself.

## Last Run Result

```
platform win32 -- Python 3.14.0, pytest-9.0.3
rootdir: D:\H251\Specialized-Project\pothole-monitoring-system
configfile: pytest.ini

tests/contract/test_schema_contracts.py::test_raw_event_v2_schema_matches_edge_bev_etl_and_final PASSED
tests/contract/test_schema_contracts.py::test_surface_area_v2_schema_matches_bev_depth_and_etl  PASSED
tests/contract/test_schema_contracts.py::test_depth_and_severity_surface_area_passthrough_contract PASSED
tests/contract/test_topic_contracts.py::test_docker_compose_creates_current_pipeline_topics       PASSED
tests/contract/test_topic_contracts.py::test_configured_topics_match_producer_consumer_flow       PASSED
tests/unit/cloud/test_bev_surface.py::test_bev_processor_downloads_raw_uploads_bev_and_returns_json_mask PASSED
tests/unit/cloud/test_bev_surface.py::test_surface_area_schema_keeps_bev_mask_as_string           PASSED
tests/unit/cloud/test_depth_service.py::test_depth_schema_preserves_surface_area                  PASSED
tests/unit/cloud/test_depth_service.py::test_depth_input_schema_uses_surface_area_v2              PASSED
tests/unit/cloud/test_depth_service.py::test_download_image_parses_s3_and_plain_keys              PASSED
tests/unit/cloud/test_etl_service.py::test_etl_arrow_timestamps_use_microseconds                  PASSED
tests/unit/cloud/test_etl_service.py::test_etl_topic_table_mappings_are_current                   PASSED
tests/unit/cloud/test_etl_service.py::test_etl_transform_functions_match_new_columns              SKIPPED (no pyarrow)
tests/unit/cloud/test_final_enrichment.py::test_aggregation_store_returns_combined_records_with_messages SKIPPED (no h3)
tests/unit/cloud/test_final_enrichment.py::test_final_enrichment_helpers_escape_and_preserve_h3_bigint  SKIPPED (no h3)
tests/unit/cloud/test_final_enrichment.py::test_final_enrichment_consumer_is_manual_commit        PASSED
tests/unit/cloud/test_severity_service.py::test_severity_discretization_boundaries                PASSED
tests/unit/cloud/test_severity_service.py::test_severity_levels                                   PASSED
tests/unit/cloud/test_severity_service.py::test_severity_produce_flush_raises_before_commit_on_delivery_error PASSED
tests/unit/edge/test_config_loader.py::test_edge_config_does_not_require_video_key                PASSED
tests/unit/edge/test_config_loader.py::test_edge_config_env_default_substitution                  PASSED
tests/unit/edge/test_pipeline_queue_drain.py::test_upload_worker_drains_queue_after_inference_stops PASSED
tests/unit/edge/test_segmenter_roi.py::test_pothole_in_trapezoid_filters_outside_masks            PASSED
tests/unit/edge/test_uploader.py::test_uploader_raw_event_schema_is_v2                            PASSED
tests/unit/edge/test_uploader.py::test_process_detection_creates_one_event_per_mask               PASSED
tests/unit/edge/test_uploader.py::test_upload_image_returns_s3_object_key                         PASSED
cloud/bev_surface_service/test_bev_processor.py::test_process_success_returns_bev_key_and_area    PASSED
cloud/bev_surface_service/test_bev_processor.py::test_process_download_failure_returns_zeros      PASSED
cloud/bev_surface_service/test_bev_processor.py::test_process_estimator_failure_returns_zeros     PASSED

26 passed, 3 skipped, 1 deselected in 4.76s
```

---

## Directory Layout

```
tests/
├── conftest.py              # shared fixtures (repo_root, load_module, schema_constant)
├── config/                  # per-service YAML configs used only during E2E runs
│   ├── edge.e2e.yaml
│   ├── bev_surface.e2e.yaml
│   ├── depth.e2e.yaml
│   ├── severity.e2e.yaml
│   ├── etl.e2e.yaml
│   └── final_enrichment.e2e.yaml
├── fixtures/
│   ├── clients.py           # helpers for Kafka / MinIO / Trino / Redis in integration tests
│   └── sample_records.py    # canonical event dicts for all four Avro schemas
├── contract/                # schema and topic consistency checks
│   ├── test_schema_contracts.py
│   └── test_topic_contracts.py
├── unit/
│   ├── cloud/               # unit tests for each cloud microservice
│   └── edge/                # unit tests for the edge service
└── e2e/
    └── test_real_video_pipeline.py
```

---

## Markers

Defined in `pytest.ini`:

| Marker | Meaning |
|--------|---------|
| `unit` | Fast, no external services |
| `contract` | Schema and topic compatibility, reads source files only |
| `integration` | Needs one or more running local services |
| `e2e` | Full edge-to-cloud run, needs Docker + ML model weights |
| `requires_models` | Needs local `.pth` / `.pt` model files |
| `requires_docker` | Needs Docker Compose stack |

Filter by marker:

```powershell
python -m pytest -m unit -v
python -m pytest -m contract -v
python -m pytest -m "not e2e" -v   # everything except E2E (CI default)
```

---

## Test Descriptions

### Contract Tests (`tests/contract/`)

These tests read source files directly (via `ast.parse`) and compare string constants — no imports, no running services.

**`test_schema_contracts.py`**

| Test | What it checks |
|------|---------------|
| `test_raw_event_v2_schema_matches_edge_bev_etl_and_final` | `RAW_EVENT_SCHEMA_STR` in `edge/uploader.py`, `cloud/bev_surface_service/bev_surface_service.py`, `cloud/etl_service/etl_microservice.py`, and `cloud/final_enrichment_service/final_enrichment_service.py` all define identical field sets under namespace `pothole.raw.v2`. |
| `test_surface_area_v2_schema_matches_bev_depth_and_etl` | `SURFACE_AREA_SCHEMA_STR` in the BEV service, depth pipeline, and ETL service all agree on the `pothole.surface.v2` schema including `raw_image_object_key` and `bev_mask`. |
| `test_depth_and_severity_surface_area_passthrough_contract` | `DEPTH_ESTIMATE_SCHEMA_STR` in `cloud_pipeline.py` and `severity_aggregator.py` both contain `surface_area_cm2`, confirming the passthrough field survives the depth→severity hop. |

**`test_topic_contracts.py`**

| Test | What it checks |
|------|---------------|
| `test_docker_compose_creates_current_pipeline_topics` | Parses `docker-compose.yml` and asserts the `kafka-init` service creates all four pipeline topics (`pothole.raw.events.v2`, `pothole.surface.area.v2`, `pothole.depth.v1`, `pothole.severity.score.v1`) plus their `.dlq.v1` counterparts. |
| `test_configured_topics_match_producer_consumer_flow` | Reads each service's `config.yaml` and asserts producer outputs match downstream consumer inputs (e.g., BEV service output topic == depth service source topic). |

---

### Unit Tests — Cloud (`tests/unit/cloud/`)

All tests mock or stub external dependencies (Kafka, MinIO, etc.) and run instantly.

**`test_bev_surface.py`**

| Test | What it checks |
|------|---------------|
| `test_bev_processor_downloads_raw_uploads_bev_and_returns_json_mask` | `BEVProcessor.process()` downloads the raw image, calls `PotholeAreaEstimator`, uploads the BEV image, and returns `(bev_key, json_mask, area_cm2, confidence)`. Mocks MinIO and the estimator. |
| `test_surface_area_schema_keeps_bev_mask_as_string` | `SURFACE_AREA_SCHEMA_STR` in `bev_surface_service.py` defines `bev_mask` as Avro `"string"` (not bytes). |

**`test_depth_service.py`**

| Test | What it checks |
|------|---------------|
| `test_depth_schema_preserves_surface_area` | `DEPTH_ESTIMATE_SCHEMA_STR` in `cloud_pipeline.py` contains a `surface_area_cm2` field of type `double`. |
| `test_depth_input_schema_uses_surface_area_v2` | The depth pipeline consumes `SURFACE_AREA_SCHEMA_STR` (namespace `pothole.surface.v2`), not the old raw event schema. |
| `test_download_image_parses_s3_and_plain_keys` | `download_image_from_minio()` strips the `s3://bucket/` prefix before calling MinIO for both `s3://warehouse/path` and bare `path` keys. |

**`test_etl_service.py`**

| Test | What it checks |
|------|---------------|
| `test_etl_arrow_timestamps_use_microseconds` | Every `pa.timestamp` in the ETL Arrow schemas uses `"us"` (microseconds), not `"ms"`. PyIceberg rejects millisecond precision. |
| `test_etl_topic_table_mappings_are_current` | `TOPIC_TABLE_MAPPINGS` contains entries for `pothole.raw.events.v2`, `pothole.surface.area.v2`, and `pothole.severity.score.v1` — no stale v1 raw topic. |
| `test_etl_transform_functions_match_new_columns` *(skipped: no pyarrow)* | `transform_raw_event()` output matches `RAW_EVENTS_ARROW_SCHEMA` column names; `transform_surface_area()` output matches `SURFACE_AREA_ARROW_SCHEMA`. |

**`test_final_enrichment.py`**

| Test | What it checks |
|------|---------------|
| `test_aggregation_store_returns_combined_records_with_messages` *(skipped: no h3)* | `EventAggregationStore.add_raw_event()` / `.add_severity()` return `None` until both halves arrive, then return the combined dict and remove the entry. |
| `test_final_enrichment_helpers_escape_and_preserve_h3_bigint` *(skipped: no h3)* | `escape_sql_string()` doubles single quotes; `h3.str_to_int()` round-trips correctly for the cell returned by `h3.latlng_to_cell()`. |
| `test_final_enrichment_consumer_is_manual_commit` | `FinalEnrichmentService` Kafka consumer config has `enable.auto.commit: false`. |

**`test_severity_service.py`**

| Test | What it checks |
|------|---------------|
| `test_severity_discretization_boundaries` | `map_area_to_discrete()` and `map_depth_to_discrete()` return correct bucket values at every boundary (e.g., area 299.99→1, 300→2; depth 0.99→1, 1.0→2). |
| `test_severity_levels` | `get_severity_level()` maps scores 1–3 → MINOR, 4–5 → MODERATE, 6–7 → HIGH, 8–10 → CRITICAL. |
| `test_severity_produce_flush_raises_before_commit_on_delivery_error` | `produce_and_flush()` raises `RuntimeError` on delivery failure, which prevents the subsequent `consumer.commit()` — offset is not advanced on bad delivery. |

---

### Unit Tests — Edge (`tests/unit/edge/`)

**`test_uploader.py`**

| Test | What it checks |
|------|---------------|
| `test_uploader_raw_event_schema_is_v2` | `RAW_EVENT_SCHEMA_STR` in `edge/uploader.py` uses namespace `pothole.raw.v2` and contains `raw_image_object_key` (not the old `image_path`). |
| `test_process_detection_creates_one_event_per_mask` | `Uploader.process_detection()` returns one `BundledData` per mask in the detection, each with a unique `event_id`. |
| `test_upload_image_returns_s3_object_key` | `_upload_image_to_minio()` returns an `s3://bucket/prefix/event_id.jpg` key. MinIO client is mocked. |

**`test_config_loader.py`**

| Test | What it checks |
|------|---------------|
| `test_edge_config_does_not_require_video_key` | Edge `ConfigLoader` loads a YAML without a `video` key without raising. |
| `test_edge_config_env_default_substitution` | `${VAR_NAME:default}` syntax in config YAML resolves to the default when the env var is not set. |

**`test_pipeline_queue_drain.py`**

| Test | What it checks |
|------|---------------|
| `test_upload_worker_drains_queue_after_inference_stops` | The upload worker thread processes all items remaining in the queue after the inference thread signals EOF, before it exits. |

**`test_segmenter_roi.py`**

| Test | What it checks |
|------|---------------|
| `test_pothole_in_trapezoid_filters_outside_masks` | The segmenter ROI filter passes detections whose centroid falls inside the trapezoid and drops those outside. |

---

### BEV Service Tests (`cloud/bev_surface_service/test_bev_processor.py`)

Co-located with the service they test, also picked up by `pytest.ini` via `testpaths`.

| Test | What it checks |
|------|---------------|
| `test_process_success_returns_bev_key_and_area` | Happy path: MinIO download succeeds, estimator returns area and mask, BEV image is uploaded, returns `(bev_key, json_mask, area_cm2, 1.0)`. |
| `test_process_download_failure_returns_zeros` | MinIO raises `S3Error` → `process()` returns `(None, None, 0.0, 0.0)` without crashing. |
| `test_process_estimator_failure_returns_zeros` | `PotholeAreaEstimator.compute_pothole_area()` raises → `process()` returns `(None, None, 0.0, 0.0)`. |

---

## End-to-End Test

`tests/e2e/test_real_video_pipeline.py::test_real_video_edge_to_cloud_pipeline`

Runs the full pipeline from a real video file through all five cloud services and verifies data landed in every Iceberg table.

### Prerequisites

All four artifacts must be present before the test can run:

| Artifact | Path |
|----------|------|
| Test video | `edge/assets/test.mp4` |
| YOLO weights | `edge/models/yolo11s.pt` |
| Camera calibration | `edge/.conf/camera_calibration.json` |
| Depth-Anything-V2 weights | `cloud/depth_estimation_model/Depth-Anything-V2/depth_anything_v2_vitl.pth` |

### Running

```powershell
# Start infra + run E2E (tears down on exit)
.\scripts\run-e2e.ps1

# Keep the Docker stack running after the test
.\scripts\run-e2e.ps1 -KeepStack

# Skip docker build if images are already built
.\scripts\run-e2e.ps1 -SkipBuild

# Custom timeout (default 600s)
.\scripts\run-e2e.ps1 -TimeoutSeconds 900
```

The script:
1. Starts `docker-compose.yml` + `docker-compose.test.yml` under project name `pms-e2e` (isolated volumes)
2. Polls Schema Registry (`localhost:8082`), MinIO (`localhost:9000`), Polaris (`localhost:8182`), Trino (`localhost:8081`) until ready
3. Sets `RUN_REAL_E2E=1` and runs pytest
4. Tears down containers and volumes on exit (unless `-KeepStack`)

### What the test asserts

1. All pipeline + DLQ topics exist in Kafka
2. Redis is reachable
3. Edge processes the video and exits with code 0
4. At least one image appears under `raw_images/` in MinIO (within 120s)
5. At least one BEV image appears under `bev_images/` in MinIO (within 180s)
6. All four pipeline topics have at least one record
7. `raw_events`, `surface_area_events`, `severity_scores`, `potholes`, `pothole_history` tables all have rows (each within 240s)
8. A cross-table JOIN on `event_id` returns at least one row — `surface_area_cm2` from `surface_area_events` equals `surface_area_cm2` from `severity_scores` (passthrough verified end-to-end), `raw_image_object_key` is non-null, `geom_h3` is an integer

Service logs are written to `tests/.artifacts/<service>.log` for post-mortem inspection.

### Running without the helper script

```powershell
# 1. Start infra manually
docker compose up -d

# 2. Set env var and run
$env:RUN_REAL_E2E = "1"
python -m pytest tests/e2e/test_real_video_pipeline.py -m e2e -v
```

---

## Shared Fixtures (`tests/conftest.py`)

| Fixture | Type | Description |
|---------|------|-------------|
| `repo_root` | `Path` | Absolute path to the repository root |
| `load_module` | callable | Dynamically imports a Python source file by path without polluting `sys.modules` |
| `schema_constant` | callable | Extracts a string constant from a source file using `ast.parse` — no imports needed |

The `load_module` fixture saves and restores local module names (`config_loader`, `data_models`, `uploader`, etc.) so tests loading modules from different services don't bleed state into each other.

## Canonical Sample Records (`tests/fixtures/sample_records.py`)

Pre-built event dicts matching the current Avro schemas, used as test inputs:

| Name | Schema |
|------|--------|
| `RAW_EVENT_V2` | `pothole.raw.v2` — has `raw_image_object_key`, `original_mask`, `detection_confidence` |
| `SURFACE_AREA_V2` | `pothole.surface.v2` — has `bev_object_key`, `bev_mask`, `surface_area_cm2`, `raw_image_object_key` |
| `DEPTH_V1` | `pothole.depth.v1` — has `depth_cm`, `surface_area_cm2` passthrough |
| `SEVERITY_SCORE_V1` | `pothole.severity.v1` — has `severity_score`, `severity_level` |
