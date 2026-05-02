# Cloud Services - CLAUDE.md

## What Lives Here

Five Python microservices forming the cloud half of the pothole monitoring pipeline:

| Service | Entry Point | Role |
|---------|-------------|------|
| ETL | `etl_service/etl_microservice.py` | Kafka -> Iceberg ingestion |
| BEV Surface | `bev_surface_service/bev_surface_service.py` | BEV transform + pothole area |
| Depth Estimation | `depth_estimation_model/cloud_pipeline.py` | Depth-Anything-V2 inference |
| Severity Calculator | `severity_calculation_service/severity_aggregator.py` | Severity scoring |
| Final Enrichment | `final_enrichment_service/final_enrichment_service.py` | H3 dedup + OSM geocoding + Iceberg write |

Plus `shared/latency_tracker.py` for Redis-backed pipeline latency metrics.

## Running Services

```bash
python cloud/etl_service/etl_microservice.py
python cloud/bev_surface_service/bev_surface_service.py
python cloud/depth_estimation_model/cloud_pipeline.py
python cloud/severity_calculation_service/severity_aggregator.py
python cloud/final_enrichment_service/final_enrichment_service.py
```

## Dependencies

```bash
pip install -r cloud/requirements.txt
```

Key packages: `confluent-kafka`, `pyiceberg`, `trino`, `minio`, `h3`, `redis`, `pyarrow`, `torch`, `opencv-python`

## Configuration

Each service has its own `config.yaml`. All values support env-var override syntax: `${VAR_NAME:default_value}`.

Commonly overridden env vars:
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` - MinIO credentials (default: `minioadmin`)
- `POLARIS_CREDENTIALS` - `client_id:client_secret` for Polaris catalog (default: `root:s3cr3t`)
- `BATCH_SIZE` / `BATCH_TIMEOUT_SECONDS` - ETL batching tuning
- `AGGREGATION_TIMEOUT_SECONDS` - how long final enrichment waits for matching Kafka events

## Topic Flow

```
pothole.raw.events.v2  ----+--> ETL (-> iceberg.city.raw_events)
                           +--> BEV Surface Service (-> pothole.surface.area.v2)

pothole.surface.area.v2 ---+--> ETL (-> iceberg.city.surface_area_events)
                           +--> Depth Estimation (-> pothole.depth.v1)

pothole.depth.v1  -----------> Severity Calculator (-> pothole.severity.score.v1)

pothole.severity.score.v1 -+--> ETL (-> iceberg.city.severity_scores)
                           +--> Final Enrichment

pothole.raw.events.v2  ------> Final Enrichment (aggregates with severity)
```

## Iceberg Tables (all in `iceberg.city` namespace)

| Table | Written By | Partitioning |
|-------|------------|--------------|
| `raw_events` | ETL | `day(created_at)` |
| `surface_area_events` | ETL | `day(processed_at)` |
| `severity_scores` | ETL | `day(calculated_at)` |
| `potholes` | Final Enrichment | `month(reported_at)` |
| `pothole_history` | Final Enrichment | `day(recorded_at)` |

Tables are auto-created via Trino DDL on service startup.

## Key Design Notes

- **Aggregation pattern:** Final enrichment waits for raw + severity messages keyed by `event_id`; severity consumes `depth.v1` directly because depth includes `surface_area_cm2`.
- **Timestamps:** Avro `timestamp-millis` fields may arrive as `datetime` objects or raw `int` ms. Use the service's `_convert_timestamp()` / `format_timestamp()` helpers; do not add raw ms directly to SQL strings.
- **PyArrow precision:** Always use `pa.timestamp("us")` (microseconds) for Iceberg; `"ms"` will fail.
- **H3 storage:** Cell hex strings are stored as `BIGINT` via `h3.str_to_int()`.
- **SQL construction:** Final enrichment uses f-string SQL with `escape_sql_string()`; keep this consistent when adding new fields.

## Detailed Context

See `.claude/contexts/cloud-context.md` for full per-service internals including Avro schemas, class descriptions, severity formula details, and infrastructure connection defaults.
