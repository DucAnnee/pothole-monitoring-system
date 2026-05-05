# Schema Documentation

Standardized storage schemas for the Lakehouse/Streamhouse refactor. Kafka
schema details live in `KAFKA-CONF.md`; executable DDL now lives under
`lakehouse/`.

## Lakehouse Standard

The durable analytical source of truth is Apache Iceberg v2 on MinIO, cataloged
by Polaris and queried by Trino. The target layout is medallion-based:

```text
iceberg.bronze  append-only source events
iceberg.silver  conformed ITS domain entities
iceberg.gold    analytics and serving marts
iceberg.ml      training, annotation, validation, and lineage assets
```

PostgreSQL/PostGIS is a serving projection only. Rebuild
`serving.current_road_defects` from `iceberg.gold.current_road_defects` whenever
the API/workflow database needs recovery.

Authoritative DDL files:

| Layer | DDL |
|---|---|
| Namespaces | `lakehouse/iceberg/001_medallion_namespaces.sql` |
| Bronze | `lakehouse/iceberg/010_bronze_tables.sql` |
| Silver | `lakehouse/iceberg/020_silver_tables.sql` |
| Gold | `lakehouse/iceberg/030_gold_tables.sql` |
| ML | `lakehouse/iceberg/040_ml_tables.sql` |
| PostGIS serving | `lakehouse/postgis/001_serving_schema.sql` |

## Standard Tables

Bronze event tables:

```text
bronze.raw_detection_events
bronze.surface_area_events
bronze.depth_estimation_events
bronze.severity_score_events
bronze.pipeline_latency_events
bronze.device_telemetry_events
bronze.model_deployment_events
bronze.review_events
```

Silver conformed tables:

```text
silver.detections
silver.observations
silver.defect_evidence
silver.devices
silver.vehicles
silver.models
silver.calibrations
silver.road_segments
silver.admin_areas
silver.quality_flags
```

Gold marts:

```text
gold.current_road_defects
gold.defect_observation_history
gold.dashboard_summary_daily
gold.district_severity_daily
gold.model_quality_metrics
gold.device_health_latest
gold.pipeline_latency_summary
```

ML tables:

```text
ml.training_dataset_items
ml.dataset_versions
ml.annotation_versions
ml.model_validation_runs
ml.model_lineage
```

## PostGIS Serving Projection

The API-facing database is `postgres.postgis_serving`, schema `serving`.
`serving.current_road_defects` is the OGC API Features source and includes:

```text
defect_id, defect_type, status, severity_score, severity_level, confidence,
quality_flags, geometry GEOMETRY(Point, 4326), road_segment_id, district, ward,
first_seen_at, last_seen_at, observation_count, latest_raw_image_object_key,
latest_bev_object_key
```

The projection includes a GiST geometry index for bbox queries and secondary
indexes for district, ward, road segment, severity, status, and recency filters.

## Legacy `iceberg.city` Tables

The old `iceberg.city.*` tables remain documented below as migration context.
They are not the target schema and should not receive new table ownership.

## Legacy Kafka-To-Iceberg Flow

| Kafka topic | Iceberg table | Writer |
|---|---|---|
| `pothole.raw.events.v2` | `iceberg.city.raw_events` | `cloud/etl_service/etl_microservice.py` |
| `pothole.surface.area.v2` | `iceberg.city.surface_area_events` | `cloud/etl_service/etl_microservice.py` |
| `pothole.severity.score.v1` | `iceberg.city.severity_scores` | `cloud/etl_service/etl_microservice.py` |
| raw + severity aggregation | `iceberg.city.potholes` | `cloud/final_enrichment_service/final_enrichment_service.py` |
| raw + severity aggregation | `iceberg.city.pothole_history` | `cloud/final_enrichment_service/final_enrichment_service.py` |

## `iceberg.city.raw_events`

Raw edge detections. These records do not contain BEV or surface area fields.

| Column | Type | Description |
|---|---|---|
| `event_id` | VARCHAR | Unique event identifier. |
| `vehicle_id` | VARCHAR | Vehicle identifier used as the Kafka partition key. Currently generated at edge startup. |
| `created_at` | TIMESTAMP(3) | Device event timestamp from raw Avro `timestamp`. |
| `gps_lat` | DOUBLE | Latitude reported by the edge device. Currently simulated. |
| `gps_lon` | DOUBLE | Longitude reported by the edge device. Currently simulated. |
| `gps_accuracy` | DOUBLE | Optional GPS accuracy in meters. |
| `raw_image_object_key` | VARCHAR | MinIO object key or S3-style key for the raw JPEG. |
| `original_mask` | ARRAY(ARRAY(DOUBLE)) | Edge segmentation polygon points as `[[x, y], ...]`. |
| `detection_confidence` | DOUBLE | Optional edge model confidence. |
| `ingested_at` | TIMESTAMP(3) | ETL ingestion timestamp. |

```sql
CREATE TABLE IF NOT EXISTS iceberg.city.raw_events (
    event_id VARCHAR NOT NULL,
    vehicle_id VARCHAR NOT NULL,
    created_at TIMESTAMP(3) NOT NULL,
    gps_lat DOUBLE NOT NULL,
    gps_lon DOUBLE NOT NULL,
    gps_accuracy DOUBLE,
    raw_image_object_key VARCHAR NOT NULL,
    original_mask ARRAY(ARRAY(DOUBLE)) NOT NULL,
    detection_confidence DOUBLE,
    ingested_at TIMESTAMP(3) NOT NULL
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
);
```

## `iceberg.city.surface_area_events`

Cloud-side BEV transformation and surface area output.

| Column | Type | Description |
|---|---|---|
| `event_id` | VARCHAR | Raw event identifier. |
| `raw_image_object_key` | VARCHAR | Raw image object key copied from the raw event. |
| `bev_object_key` | VARCHAR | BEV image object key, usually `bev_images/{event_id}.jpg`; empty string when BEV fails. |
| `bev_mask` | VARCHAR | JSON-encoded BEV mask polygon. |
| `surface_area_cm2` | DOUBLE | Estimated pothole surface area in square centimeters. |
| `confidence` | DOUBLE | BEV estimation confidence; `0.0` marks BEV failure, `1.0` success. |
| `processed_at` | TIMESTAMP(3) | BEV processing timestamp. |
| `ingested_at` | TIMESTAMP(3) | ETL ingestion timestamp. |

```sql
CREATE TABLE IF NOT EXISTS iceberg.city.surface_area_events (
    event_id VARCHAR NOT NULL,
    raw_image_object_key VARCHAR NOT NULL,
    bev_object_key VARCHAR NOT NULL,
    bev_mask VARCHAR NOT NULL,
    surface_area_cm2 DOUBLE NOT NULL,
    confidence DOUBLE,
    processed_at TIMESTAMP(3) NOT NULL,
    ingested_at TIMESTAMP(3) NOT NULL
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(processed_at)']
);
```

## `iceberg.city.severity_scores`

Intermediate ML output after depth and severity scoring.

| Column | Type | Description |
|---|---|---|
| `event_id` | VARCHAR | Event identifier. |
| `depth_cm` | DOUBLE | Estimated depth in centimeters. |
| `surface_area_cm2` | DOUBLE | Surface area passed through from the BEV event via `pothole.depth.v1`. |
| `severity_score` | INTEGER | Integer severity score from 1 to 10. |
| `severity_level` | VARCHAR | `MINOR`, `MODERATE`, `HIGH`, or `CRITICAL`. |
| `calculated_at` | TIMESTAMP(3) | Severity calculation timestamp. |

```sql
CREATE TABLE IF NOT EXISTS iceberg.city.severity_scores (
    event_id VARCHAR NOT NULL,
    depth_cm DOUBLE NOT NULL,
    surface_area_cm2 DOUBLE NOT NULL,
    severity_score INTEGER NOT NULL,
    severity_level VARCHAR NOT NULL,
    calculated_at TIMESTAMP(3) NOT NULL
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(calculated_at)']
);
```

## `iceberg.city.potholes`

Current authoritative pothole state maintained by final enrichment.

| Column | Type | Description |
|---|---|---|
| `pothole_id` | VARCHAR | UUID for the deduplicated pothole. |
| `first_event_id` | VARCHAR | First event that created the pothole record. |
| `reported_at` | TIMESTAMP(3) | First detection timestamp. |
| `gps_lat` | DOUBLE | Latest latitude used for the pothole. |
| `gps_lon` | DOUBLE | Latest longitude used for the pothole. |
| `geom_h3` | BIGINT | H3 cell at dedup resolution, stored as integer via `h3.str_to_int()`. |
| `city` | VARCHAR | OSM city value when available. |
| `ward` | VARCHAR | OSM ward value when available. |
| `district` | VARCHAR | OSM district value when available. |
| `street_name` | VARCHAR | OSM street or road name when available. |
| `road_id` | VARCHAR | OSM road identifier when available. |
| `depth_cm` | DOUBLE | Latest depth estimate. |
| `surface_area_cm2` | DOUBLE | Latest surface area estimate. |
| `severity_score` | DOUBLE | Latest severity score. |
| `severity_level` | VARCHAR | Latest severity level. |
| `pothole_polygon` | VARCHAR | GeoJSON-like polygon string generated from `original_mask`. |
| `raw_image_object_key` | VARCHAR | Raw image key from the most recent detection. |
| `status` | VARCHAR | Current lifecycle status: `reported`, `in_progress`, or `fixed`. |
| `in_progress_at` | TIMESTAMP(3) | Repair start timestamp, if tracked. |
| `fixed_at` | TIMESTAMP(3) | Repair completion timestamp, if tracked. |
| `last_updated_at` | TIMESTAMP(3) | Last update timestamp. |
| `observation_count` | INTEGER | Number of observations merged into this pothole. |

Current implementation note: BEV evidence is persisted in `surface_area_events`,
but `potholes` does not yet retain `bev_object_key` or `bev_mask`. Add those
columns before treating the final table as a complete evidence package.

## `iceberg.city.pothole_history`

Observation history for each deduplicated pothole.

| Column | Type | Description |
|---|---|---|
| `observation_id` | VARCHAR | UUID for this observation. |
| `pothole_id` | VARCHAR | Deduplicated pothole identifier. |
| `event_id` | VARCHAR | Event that generated this observation. |
| `recorded_at` | TIMESTAMP(3) | Observation timestamp. |
| `depth_cm` | DOUBLE | Depth at this observation. |
| `surface_area_cm2` | DOUBLE | Surface area at this observation. |
| `severity_score` | DOUBLE | Severity at this observation. |
| `severity_level` | VARCHAR | Severity level at this observation. |
| `gps_lat` | DOUBLE | Observation latitude. |
| `gps_lon` | DOUBLE | Observation longitude. |
| `pothole_polygon` | VARCHAR | Polygon string at this observation. |
| `status` | VARCHAR | Status recorded for this observation. |

## Latency Metrics In Redis

Final enrichment writes Redis-backed latency metrics:

| Key pattern | Type | Purpose |
|---|---|---|
| `latency:events:recent` | List | Recent event latency payloads. |
| `latency:stats` | Hash | Last event and summary values. |
| `latency:stage:{stage}` | Sorted set | Per-stage samples for percentile calculations. |

Current stage names include `edge_to_kafka_ms`, `kafka_to_raw_storage_ms`,
`raw_to_severity_ms`, `severity_to_pothole_ms`, and `total_pipeline_ms`.
