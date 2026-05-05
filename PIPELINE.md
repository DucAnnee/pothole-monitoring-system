# Pipeline

Standardized event flow, based on the code, `graphify-out/GRAPH_REPORT.md`, and
the Lakehouse refactor.

## Target Streamhouse Flow

```text
Kafka topics
  -> Flink ingestion
  -> iceberg.bronze.*
  -> Flink conformance
  -> iceberg.silver.*
  -> Flink marts
  -> iceberg.gold.*
  -> Flink JDBC projection
  -> PostGIS serving.current_road_defects
  -> OGC API Features routes in web/app/routes/api.v1.*
```

The first migration milestone keeps the current topic names:

```text
pothole.raw.events.v2
pothole.surface.area.v2
pothole.depth.v1
pothole.severity.score.v1
```

The Flink SQL entrypoints live in `lakehouse/flink/sql/`:

| Job | Purpose |
|---|---|
| `010_kafka_to_bronze.sql` | Current Kafka topics to Bronze Iceberg tables. |
| `020_silver_materialization.sql` | Bronze events to conformed detections, observations, and evidence. |
| `030_gold_materialization.sql` | Silver observations to current defects and history. |
| `040_gold_to_postgis_projection.sql` | Gold current defects to PostGIS serving projection. |

`cloud/etl_service` and Iceberg writes in `cloud/final_enrichment_service` are
legacy table owners during migration. New table ownership belongs to Flink.

## Flink Connector Status

Flink services mount connector/runtime JARs from `container-conf/flink/lib/`.
The verified set includes Iceberg `1.10.1`, Flink Kafka `3.3.0-1.19`, Flink
JDBC `3.3.0-1.19`, Flink Avro and Confluent Avro `1.19.3`, PostgreSQL JDBC
`42.7.11`, plus Hadoop client API/runtime `3.3.6`.

Validated with Flink SQL probes:

- Kafka and raw format table creation.
- Kafka and `avro-confluent` table creation.
- JDBC table creation for PostGIS projection writes.
- Iceberg REST catalog creation against live Polaris + MinIO.

The full streaming insert jobs still need an end-to-end run with Kafka topics,
registered schemas, Iceberg DDL, and sample events.

## Legacy Runtime Flow

The currently running Python services still follow this flow until the Flink
jobs replace their storage ownership:

```text
Edge device
  - captures camera/video frames
  - segments potholes with YOLO/RF-DETR
  - uploads raw JPEG to MinIO
  - publishes Avro raw event
        |
        | MinIO: warehouse/raw_images/{event_id}.jpg
        v
Kafka: pothole.raw.events.v2
  key: vehicle_id
        |
        +--> ETL Service
        |      -> Iceberg: iceberg.city.raw_events
        |
        +--> BEV Surface Service
        |      - downloads raw image
        |      - applies camera calibration + homography
        |      - uploads BEV JPEG
        |      -> MinIO: warehouse/bev_images/{event_id}.jpg
        |      -> Kafka: pothole.surface.area.v2
        |
        +--> Final Enrichment Service
               - keeps raw half in memory until severity arrives

Kafka: pothole.surface.area.v2
  key: event_id
        |
        +--> ETL Service
        |      -> Iceberg: iceberg.city.surface_area_events
        |
        +--> Depth Estimation Service
               - prefers bev_object_key
               - falls back to raw_image_object_key when BEV failed
               - batches images and calls Triton over gRPC
               -> Kafka: pothole.depth.v1

Kafka: pothole.depth.v1
  key: event_id
        |
        v
Severity Calculation Service
  - consumes one topic only
  - uses surface_area_cm2 passthrough from depth.v1
  - maps area/depth to discrete scores
  -> Kafka: pothole.severity.score.v1

Kafka: pothole.severity.score.v1
  key: event_id
        |
        +--> ETL Service
        |      -> Iceberg: iceberg.city.severity_scores
        |
        +--> Final Enrichment Service
               - joins raw + severity by event_id
               - H3 deduplicates existing potholes
               - reverse-geocodes with OSM + Redis cache
               - writes current state and history
               -> Iceberg: iceberg.city.potholes
               -> Iceberg: iceberg.city.pothole_history
               -> Redis: latency:* keys
```

## Active Kafka Topics

| Topic | Producer | Consumers | Notes |
|---|---|---|---|
| `pothole.raw.events.v2` | Edge | ETL, BEV Surface, Final Enrichment | Contains raw image object key and original mask only. No BEV or surface area fields. |
| `pothole.surface.area.v2` | BEV Surface | ETL, Depth Estimation | Contains BEV object key, JSON-encoded BEV mask, surface area, and confidence. |
| `pothole.depth.v1` | Depth Estimation | Severity Calculation | Contains depth estimate and `surface_area_cm2` passthrough. |
| `pothole.severity.score.v1` | Severity Calculation | ETL, Final Enrichment | Contains final score and severity level for each event. |

## Important Invariants

- The current chain is sequential: `raw v2 -> surface.area v2 -> depth v1 -> severity.score v1`.
- Iceberg is the analytical source of truth; PostGIS is an API/workflow
  projection and can be rebuilt from Gold.
- OGC API Features endpoints expose current road defects as GeoJSON at
  `/api/v1/collections/road-defects/items`.
- Surface area is cloud-side in `cloud/bev_surface_service`, not active in the edge pipeline.
- Depth inference is served by Triton (`cloud/triton_inference_server`) and called by `cloud/depth_estimation_model/cloud_pipeline.py`.
- Final enrichment does not consume `pothole.surface.area.v2` directly. It receives `surface_area_cm2` from `pothole.severity.score.v1`.
- `pothole.raw.events.v1` and `pothole.surface.area.v1` are historical topics and should not appear in new pipeline code or docs except as migration history.
