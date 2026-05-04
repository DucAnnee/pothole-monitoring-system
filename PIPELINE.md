# Pipeline

Current event flow, based on the code and `graphify-out/GRAPH_REPORT.md`.

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
- Surface area is cloud-side in `cloud/bev_surface_service`, not active in the edge pipeline.
- Depth inference is served by Triton (`cloud/triton_inference_server`) and called by `cloud/depth_estimation_model/cloud_pipeline.py`.
- Final enrichment does not consume `pothole.surface.area.v2` directly. It receives `surface_area_cm2` from `pothole.severity.score.v1`.
- `pothole.raw.events.v1` and `pothole.surface.area.v1` are historical topics and should not appear in new pipeline code or docs except as migration history.
