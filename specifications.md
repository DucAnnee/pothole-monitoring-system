# Final Enrichment Service - Current Specification

This document reflects the active implementation in
`cloud/final_enrichment_service/final_enrichment_service.py`.

For whole-system flow, see `PIPELINE.md`. For table schemas, see `SCHEMA.md`.

## Responsibility

The Final Enrichment Service is the last processing stage. It:

1. Consumes `pothole.raw.events.v2`.
2. Consumes `pothole.severity.score.v1`.
3. Aggregates both messages by `event_id`.
4. Deduplicates potholes with H3 and a Haversine distance check.
5. Reverse-geocodes GPS coordinates through OSM Nominatim with Redis caching.
6. Writes current pothole state to `iceberg.city.potholes`.
7. Writes every observation to `iceberg.city.pothole_history`.
8. Records latency metrics to Redis.

It does not consume `pothole.surface.area.v2` directly. Surface area reaches
this service through the severity score event.

## Inputs

### `pothole.raw.events.v2`

```text
event_id
vehicle_id
timestamp
gps_lat
gps_lon
gps_accuracy
raw_image_object_key
original_mask
detection_confidence
```

### `pothole.severity.score.v1`

```text
event_id
depth_cm
surface_area_cm2
severity_score
severity_level
calculated_at
```

## Processing Flow

```text
Kafka raw v2 + severity score v1
      |
      v
EventAggregationStore
      |
      | complete when raw + severity exist for event_id
      v
find_existing_pothole()
  - h3.latlng_to_cell()
  - h3.str_to_int()
  - query same geom_h3
  - Haversine check within 2m
      |
      v
OSMGeocoder.reverse_geocode()
  - Redis cache by coarse H3 cell
  - rate-limited Nominatim fallback
      |
      v
_upsert_pothole()
_insert_history()
_record_latency()
      |
      v
manual Kafka commit for both source messages
```

## Output Tables

### `iceberg.city.potholes`

Current state table. Key columns:

```text
pothole_id
first_event_id
reported_at
gps_lat
gps_lon
geom_h3
city
ward
district
street_name
road_id
depth_cm
surface_area_cm2
severity_score
severity_level
pothole_polygon
raw_image_object_key
status
in_progress_at
fixed_at
last_updated_at
observation_count
```

### `iceberg.city.pothole_history`

Observation history table. Key columns:

```text
observation_id
pothole_id
event_id
recorded_at
depth_cm
surface_area_cm2
severity_score
severity_level
gps_lat
gps_lon
pothole_polygon
status
```

## Redis Keys

| Key | Purpose |
|---|---|
| `osm:h3:{cell}` | OSM reverse-geocode cache. |
| `latency:events:recent` | Recent end-to-end latency events. |
| `latency:stats` | Last latency event summary. |
| `latency:stage:{stage}` | Stage samples for percentile calculation. |

## Implementation Invariants

- Kafka consumer uses `enable.auto.commit: False`.
- Raw and severity messages are committed after successful Trino writes.
- `geom_h3` is stored as BIGINT; convert H3 strings with `h3.str_to_int()`.
- String values used in SQL must go through `escape_sql_string()`.
- There is no Trino `MERGE INTO`; UPSERT is check-then-insert/update.
- `EventAggregationStore` is in-memory only. Pending unmatched messages are
  lost on restart and can time out.
- The final tables currently retain `raw_image_object_key` but do not retain
  `bev_object_key`, `bev_mask`, model versions, calibration id, or quality flags.

## Production Gaps

- Add idempotency for repeated Kafka deliveries.
- Add DLQ behavior for malformed raw/severity payloads and Trino write failures.
- Persist BEV/model/calibration evidence in final records.
- Add real device identity and signed event metadata.
- Add quality flags for BEV/depth/geocoding failures.
- Add production auth/RBAC and audit logging around review/export/image access.
