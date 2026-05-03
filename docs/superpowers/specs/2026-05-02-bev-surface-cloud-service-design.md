# BEV Surface Area Cloud Service — Design Spec

**Date:** 2026-05-02
**Status:** Approved

## Summary

Move BEV (Bird's Eye View) transformation and pothole surface area estimation from the edge service into a new dedicated cloud microservice (`bev_surface_service`). Update all downstream data contracts accordingly.

**Motivation:** Edge hardware is resource-constrained. Homography computation and BEV transform belong in cloud where compute is elastic. Depth estimation already needs BEV images — making BEV a cloud step gives depth service a clean, guaranteed input.

---

## Architecture

### New Pipeline Topology (Sequential Chain)

```
Edge
  └─ publishes pothole.raw.events.v2
       │ (raw_image_object_key, original_mask, gps_lat/lon, event_id)
       ▼
bev_surface_service  [NEW]
  ├─ downloads raw image from MinIO
  ├─ runs PotholeAreaEstimator (homography → BEV → area_cm2)
  ├─ uploads BEV image to MinIO (bev_images/{event_id}.jpg)
  └─ publishes pothole.surface.area.v2
       │ (event_id, raw_image_object_key, bev_object_key, bev_mask,
       │  surface_area_cm2, confidence)
       ▼
depth_estimation_model  [MODIFIED]
  ├─ downloads BEV image (bev_object_key) from MinIO
  ├─ falls back to raw_image_object_key if confidence=0.0
  ├─ runs Depth-Anything-V2
  └─ publishes pothole.depth.v1  [SCHEMA EXTENDED]
       │ (event_id, mean_depth_cm, std_depth_cm, surface_area_cm2)
       ▼
severity_calculation_service  [SIMPLIFIED]
  ├─ single-topic consumer — no AggregationStore join
  └─ publishes pothole.severity.score.v1  (unchanged)

pothole.raw.events.v2 ──────────────────────────────────┐
pothole.severity.score.v1 ──────────────────────────────┤
                                                         ▼
                                            final_enrichment_service  [MINOR UPDATE]

pothole.raw.events.v2 ──────────────────────────────────▶┐
pothole.surface.area.v2 ────────────────────────────────▶┤ etl_service  [SCHEMA UPDATE]
pothole.severity.score.v1 ──────────────────────────────▶┘ (each topic ingested independently)
```

---

## New Service: `bev_surface_service`

### Directory Structure

```
cloud/bev_surface_service/
├── bev_surface_service.py       # entry point: Kafka consumer/producer loop
├── bev_processor.py             # wraps PotholeAreaEstimator
├── config.py                    # env var config
└── CLAUDE.md
```

### Runtime Behavior

1. Consume `pothole.raw.events.v2` (Avro, manual offset commit after successful publish)
2. Download `raw_image_object_key` from MinIO → decode JPEG → `np.ndarray`
3. Deserialize `original_mask` polygon points
4. Run `PotholeAreaEstimator.compute_pothole_area(img, mask_polygon)` using trapezoid config
5. Upload BEV image to MinIO at `bev_images/{event_id}.jpg`
6. Publish `pothole.surface.area.v2`:
   - Success: `confidence=1.0`, populated `bev_object_key`, `surface_area_cm2`
   - Failure: `confidence=0.0`, empty `bev_object_key`, `surface_area_cm2=0.0`
7. Commit Kafka offset

### `PotholeAreaEstimator` Reuse

Copy from `edge/surface_area/pothole_area_estimator.py` into `cloud/bev_surface_service/`. No shared module — edge (embedded hardware) and cloud (Docker) dependency trees stay decoupled.

### Configuration

| Variable | Purpose |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka cluster |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry |
| `MINIO_ENDPOINT` | MinIO host |
| `MINIO_ACCESS_KEY` | MinIO credentials |
| `MINIO_SECRET_KEY` | MinIO credentials |
| `MINIO_BUCKET` | Target bucket (`warehouse`) |
| `BEV_TRAPEZOID_COORDS` | Homography source points (same calibration as edge) |
| `CAMERA_MATRIX` | Intrinsic matrix for undistortion |
| `DIST_COEFFS` | Distortion coefficients |

---

## Schema Contract Changes

### `pothole.surface.area.v2` — Updated (add passthrough field)

```json
{
  "event_id": "string",
  "raw_image_object_key": "string",   ← ADD (passthrough from raw event)
  "bev_object_key": "string",
  "bev_mask": "bytes",
  "surface_area_cm2": "float",
  "confidence": "float",
  "processed_at": "long (timestamp-ms)"
}
```

### `pothole.depth.v1` — Schema extended (add passthrough field)

```json
{
  "event_id": "string",
  "mean_depth_cm": "float",
  "std_depth_cm": "float",
  "surface_area_cm2": "float"    ← ADD (passthrough from surface.area.v2)
}
```

### `pothole.raw.events.v2` — No change (already updated by user)

### `pothole.severity.score.v1` — No change

---

## Updates to Existing Services

### `depth_estimation_model`

- **Consumed topic:** `pothole.raw.events.v2` → `pothole.surface.area.v2`
- **Image input:** Use `bev_object_key`; fall back to `raw_image_object_key` when `confidence=0.0`
- **Output:** Extend `pothole.depth.v1` — pass through `surface_area_cm2` from consumed event
- **Remove:** `use_bev_image` config flag (BEV image is now always the primary input)

### `severity_calculation_service`

- **Remove** `AggregationStore` two-topic join entirely
- **Single consumer** on `pothole.depth.v1` (already carries `mean_depth_cm` + `surface_area_cm2`)
- Discretization logic and severity formula unchanged

### `final_enrichment_service`

- **Join logic unchanged:** still joins `pothole.raw.events.v2` + `pothole.severity.score.v1` by `event_id` for GPS coordinates
- **Update** field name references: `raw_image_object_key` (replaces old `image_path`)

### `etl_service`

- **Add** `pothole.surface.area.v2` → `iceberg.city.surface_area_events` topic/table mapping
- **Update** `raw_events` PyArrow schema to match v2 field names
- `surface_area_events` table auto-created on startup via Trino DDL (existing pattern)
- PyArrow schema must use `pa.timestamp("us")` for `processed_at` field

### Edge

- **Remove** BEV capture and homography from `inference_worker` / `uploading_worker`
- **Update** Avro schema to v2 fields (`raw_image_object_key`, `original_mask`, `gps_accuracy`; remove `bev_image_path`, `bev_mask`, `surface_area_cm2`)
- **Update** Kafka topic: `pothole.raw.events.v1` → `pothole.raw.events.v2`
- `surface_area/` module: remove wiring, keep or delete files (no functional impact)

---

## Error Handling

| Failure | Behavior |
|---|---|
| MinIO download fails in BEV service | Log error, publish to DLQ, skip offset commit |
| `PotholeAreaEstimator` raises | Publish `confidence=0.0` event, commit offset (downstream handles gracefully) |
| BEV upload to MinIO fails | Log error, publish to DLQ, skip offset commit |
| Depth service gets `confidence=0.0` | Fall back to `raw_image_object_key` for depth inference |

---

## Implementation Order

1. Update `pothole.surface.area.v2` Avro schema (add `raw_image_object_key`)
2. Update `pothole.depth.v1` Avro schema (add `surface_area_cm2`)
3. Update edge — remove BEV, update schema + topic to v2
4. Build `cloud/bev_surface_service/`
5. Update `depth_estimation_model` — consume `surface.area.v2`, pass through `surface_area_cm2`
6. Simplify `severity_calculation_service` — remove join, single-topic
7. Update `final_enrichment_service` — field name fixes
8. Update `etl_service` — add `surface.area.v2` mapping, update `raw_events` schema
9. Register new/updated Avro schemas in Schema Registry
10. Update `KAFKA-CONF.md` with final schema (add `raw_image_object_key` to `surface.area.v2`)
