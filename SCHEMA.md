## 📘 **Schema Documentation (Column Name → Type → Description)**

---

### 🟦 **Table: `iceberg.city.raw_events`**

*(Raw ingestion metadata for each detected pothole event)*

| Column                 | Data Type              | Description                                                          |
| ---------------------- | ---------------------- | -------------------------------------------------------------------- |
| `event_id`             | VARCHAR                | Unique event identifier                                              |
| `vehicle_id`           | VARCHAR                | Vehicle identifier that generated the event                          |
| `created_at`           | TIMESTAMP(3)           | Event timestamp reported by the device                               |
| `gps_lat`              | DOUBLE                 | Latitude reported by the device                                      |
| `gps_lon`              | DOUBLE                 | Longitude reported by the device                                     |
| `gps_accuracy`         | DOUBLE                 | GPS accuracy in meters                                               |
| `raw_image_path`       | VARCHAR                | S3 URI of the raw image (`s3://warehouse/raw_images/{event_id}.jpg`) |
| `bev_image_path`       | VARCHAR                | S3 URI of the bird's-eye view transformed image                      |
| `original_mask`        | ARRAY(ARRAY(DOUBLE))   | Polygon mask coordinates from edge detection (list of [x, y] points) |
| `bev_mask`             | ARRAY(ARRAY(DOUBLE))   | Polygon mask in BEV coordinates (list of [x, y] points)              |
| `surface_area_cm2`     | DOUBLE                 | Estimated surface area computed at edge (cm²)                        |
| `detection_confidence` | DOUBLE                 | Edge detection model confidence score                                |
| `ingested_at`          | TIMESTAMP(3)           | Timestamp when the event was ingested into Iceberg                   |

---

### 🟦 **Table: `iceberg.city.severity_scores`**

*(Intermediate ML output for pothole severity calculation)*

| Column             | Data Type    | Description                                                                |
| ------------------ | ------------ | -------------------------------------------------------------------------- |
| `event_id`         | VARCHAR      | Reference to the associated raw event                                      |
| `depth_cm`         | DOUBLE       | Estimated pothole depth in centimeters                                     |
| `surface_area_cm2` | DOUBLE       | Estimated pothole surface area in square centimeters                       |
| `severity_score`   | INTEGER      | Calculated severity score (1-10). Formula: `clip[1,10](ceil(0.6*area + 0.4*depth))` |
| `severity_level`   | VARCHAR      | Severity category (`MINOR`, `MODERATE`, `HIGH`, `CRITICAL`)                |
| `calculated_at`    | TIMESTAMP(3) | Timestamp when severity was calculated                                     |

---

### 🟦 **Table: `iceberg.city.potholes`**

*(Current authoritative state of each pothole)*

| Column              | Data Type    | Description                                                          |
| ------------------- | ------------ | -------------------------------------------------------------------- |
| `pothole_id`        | VARCHAR      | Unique identifier for the pothole                                    |
| `first_event_id`    | VARCHAR      | Event that first detected this pothole                               |
| `reported_at`       | TIMESTAMP(3) | Timestamp of first detection                                         |
| `gps_lat`           | DOUBLE       | Latitude of the pothole centroid                                     |
| `gps_lon`           | DOUBLE       | Longitude of the pothole centroid                                    |
| `geom_h3`           | BIGINT       | H3 index for spatial indexing                                        |
| `city`              | VARCHAR      | City name                                                            |
| `ward`              | VARCHAR      | Ward identifier                                                      |
| `district`          | VARCHAR      | District name                                                        |
| `street_name`       | VARCHAR      | Street name                                                          |
| `road_id`           | VARCHAR      | Road or OSM way identifier                                           |
| `depth_cm`          | DOUBLE       | Latest estimated depth in centimeters                                |
| `surface_area_cm2`  | DOUBLE       | Latest estimated surface area                                        |
| `severity_score`    | DOUBLE       | Latest severity score                                                |
| `severity_level`    | VARCHAR      | Latest severity level                                                |
| `pothole_polygon`   | VARCHAR      | Latest GeoJSON polygon                                               |
| `raw_image_path`    | VARCHAR      | S3 path to raw perspective image (from most recent detection)        |
| `bev_image_path`    | VARCHAR      | S3 path to bird's-eye view image (from most recent detection)        |
| `status`            | VARCHAR      | Lifecycle status (`reported`, `in_progress`, `fixed`)                |
| `in_progress_at`    | TIMESTAMP(3) | Timestamp when repair work started                                   |
| `fixed_at`          | TIMESTAMP(3) | Timestamp when repair was completed                                  |
| `last_updated_at`   | TIMESTAMP(3) | Timestamp of last update (UPSERT time)                               |
| `observation_count` | INTEGER      | Number of observations associated with this pothole                  |

---

### 🟦 **Table: `iceberg.city.pothole_history`**

*(Time series evolution of pothole properties)*

| Column             | Data Type    | Description                            |
| ------------------ | ------------ | -------------------------------------- |
| `observation_id`   | VARCHAR      | Unique identifier for this observation |
| `pothole_id`       | VARCHAR      | Reference to the pothole               |
| `event_id`         | VARCHAR      | Event that generated this observation  |
| `recorded_at`      | TIMESTAMP(3) | Observation timestamp                  |
| `depth_cm`         | DOUBLE       | Depth at this observation (cm)         |
| `surface_area_cm2` | DOUBLE       | Surface area at this observation       |
| `severity_score`   | DOUBLE       | Severity score at this observation     |
| `severity_level`   | VARCHAR      | Severity level at this observation     |
| `gps_lat`          | DOUBLE       | Latitude at this observation           |
| `gps_lon`          | DOUBLE       | Longitude at this observation          |
| `pothole_polygon`  | VARCHAR      | GeoJSON polygon at this observation    |
| `status`           | VARCHAR      | Lifecycle status at this point in time |

---

## 3. Trino SQL for creating tables:

#### **iceberg.city.raw_events**
```sql
CREATE TABLE iceberg.city.raw_events (
    event_id VARCHAR NOT NULL COMMENT 'Unique event identifier',
    vehicle_id VARCHAR NOT NULL COMMENT 'Vehicle identifier',
    created_at TIMESTAMP(3) NOT NULL COMMENT 'Event timestamp from device',
    
    gps_lat DOUBLE NOT NULL COMMENT 'Latitude',
    gps_lon DOUBLE NOT NULL COMMENT 'Longitude',
    gps_accuracy DOUBLE COMMENT 'GPS accuracy in meters',
    
    raw_image_path VARCHAR NOT NULL COMMENT 'S3 URI (s3://warehouse/raw_images/{event_id}.jpg)',
    bev_image_path VARCHAR COMMENT 'S3 URI of birds-eye view transformed image',
    original_mask ARRAY(ARRAY(DOUBLE)) NOT NULL COMMENT 'Polygon mask from edge detection [[x,y], ...]',
    bev_mask ARRAY(ARRAY(DOUBLE)) COMMENT 'Polygon mask in BEV coordinates [[x,y], ...]',
    surface_area_cm2 DOUBLE NOT NULL COMMENT 'Surface area computed at edge (cm²)',
    detection_confidence DOUBLE COMMENT 'Edge model confidence score',
    
    ingested_at TIMESTAMP(3) NOT NULL COMMENT 'When ingested into Iceberg'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
);
```
**Changes:**
- ✅ Renamed `pothole_polygon` → `original_mask` with type `ARRAY(ARRAY(DOUBLE))`
- ✅ Added `bev_mask` (bird's-eye view mask) with same type
- ✅ Added `bev_image_path` for BEV transformed image
- ✅ Added `surface_area_cm2` (computed at edge device)
- ✅ Changed all timestamps to `TIMESTAMP(3)` (millisecond precision)
- ✅ Added NOT NULL constraints where appropriate

---

#### **iceberg.city.severity_scores**

```sql
CREATE TABLE iceberg.city.severity_scores (
    event_id VARCHAR NOT NULL COMMENT 'Links to raw_events',
    
    depth_cm DOUBLE NOT NULL COMMENT 'Estimated depth in centimeters',
    surface_area_cm2 DOUBLE NOT NULL COMMENT 'Estimated surface area',
    severity_score DOUBLE NOT NULL COMMENT 'Calculated severity (1-10 scale)',
    severity_level VARCHAR NOT NULL COMMENT 'MINOR/MODERATE/HIGH/CRITICAL',
    
    calculated_at TIMESTAMP(3) NOT NULL COMMENT 'When severity was calculated'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(calculated_at)']
);
```
**Why this exists:** 
- Keeps intermediate ML results for debugging
- Can re-calculate severity without re-running depth/surface models

---

#### **iceberg.city.potholes** (Current State Table)
```sql
CREATE TABLE iceberg.city.potholes (
    pothole_id VARCHAR NOT NULL COMMENT 'UUID for this pothole',
    
    -- First detection info
    first_event_id VARCHAR NOT NULL COMMENT 'Event that first detected this pothole',
    reported_at TIMESTAMP(3) NOT NULL COMMENT 'First detection timestamp',
    
    -- Location
    gps_lat DOUBLE NOT NULL COMMENT 'Latitude',
    gps_lon DOUBLE NOT NULL COMMENT 'Longitude',
    geom_h3 BIGINT NOT NULL COMMENT 'H3 index (resolution 12?)',
    
    -- Address (from OpenStreetMap)
    city VARCHAR COMMENT 'City name',
    ward VARCHAR COMMENT 'Ward',
    district VARCHAR COMMENT 'District',
    street_name VARCHAR COMMENT 'Street name',
    road_id VARCHAR COMMENT 'OSM way_id or road identifier',
    
    -- Physical properties (from latest observation)
    depth_cm DOUBLE NOT NULL COMMENT 'Latest depth in centimeters',
    surface_area_cm2 DOUBLE NOT NULL COMMENT 'Latest surface area',
    severity_score DOUBLE NOT NULL COMMENT 'Latest severity',
    severity_level VARCHAR NOT NULL COMMENT 'Latest severity level',
    pothole_polygon VARCHAR NOT NULL COMMENT 'Latest GeoJSON polygon',
    
    -- Lifecycle
    status VARCHAR NOT NULL COMMENT 'reported | in_progress | fixed',
    in_progress_at TIMESTAMP(3) COMMENT 'When repair started',
    fixed_at TIMESTAMP(3) COMMENT 'When repair completed',
    
    -- Metadata
    last_updated_at TIMESTAMP(3) NOT NULL COMMENT 'Last UPSERT timestamp',
    observation_count INTEGER NOT NULL COMMENT 'How many times we seen this pothole'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ward', 'month(reported_at)']
);
```
**Key changes:**
- ✅ Added `first_event_id` to trace back to original detection
- ✅ Added `observation_count` (how many times vehicle passed by)
- ✅ Partitioned by `ward` + `month(reported_at)` (better for querying by location + time)
- ✅ Removed `segmentation_mask_path` (you can reconstruct from `raw_image_path` if needed)

---

#### **iceberg.city.pothole_history** (Time Series)
```sql
CREATE TABLE iceberg.city.pothole_history (
    observation_id VARCHAR NOT NULL COMMENT 'UUID for this observation',
    pothole_id VARCHAR NOT NULL COMMENT 'Links to potholes table',
    event_id VARCHAR NOT NULL COMMENT 'Links to raw_events',
    
    recorded_at TIMESTAMP(3) NOT NULL COMMENT 'Observation timestamp',
    
    -- Physical properties at this time
    depth_cm DOUBLE NOT NULL COMMENT 'Depth at observation (cm)',
    surface_area_cm2 DOUBLE NOT NULL COMMENT 'Surface area at observation',
    severity_score DOUBLE NOT NULL COMMENT 'Severity at observation',
    severity_level VARCHAR NOT NULL COMMENT 'Severity level at observation',
    
    gps_lat DOUBLE NOT NULL COMMENT 'Lat at observation',
    gps_lon DOUBLE NOT NULL COMMENT 'Lon at observation',
    pothole_polygon VARCHAR NOT NULL COMMENT 'Polygon at observation',
    
    -- Lifecycle
    status VARCHAR NOT NULL COMMENT 'Status at this observation'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(recorded_at)', 'pothole_id']
);
```

**Why partition by `pothole_id`?**
- Queries like "show me history of pothole XYZ" become fast
- Trade-off: many small files if you have 10k potholes

---

## 4. Pipeline Latency Tracking (Redis)

The system tracks end-to-end pipeline latency from edge detection to frontend availability using Redis as a real-time metrics store.

### Latency Stages

| Stage | Description | Timestamp Source |
|-------|-------------|------------------|
| `edge_to_kafka` | Time from edge detection to Kafka producer | `timestamp` field in raw event |
| `kafka_to_storage` | Time from Kafka to Iceberg raw_events table | `ingested_at` - `kafka_produced_at` |
| `depth_estimation` | Time from edge detection to severity calculation | `calculated_at` - `timestamp` |
| `enrichment` | Time from severity calculation to potholes table | `pothole_stored_at` - `calculated_at` |
| `total` | Total end-to-end pipeline latency | `pothole_stored_at` - `edge_detected_at` |

### Redis Keys

| Key Pattern | Type | Description |
|-------------|------|-------------|
| `latency:events:recent` | List | Recent latency events (FIFO, max 100) |
| `latency:stats` | Hash | Summary statistics (last_updated, last_event_id) |
| `latency:stage:{stage_name}` | Sorted Set | Per-stage latency values for percentile calculation |

### Latency Event Schema (JSON)

```json
{
  "event_id": "abc123-...",
  "edge_detected_at": 1702982400000,
  "kafka_produced_at": 1702982400100,
  "raw_event_ingested_at": 1702982400500,
  "severity_calculated_at": 1702982401000,
  "pothole_stored_at": 1702982401500,
  "edge_to_kafka_ms": 100,
  "kafka_to_raw_storage_ms": 400,
  "raw_to_severity_ms": 1000,
  "severity_to_pothole_ms": 500,
  "total_pipeline_ms": 1500
}
```

### API Endpoint

**GET /api/v1/latency**

Returns:
- `stages`: Per-stage latency stats (count, avg, min, max, p50, p95, p99)
- `recent_events`: Last 20 latency events
- `total_pipeline`: Aggregate pipeline latency metrics
- `microservices`: Frontend-friendly service latency array

