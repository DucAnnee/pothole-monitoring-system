## 📘 **Schema Documentation (Column Name → Type → Description)**

---

### 🟦 **Table: `iceberg.city.raw_events`**

| Column                 | Data Type | Description                                           |
| ---------------------- | --------- | ----------------------------------------------------- |
| `event_id`             | VARCHAR   | Unique event identifier                               |
| `vehicle_id`           | VARCHAR   | Identifier of the vehicle generating the event        |
| `created_at`           | TIMESTAMP | Original event timestamp from Kafka                   |
| `gps_lat`              | DOUBLE    | Latitude reported by the device                       |
| `gps_lon`              | DOUBLE    | Longitude reported by the device                      |
| `gps_accuracy`         | DOUBLE    | Reported GPS accuracy in meters                       |
| `raw_image_path`       | VARCHAR   | S3 URI of the event image                             |
| `detection_confidence` | DOUBLE    | Model confidence score (optional)                     |
| `ingested_at`          | TIMESTAMP | Timestamp when the event was ingested into this table |

---

### 🟦 **Table: `iceberg.city.processed_events`**

| Column                   | Data Type | Description                                             |
| ------------------------ | --------- | ------------------------------------------------------- |
| `processed_event_id`     | VARCHAR   | Unique identifier for this processed event              |
| `event_id`               | VARCHAR   | Reference to the raw event that produced this record    |
| `created_at`             | TIMESTAMP | Timestamp when this event was processed                 |
| `gps_lat_corrected`      | DOUBLE    | Corrected latitude after geospatial adjustment          |
| `gps_lon_corrected`      | DOUBLE    | Corrected longitude after geospatial adjustment         |
| `depth_mm`               | DOUBLE    | Estimated pothole depth in millimeters                  |
| `severity`               | DOUBLE    | Severity index assigned by the model                    |
| `pothole_polygon`        | VARCHAR   | GeoJSON polygon representing pothole boundary           |
| `segmentation_mask_path` | VARCHAR   | S3 URI pointing to the segmentation mask file           |
| `updated_at`             | TIMESTAMP | Time when the processed record was processed & ingested |

---

### 🟦 **Table: `iceberg.city.potholes`**

*(Current state of each pothole)*

| Column                   | Data Type | Description                                               |
| ------------------------ | --------- | --------------------------------------------------------- |
| `pothole_id`             | VARCHAR   | Unique identifier for this pothole instance               |
| `reported_at`            | TIMESTAMP | Timestamp when the pothole was first reported or detected |
| `in_progress_at`         | TIMESTAMP | Timestamp when repair work officially started             |
| `fixed_at`               | TIMESTAMP | Timestamp when the pothole repair was completed           |
| `gps_lat`                | DOUBLE    | Latitude of the pothole centroid                          |
| `gps_lon`                | DOUBLE    | Longitude of the pothole centroid                         |
| `depth_mm`               | DOUBLE    | Estimated pothole depth in millimeters                    |
| `severity`               | DOUBLE    | Severity index                                            |
| `pothole_polygon`        | VARCHAR   | GeoJSON polygon outlining the pothole boundary            |
| `segmentation_mask_path` | VARCHAR   | S3 URI pointing to the segmentation mask file             |
| `status`                 | VARCHAR   | Lifecycle status: reported, in_progress, fixed            |
| `city`                   | VARCHAR   | City in which the pothole is located                      |
| `ward`                   | VARCHAR   | Municipal ward identifier                                 |
| `street_name`            | VARCHAR   | Street where the pothole is located                       |
| `road_id`                | VARCHAR   | Road segment or asset identifier                          |
| `geom_h3`                | BIGINT    | H3 index of pothole centroid for spatial indexing         |

---

### 🟦 **Table: `iceberg.city.pothole_history`**

*(Time-series evolution of each pothole)*

| Column            | Data Type | Description                                                          |
| ----------------- | --------- | -------------------------------------------------------------------- |
| `observation_id`  | VARCHAR   | Unique identifier for this observation event                         |
| `pothole_id`      | VARCHAR   | Identifier linking this record to the main potholes table            |
| `event_id`        | VARCHAR   | ID of the raw or processed event that generated this observation     |
| `recorded_at`     | TIMESTAMP | Timestamp when this pothole state was observed or recorded           |
| `status`          | VARCHAR   | Lifecycle status at this time: reported, in_progress, fixed, expired |
| `severity`        | DOUBLE    | Severity index at this point in time                                 |
| `depth_mm`        | DOUBLE    | Estimated depth in millimeters at this event                         |
| `pothole_polygon` | VARCHAR   | GeoJSON polygon of the pothole shape for this observation            |
| `gps_lat`         | DOUBLE    | Latitude of the pothole centroid at this observation                 |
| `gps_lon`         | DOUBLE    | Longitude of the pothole centroid at this observation                |
| `geom_h3`         | BIGINT    | H3 index for spatial bucketing                                       |


## 3. Trino SQL for creating tables:

### raw_events

```sql
CREATE TABLE iceberg.city.raw_events (
    event_id VARCHAR COMMENT 'Unique event identifier',
    vehicle_id VARCHAR COMMENT 'Identifier of the vehicle generating the event',
    created_at TIMESTAMP COMMENT 'Original event timestamp from Kafka',
    gps_lat DOUBLE COMMENT 'Latitude reported by the device',
    gps_lon DOUBLE COMMENT 'Longitude reported by the device',
    gps_accuracy DOUBLE COMMENT 'Reported GPS accuracy in meters',
    raw_image_path VARCHAR COMMENT 'S3 URI of the event image',
    detection_confidence DOUBLE COMMENT 'Model confidence score (optional)',
    ingested_at TIMESTAMP COMMENT 'Timestamp when the event was ingested into this table of the data lake'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(timestamp)']
);
```
### processed_events

```sql
CREATE TABLE iceberg.city.processed_events (
    processed_event_id VARCHAR COMMENT 'Unique identifier for this processed event',
    event_id VARCHAR COMMENT 'Reference to the raw event that produced this record',
    created_at TIMESTAMP COMMENT 'Timestamp when this event was processed',
    
    gps_lat_corrected DOUBLE COMMENT 'Corrected latitude after geospatial adjustment',
    gps_lon_corrected DOUBLE COMMENT 'Corrected longitude after geospatial adjustment',
    
    depth_mm DOUBLE COMMENT 'Estimated pothole depth in millimeters',
    severity DOUBLE COMMENT 'Severity index assigned by the model',
    
    pothole_polygon VARCHAR COMMENT 'GeoJSON polygon representing pothole boundary',
    segmentation_mask_path VARCHAR COMMENT 'S3 URI pointing to the segmentation mask file',
    
    updated_at TIMESTAMP COMMENT 'Time when the processed record was processed & ingested into this table of the data lake'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(timestamp)']
);
```

### potholes

```sql
CREATE TABLE iceberg.city.potholes (
    pothole_id VARCHAR COMMENT 'Unique identifier for this pothole instance',

    reported_at TIMESTAMP COMMENT 'Timestamp when the pothole was first reported or detected',
    in_progress_at TIMESTAMP COMMENT 'Timestamp when repair work officially started',
    fixed_at TIMESTAMP COMMENT 'Timestamp when the pothole repair was completed',

    gps_lat DOUBLE COMMENT 'Latitude of the pothole centroid',
    gps_lon DOUBLE COMMENT 'Longitude of the pothole centroid',

    depth_mm DOUBLE COMMENT 'Estimated pothole depth in millimeters',
    severity DOUBLE COMMENT 'Severity index',

    pothole_polygon VARCHAR COMMENT 'GeoJSON polygon outlining the pothole boundary',
    segmentation_mask_path VARCHAR COMMENT 'S3 URI pointing to the segmentation mask file',

    status VARCHAR COMMENT 'Lifecycle status: reported, in_progress, fixed',

    city VARCHAR COMMENT 'City in which the pothole is located',
    ward VARCHAR COMMENT 'Municipal ward identifier',
    street_name VARCHAR COMMENT 'Street where the pothole is located',
    road_id VARCHAR COMMENT 'Road segment or asset identifier',

    geom_h3 BIGINT COMMENT 'H3 index of pothole centroid for spatial indexing',
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ward']
);
```

### pothole_history

```sql
CREATE TABLE iceberg.city.pothole_history (
    observation_id VARCHAR COMMENT 'Unique identifier for this observation event',
    
    pothole_id VARCHAR COMMENT 'Identifier linking this record to a pothole in the main table',

    event_id VARCHAR COMMENT 'ID of the raw or processed event that generated this observation',
    
    recorded_at TIMESTAMP COMMENT 'Timestamp when this pothole state was observed or recorded',

    status VARCHAR COMMENT 'Lifecycle status at this time: reported, in_progress, fixed, expired',

    severity DOUBLE COMMENT 'Severity index at this point in time',
    depth_mm DOUBLE COMMENT 'Estimated depth in millimeters at this event',

    pothole_polygon VARCHAR COMMENT 'GeoJSON polygon of pothole shape at this observation',
    
    gps_lat DOUBLE COMMENT 'Latitude of the pothole centroid at this observation',
    gps_lon DOUBLE COMMENT 'Longitude of the pothole centroid at this observation',
    geom_h3 BIGINT COMMENT 'H3 index for spatial bucketing',
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(recorded_at)']
);

```