### 📋 Kafka Schemas

#### **1. pothole.raw.events.v1**
```json
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "raw_image_path", "type": "string"},
    {"name": "bev_image_path", "type": ["null", "string"], "default": null},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "bev_mask", "type": ["null", {"type": "array", "items": {"type": "array", "items": "double"}}], "default": null},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null}
  ]
}
```
**Partition key:** `vehicle_id` (12 partitions, good for load distribution)

**Changes from v0:**
- Renamed `image_path` → `raw_image_path`
- Added `bev_image_path` (bird's-eye view image, nullable)
- Renamed `pothole_polygon` → `original_mask` (now array of [x,y] points instead of GeoJSON string)
- Added `bev_mask` (bird's-eye view mask coordinates, nullable)
- Added `surface_area_cm2` (computed at edge device)

---

#### **2. pothole.depth.v1** (NEW)
```json
{
  "type": "record",
  "name": "DepthEstimate",
  "namespace": "pothole.depth.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "confidence", "type": ["null", "double"], "default": null},
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```
**Partition key:** `event_id` (so aggregator can consume partitions aligned with other topics)

---

#### **3. pothole.surface.area.v1** ⚠️ DEPRECATED
> **Note:** This topic is deprecated. Surface area is now computed at the edge device and included in `pothole.raw.events.v1` as `surface_area_cm2`. This topic may be removed in a future version.

```json
{
  "type": "record",
  "name": "SurfaceAreaEstimate",
  "namespace": "pothole.surface.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "confidence", "type": ["null", "double"], "default": null},
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```
**Partition key:** `event_id`

---

#### **4. pothole.severity.score.v1**
```json
{
  "type": "record",
  "name": "SeverityScore",
  "namespace": "pothole.severity.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "severity_score", "type": "double"},
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["MINOR", "MODERATE", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```
**Partition key:** `event_id`
**Notes:**
- Added `severity_level` enum for easier filtering
- Includes depth + surface_area so final enrichment service doesn't need to re-join

## 📊 Topic Configuration Summary

| Topic                         | Partitions | Key           | Retention | Compaction |
|-------------------------------|------------|---------------|-----------|------------|
| `pothole.raw.events.v1`       | 12         | `vehicle_id`  | 7 days    | Delete     |
| `pothole.depth.v1`            | 12         | `event_id`    | 3 days    | Delete     |
| `pothole.surface.area.v1`     | 12         | `event_id`    | 3 days    | Delete     |
| `pothole.severity.score.v1`   | 12         | `event_id`    | 7 days    | Delete     |

DLQ topics:

```
pothole.raw.events.dlq.v1
pothole.depth.dlq.v1
pothole.surface.area.dlq.v1
pothole.severity.score.dlq.v1
```

**Why these retention periods?**
- Raw events: 7 days (in case you need to reprocess)
- Intermediate (depth/surface): 3 days (just for joins, then trash)
- Severity: 7 days (final enrichment service might lag)

## Core topics creation commands

### 1. pothole.raw.events.v1

7 days retention

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.raw.events.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=604800000 \
--config cleanup.policy=delete
```

Partition key
`vehicle_id`

---

### 2. pothole.depth.v1

3 days retention

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.depth.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=259200000 \
--config cleanup.policy=delete
```

Partition key
`event_id`

---

### 3. pothole.surface.area.v1

3 days retention

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.surface.area.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=259200000 \
--config cleanup.policy=delete
```

Partition key
`event_id`

---

### 4. pothole.severity.score.v1

7 days retention

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.severity.score.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=604800000 \
--config cleanup.policy=delete
```

Partition key
`event_id`

---

## Dead Letter Queue topics

These should be boring and forgiving. Same partitions for observability consistency. Retention usually longer so you can inspect failures.

### pothole.raw.events.dlq.v1

14 days retention recommended

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.raw.events.dlq.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=1209600000 \
--config cleanup.policy=delete
```

---

### pothole.depth.dlq.v1

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.depth.dlq.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=1209600000 \
--config cleanup.policy=delete
```

---

### pothole.surface.area.dlq.v1

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.surface.area.dlq.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=1209600000 \
--config cleanup.policy=delete
```

---

### pothole.severity.score.dlq.v1

You had a typo in your list. I fixed it. DLQ topic **must not reuse the main topic name**.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--topic pothole.severity.score.dlq.v1 \
--partitions 12 \
--replication-factor 3 \
--config retention.ms=1209600000 \
--config cleanup.policy=delete
```

