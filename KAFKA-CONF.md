# Kafka Configuration

Current Kafka contracts for the v2 pothole pipeline. These topics are retained
unchanged for the first Lakehouse/Streamhouse milestone; Flink consumes them and
writes standardized Bronze Iceberg tables.

## Topic Chain

```text
pothole.raw.events.v2
  -> pothole.surface.area.v2
  -> pothole.depth.v1
  -> pothole.severity.score.v1
```

## Lakehouse Consumers

The standardized storage path is:

```text
current Kafka topics
  -> lakehouse/flink/sql/010_kafka_to_bronze.sql
  -> iceberg.bronze.*
  -> iceberg.silver.*
  -> iceberg.gold.*
  -> serving.current_road_defects
```

Do not introduce topic schema v3 as part of the first storage refactor. Plan v3
separately for stable device identity, real GPS, model lineage, and normalized
quality flags.

## Avro Schemas

### `pothole.raw.events.v2`

Produced by edge and consumed by ETL, BEV Surface, and Final Enrichment.

```json
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v2",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "raw_image_object_key", "type": "string"},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null}
  ]
}
```

Partition key: `vehicle_id`.

Raw v2 deliberately excludes BEV and surface-area fields. Those are produced by
the cloud BEV service.

### `pothole.surface.area.v2`

Produced by BEV Surface and consumed by ETL and Depth Estimation.

```json
{
  "type": "record",
  "name": "SurfaceAreaEstimate",
  "namespace": "pothole.surface.v2",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "raw_image_object_key", "type": "string"},
    {"name": "bev_object_key", "type": "string"},
    {"name": "bev_mask", "type": "string"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "confidence", "type": ["null", "double"], "default": null},
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

Partition key: `event_id`.

`bev_mask` is a JSON string containing a polygon such as `[[x, y], ...]`.
`bev_object_key` can be an empty string when BEV processing fails.

### `pothole.depth.v1`

Produced by Depth Estimation and consumed by Severity Calculation.

```json
{
  "type": "record",
  "name": "DepthEstimate",
  "namespace": "pothole.depth.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "confidence", "type": ["null", "double"], "default": null},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

Partition key: `event_id`.

`surface_area_cm2` is passed through from `pothole.surface.area.v2`.

### `pothole.severity.score.v1`

Produced by Severity Calculation and consumed by ETL and Final Enrichment.

```json
{
  "type": "record",
  "name": "SeverityScore",
  "namespace": "pothole.severity.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "severity_score", "type": "int"},
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["MINOR", "MODERATE", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

Partition key: `event_id`.

## Topic Configuration Summary

| Topic | Partitions | Key | Retention | Cleanup |
|---|---:|---|---:|---|
| `pothole.raw.events.v2` | 12 | `vehicle_id` | 7 days | delete |
| `pothole.surface.area.v2` | 12 | `event_id` | 3 days | delete |
| `pothole.depth.v1` | 12 | `event_id` | 3 days | delete |
| `pothole.severity.score.v1` | 12 | `event_id` | 7 days | delete |

DLQ topics:

```text
pothole.raw.events.dlq.v1
pothole.surface.area.dlq.v1
pothole.depth.dlq.v1
pothole.severity.score.dlq.v1
```

DLQs should keep the same partition count for observability and use longer
retention, usually 14 days.

## Topic Creation Commands

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic pothole.raw.events.v2 \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic pothole.surface.area.v2 \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic pothole.depth.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete

kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic pothole.severity.score.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
```

DLQ example:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic pothole.raw.events.dlq.v1 \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=1209600000 \
  --config cleanup.policy=delete
```
