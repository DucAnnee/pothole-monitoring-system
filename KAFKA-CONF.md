# ⭐ 1. TOPIC DEFINITIONS (FINAL)

| Topic                        | Purpose                    | Partition Key | Partitions |
| ---------------------------- | -------------------------- | ------------- | ---------- |
| `pothole.raw.events.v1`      | Edge → Kafka raw events    | `vehicle_id`  | 12         |
| `pothole.ml.processed.v1`    | ML segmentation/depth      | `event_id`    | 24         |
| `pothole.geo.enriched.v1`    | Ward/district/road mapping | `h3_index`    | 24         |
| `pothole.cluster.updates.v1` | Stable pothole ID updates  | `pothole_id`  | 6          |
| `pothole.alerts.v1`          | Severe potholes, warnings  | `pothole_id`  | 3          |

DLQ topics:

```
pothole.raw.events.dlq.v1
pothole.ml.processed.dlq.v1
pothole.geo.enriched.dlq.v1
```

---

# ⭐ 2. AVRO SCHEMAS FOR EACH TOPIC

Why Avro?

* Strong typing
* Schema evolution
* Works with Schema Registry (Karapace, Redpanda)
* Ensures producers/consumers stay compatible

---

# 🟦 2.1 AVRO Schema: pothole.raw.events.v1

**Filename: raw_events_v1.avsc**

```json
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": "string"}, 
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "image_path", "type": "string"},
    {"name": "detection_confidence", "type": ["null","double"], "default": null}
  ]
}
```

Partition key = `vehicle_id`

---

# 🟩 2.2 AVRO Schema: pothole.ml.processed.v1

**Filename: processed_events_v1.avsc**

```json
{
  "type": "record",
  "name": "ProcessedEvent",
  "namespace": "pothole.ml.v1",
  "fields": [
    {"name": "processed_event_id", "type": "string"},
    {"name": "event_id", "type": "string"},
    {"name": "gps_lat_corrected", "type": "double"},
    {"name": "gps_lon_corrected", "type": "double"},
    {"name": "depth_mm", "type": "double"},
    {"name": "severity", "type": "string"},
    {"name": "pothole_polygon", "type": "string"}, 
    {"name": "processed_timestamp", "type": "string"}
  ]
}
```

Partition key = `event_id`

---

# 🟨 2.3 AVRO Schema: pothole.geo.enriched.v1

**Filename: geo_enriched_v1.avsc**

```json
{
  "type": "record",
  "name": "GeoEnrichedEvent",
  "namespace": "pothole.geo.v1",
  "fields": [
    {"name": "processed_event_id", "type": "string"},
    {"name": "pothole_id", "type": ["null", "string"], "default": null},
    {"name": "ward", "type": "string"},
    {"name": "district", "type": "string"},
    {"name": "street_name", "type": "string"},
    {"name": "road_id", "type": "string"},
    {"name": "h3_index", "type": "long"},
    {"name": "enriched_timestamp", "type": "string"}
  ]
}
```

Partition key = `h3_index`

---

# 🟧 2.4 AVRO Schema: pothole.cluster.updates.v1

**Filename: cluster_updates_v1.avsc**

```json
{
  "type": "record",
  "name": "ClusterUpdate",
  "namespace": "pothole.cluster.v1",
  "fields": [
    {"name": "pothole_id", "type": "string"},
    {"name": "event_ids", "type": {"type": "array", "items": "string"}},
    {"name": "first_detected_ts", "type": "string"},
    {"name": "last_detected_ts", "type": "string"},
    {"name": "severity", "type": "string"},
    {"name": "depth_mm", "type": "double"},
    {"name": "status", "type": "string"}, 
    {"name": "pothole_polygon", "type": "string"}
  ]
}
```

Partition key = `pothole_id`

---

# ⭐ 3. KAFKA TOPIC CREATION COMMANDS

Assuming your Kafka broker is `kafka-1:9092`.

---

# 🟦 3.1 Create Raw Events Topic

```bash
kafka-topics.sh --create \
  --topic pothole.raw.events.v1 \
  --partitions 12 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:9092
```

---

# 🟩 3.2 Create ML Processed Topic

```bash
kafka-topics.sh --create \
  --topic pothole.ml.processed.v1 \
  --partitions 24 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:9092
```

---

# 🟨 3.3 Create Geo Enriched Topic

```bash
kafka-topics.sh --create \
  --topic pothole.geo.enriched.v1 \
  --partitions 24 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:9092
```

---

# 🟧 3.4 Create Cluster Updates Topic

```bash
kafka-topics.sh --create \
  --topic pothole.cluster.updates.v1 \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:9092
```

---

# 🟥 3.5 Create Alerts Topic

```bash
kafka-topics.sh --create \
  --topic pothole.alerts.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:9092
```

---

# ⭐ 4. BEST PRACTICE: DLQ Topics

```bash
kafka-topics.sh --create \
  --topic pothole.raw.events.dlq.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka-1:9092
```

(similar for ML and GEO)

---

# ⭐ 5. PYTHON PRODUCER EXAMPLE (RAW EVENTS)

Using Avro + Confluent Kafka Python client + Schema Registry:

```python
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from uuid import uuid4
import time

producer_conf = {
    "bootstrap.servers": "localhost:19092",
    "key.serializer": str.encode,
}

schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

raw_event_schema = """ <INSERT AVRO SCHEMA JSON HERE> """

serializer = AvroSerializer(schema_registry_client, raw_event_schema)

producer = SerializingProducer(producer_conf)

event = {
    "event_id": str(uuid4()),
    "vehicle_id": "car-01",
    "timestamp": str(time.time()),
    "gps_lat": 10.123,
    "gps_lon": 106.234,
    "image_path": "s3://images/raw/xyz.jpg",
}

producer.produce(
    topic="pothole.raw.events.v1",
    key=event["vehicle_id"],
    value=event,
    value_serializer=serializer
)

producer.flush()
```

---

# ⭐ 6. PYTHON CONSUMER EXAMPLE

```python
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer

consumer_conf = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "ml-processor",
    "auto.offset.reset": "earliest",
}

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})
deserializer = AvroDeserializer(schema_registry_client)

consumer = DeserializingConsumer({
    **consumer_conf,
    "value.deserializer": deserializer
})

consumer.subscribe(["pothole.raw.events.v1"])

while True:
    msg = consumer.poll(1)
    if msg is None:
        continue
    print("Raw event:", msg.value())
```

---

# 🎉 Summary

You now have:

✔ Production-ready Kafka topic structure
✔ Avro schemas for each event pipeline
✔ Proper partitioning strategy
✔ Topic creation commands
✔ Python producer + consumer templates

This fully supports:

* High-throughput ingestion
* ML event processing
* Geo enrichment
* Pothole clustering
* Iceberg table ingestion