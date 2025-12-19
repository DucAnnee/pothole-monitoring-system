from uuid import uuid4
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

RAW_EVENT_SCHEMA_STR = """
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
"""

TOPIC = "pothole.raw.events.v1"


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"✅ Record delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


def main():
    # 1. Kafka config — from your host you hit the mapped ports
    producer_conf = {
        "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    }
    producer = Producer(producer_conf)

    # 2. Schema Registry client (host sees it on 8082)
    schema_registry_conf = {"url": "http://localhost:8082"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 3. Avro serializer
    #    We just pass dicts, so to_dict is identity
    avro_serializer = AvroSerializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        lambda obj, ctx: obj,  # dict passthrough
    )

    # =====================================================================
    # Pretend this comes back from your MinIO upload step
    # =====================================================================
    dummy_id = str(uuid4())
    image_path_from_minio = "s3://warehouse/raw_images/" + dummy_id + ".jpg"

    raw_event = {
        "event_id": dummy_id,
        "vehicle_id": "vehicle-123",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "gps_lat": 10.762622,
        "gps_lon": 106.660172,
        "gps_accuracy": None,          # or a float
        "image_path": image_path_from_minio,
        "detection_confidence": 0.93,  # or None
    }

    # 4. Serialize with Avro + Schema Registry framing
    serialized_value = avro_serializer(
        raw_event,
        SerializationContext(TOPIC, MessageField.VALUE),
    )

    # 5. Produce to Kafka (key = vehicle_id for partitioning)
    producer.produce(
        topic=TOPIC,
        key=raw_event["vehicle_id"],
        value=serialized_value,
        on_delivery=delivery_report,
    )

    # Give the producer a chance to send
    producer.poll(0)
    producer.flush()


if __name__ == "__main__":
    main()
