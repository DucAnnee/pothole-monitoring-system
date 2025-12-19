from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

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


def from_dict(raw: dict, ctx):
    """
    AvroDeserializer will hand us a dict; we can just return it as-is.
    If you had a dataclass or custom type, you’d convert to that here.
    """
    return raw


def main():
    # 1. Kafka consumer config
    consumer_conf = {
        "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
        "group.id": "pothole-raw-consumer",
        "auto.offset.reset": "earliest",  # start from earliest for testing
        # you can also tune enable.auto.commit here if you care about commits
    }

    consumer = Consumer(consumer_conf)

    # 2. Schema Registry client
    schema_registry_conf = {"url": "http://localhost:8082"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 3. Avro deserializer – mirror the serializer on the producer side
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        from_dict,
    )

    consumer.subscribe([TOPIC])
    print(f"🔍 Subscribed to {TOPIC}, waiting for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                # Non-fatal vs fatal error handling
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    continue
                print(f"❌ Consumer error: {msg.error()}")
                continue

            # msg.key() and msg.value() are bytes at this point
            key = msg.key().decode("utf-8") if msg.key() is not None else None

            try:
                raw_event = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as e:
                print(f"❌ Failed to deserialize message: {e}")
                continue

            print("📥 Received message:")
            print(f"  partition = {msg.partition()}, offset = {msg.offset()}")
            print(f"  key       = {key}")
            print(f"  value     = {raw_event}")

            # If you care about manual commits:
            # consumer.commit(msg)

    except KeyboardInterrupt:
        print("\n👋 Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
