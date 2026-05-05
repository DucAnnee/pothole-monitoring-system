"""
BEV Surface Area Service.

Consumes pothole.raw.events.v2, runs BEV transformation + pothole area estimation,
uploads BEV image to MinIO, publishes pothole.surface.area.v2.
"""

import os
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from minio import Minio

from bev_processor import BEVProcessor
from config import ConfigLoader
from pothole_area_estimator import PotholeAreaEstimator

try:
    from cloud.shared.dlq import send_to_dlq
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from shared.dlq import send_to_dlq

DLQ_TOPIC = "pothole.surface.area.dlq.v1"
SERVICE_NAME = "bev-surface"


RAW_EVENT_SCHEMA_STR = """
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
"""

SURFACE_AREA_SCHEMA_STR = """
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
"""


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[DELIVERED] {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_and_flush(producer, topic, key, value, timeout=30):
    """Produce one message and return only after Kafka acknowledges delivery."""
    delivery_error = {"error": None}

    def callback(err, msg):
        delivery_report(err, msg)
        if err is not None:
            delivery_error["error"] = err

    producer.produce(topic=topic, key=key, value=value, on_delivery=callback)
    remaining = producer.flush(timeout)
    if remaining > 0:
        raise TimeoutError(f"Timed out delivering message to {topic}")
    if delivery_error["error"] is not None:
        raise RuntimeError(f"Failed delivering message to {topic}: {delivery_error['error']}")


def validate_raw_event(record: dict) -> None:
    required_fields = ("event_id", "raw_image_object_key", "original_mask")
    missing = [field for field in required_fields if field not in record or record[field] in (None, "")]
    if missing:
        raise ValueError(f"Missing required raw event field(s): {', '.join(missing)}")


def main():
    print("=" * 70)
    print("BEV SURFACE AREA SERVICE")
    print("=" * 70)

    config_path = os.environ.get(
        "BEV_SURFACE_CONFIG",
        os.environ.get(
            "POTHOLE_CONFIG_PATH",
            os.path.join(os.path.dirname(__file__), "config.yaml"),
        ),
    )
    config = ConfigLoader(config_path)

    trapezoid = np.array(config.trapezoid_coords, dtype=np.float32)
    rectangle = np.array(config.rectangle_coords, dtype=np.float32)
    estimator = PotholeAreaEstimator(
        trapezoid_coords=trapezoid,
        rectangle_coords=rectangle,
        calibration_path=config.calibration_file,
    )

    minio_client = Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=config.minio_secure,
    )
    processor = BEVProcessor(minio_client, config.minio_bucket, estimator)

    schema_registry = SchemaRegistryClient({"url": config.schema_registry_url})
    deserializer = AvroDeserializer(schema_registry, RAW_EVENT_SCHEMA_STR, lambda obj, ctx: obj)
    serializer = AvroSerializer(schema_registry, SURFACE_AREA_SCHEMA_STR, lambda obj, ctx: obj)

    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.kafka_consumer_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": config.kafka_bootstrap_servers})
    dlq_producer = Producer({"bootstrap.servers": config.kafka_bootstrap_servers})

    source_topic = config.kafka_source_topic
    output_topic = config.kafka_output_topic
    consumer.subscribe([source_topic])

    print(f"[INFO] Consuming from: {source_topic}")
    print(f"[INFO] Producing to: {output_topic}")
    print("[INFO] Press Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            raw_event = None
            try:
                raw_event = deserializer(
                    msg.value(), SerializationContext(source_topic, MessageField.VALUE)
                )
                if raw_event is None:
                    consumer.commit(message=msg)
                    continue
                validate_raw_event(raw_event)

                event_id = raw_event["event_id"]
                raw_key = raw_event["raw_image_object_key"]
                mask = raw_event["original_mask"]
                count += 1

                print(f"\n[RECEIVED #{count}] event_id={event_id}")
                bev_key, bev_mask_json, area_cm2, confidence = processor.process(
                    event_id, raw_key, mask
                )

                if bev_key is None and confidence == 0.0 and area_cm2 == 0.0:
                    bev_key = ""
                    bev_mask_json = "[]"

                processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                output = {
                    "event_id": event_id,
                    "raw_image_object_key": raw_key,
                    "bev_object_key": bev_key or "",
                    "bev_mask": bev_mask_json or "[]",
                    "surface_area_cm2": area_cm2,
                    "confidence": confidence,
                    "processed_at": processed_at,
                }

                serialized = serializer(output, SerializationContext(output_topic, MessageField.VALUE))
                produce_and_flush(producer, output_topic, event_id, serialized)
                consumer.commit(message=msg)

                print(
                    f"[PUBLISHED] event_id={event_id}, "
                    f"area={area_cm2:.1f}cm2, confidence={confidence}"
                )

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                payload = raw_event if raw_event is not None else msg.value()
                key = payload.get("event_id") if isinstance(payload, dict) else None
                send_to_dlq(
                    dlq_producer,
                    DLQ_TOPIC,
                    source_topic,
                    SERVICE_NAME,
                    e,
                    payload,
                    key=key,
                )
                consumer.commit(message=msg)
                import traceback

                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
    finally:
        producer.flush()
        dlq_producer.flush()
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
