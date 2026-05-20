"""
Severity Score Aggregator Microservice.

Consumes `pothole.depth.v1`, calculates severity from depth and the
surface_area_cm2 passthrough field, and produces `pothole.severity.score.v1`.
"""

from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from config_loader import ConfigLoader

try:
    from cloud.shared.dlq import send_to_dlq
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from shared.dlq import send_to_dlq

# ============================================================================
# CONFIGURATION - Load from config.yaml
# ============================================================================
config = ConfigLoader()

DEPTH_TOPIC = config.depth_topic
OUTPUT_TOPIC = config.output_topic
CONSUMER_GROUP_ID = config.kafka_group_id
BOOTSTRAP_SERVERS = config.kafka_bootstrap_servers
SCHEMA_REGISTRY_URL = config.schema_registry_url

AREA_WEIGHT = config.area_weight
DEPTH_WEIGHT = config.depth_weight
MIN_SCORE = config.min_score
MAX_SCORE = config.max_score
THRESHOLDS = config.thresholds

LOG_TIMEOUT_WARNINGS = config.log_timeout_warnings
DLQ_TOPIC = "pothole.severity.score.dlq.v1"
SERVICE_NAME = "severity"

# ============================================================================
# AVRO SCHEMAS
# ============================================================================
DEPTH_ESTIMATE_SCHEMA_STR = """
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
"""

SEVERITY_SCORE_SCHEMA_STR = """
{
  "type": "record",
  "name": "SeverityScore",
  "namespace": "pothole.severity.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "severity_score", "type": "int"},
    {"name": "severity_level", "type": "string"},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


# ============================================================================
# SEVERITY CALCULATION
# ============================================================================
def map_area_to_discrete(surface_area_cm2: float) -> int:
    """Map surface area in cm2 to a discrete value from 1 to 10."""
    if surface_area_cm2 < 300:
        return 1
    if surface_area_cm2 < 700:
        return 2
    if surface_area_cm2 < 1200:
        return 3
    if surface_area_cm2 < 2500:
        return 4
    if surface_area_cm2 < 5000:
        return 5
    if surface_area_cm2 < 10000:
        return 6
    if surface_area_cm2 < 15000:
        return 7
    if surface_area_cm2 < 25000:
        return 8
    if surface_area_cm2 < 45000:
        return 9
    return 10


def map_depth_to_discrete(depth_cm: float) -> int:
    """Map depth in cm to a discrete value from 1 to 10."""
    if depth_cm < 1:
        return 1
    if depth_cm < 2.5:
        return 2
    if depth_cm < 5:
        return 3
    if depth_cm < 7.5:
        return 4
    if depth_cm < 10:
        return 5
    if depth_cm < 12.5:
        return 6
    if depth_cm < 15:
        return 7
    if depth_cm < 20:
        return 8
    if depth_cm < 25:
        return 9
    return 10


def calculate_severity_score(depth_cm: float, surface_area_cm2: float) -> int:
    """
    Calculate severity score based on depth and surface area.

    Formula:
    clip[MIN_SCORE, MAX_SCORE](ceil(AREA_WEIGHT * area + DEPTH_WEIGHT * depth))
    """
    import math

    area_discrete = map_area_to_discrete(surface_area_cm2)
    depth_discrete = map_depth_to_discrete(depth_cm)
    raw_score = (AREA_WEIGHT * area_discrete) + (DEPTH_WEIGHT * depth_discrete)
    return max(MIN_SCORE, min(MAX_SCORE, math.ceil(raw_score)))


def get_severity_level(severity_score: int) -> str:
    """Convert severity score (1-10) to categorical level."""
    if severity_score < THRESHOLDS.get("minor_max", 3.25):
        return "MINOR"
    if severity_score < THRESHOLDS.get("moderate_max", 5.5):
        return "MODERATE"
    if severity_score < THRESHOLDS.get("high_max", 7.75):
        return "HIGH"
    return "CRITICAL"


def validate_depth_record(record: dict) -> None:
    required_fields = ("event_id", "depth_cm", "surface_area_cm2")
    missing = [field for field in required_fields if field not in record or record[field] is None]
    if missing:
        raise ValueError(f"Missing required depth record field(s): {', '.join(missing)}")


# ============================================================================
# KAFKA SETUP
# ============================================================================
def create_consumer():
    """Create Kafka consumer for depth topic."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([DEPTH_TOPIC])
    print(f"[SUCCESS] Subscribed to {DEPTH_TOPIC}")
    return consumer


def create_producer():
    """Create Kafka producer."""
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    return Producer(producer_conf)


def create_deserializer():
    """Create Avro deserializer for depth estimates."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    return AvroDeserializer(
        schema_registry_client,
        DEPTH_ESTIMATE_SCHEMA_STR,
        lambda obj, ctx: obj,
    )


def create_serializer():
    """Create Avro serializer for severity scores."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    return AvroSerializer(
        schema_registry_client,
        SEVERITY_SCORE_SCHEMA_STR,
        lambda obj, ctx: obj,
    )


def delivery_report(err, msg):
    """Kafka delivery callback."""
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


# ============================================================================
# MAIN LOOP
# ============================================================================
def main():
    print("=" * 70)
    print("SEVERITY SCORE AGGREGATOR")
    print("=" * 70)

    consumer = create_consumer()
    producer = create_producer()
    dlq_producer = create_producer()
    depth_deserializer = create_deserializer()
    severity_serializer = create_serializer()

    print(f"\n[INFO] Consuming from: {DEPTH_TOPIC}")
    print(f"[INFO] Producing to: {OUTPUT_TOPIC}")
    print("[INFO] Press Ctrl+C to stop.\n")

    processed_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            record = None
            try:
                record = depth_deserializer(
                    msg.value(),
                    SerializationContext(DEPTH_TOPIC, MessageField.VALUE),
                )
                if not record:
                    continue
                validate_depth_record(record)

                event_id = record["event_id"]
                depth_cm = record["depth_cm"]
                surface_area_cm2 = record["surface_area_cm2"]
                processed_count += 1

                severity_score = calculate_severity_score(depth_cm, surface_area_cm2)
                severity_level = get_severity_level(severity_score)
                calculated_at = int(datetime.now(timezone.utc).timestamp() * 1000)

                print(f"\n[SEVERITY #{processed_count}] event_id={event_id}")
                print(f"  -> depth={depth_cm}cm, area={surface_area_cm2}cm2")
                print(f"  -> score={severity_score}, level={severity_level}\n")

                severity_record = {
                    "event_id": event_id,
                    "depth_cm": depth_cm,
                    "surface_area_cm2": surface_area_cm2,
                    "severity_score": severity_score,
                    "severity_level": severity_level,
                    "calculated_at": calculated_at,
                }

                serialized_value = severity_serializer(
                    severity_record,
                    SerializationContext(OUTPUT_TOPIC, MessageField.VALUE),
                )
                produce_and_flush(producer, OUTPUT_TOPIC, event_id, serialized_value)
                consumer.commit(message=msg)

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                payload = record if record is not None else msg.value()
                key = payload.get("event_id") if isinstance(payload, dict) else None
                send_to_dlq(
                    dlq_producer,
                    DLQ_TOPIC,
                    DEPTH_TOPIC,
                    SERVICE_NAME,
                    e,
                    payload,
                    key=key,
                )
                consumer.commit(message=msg)
                continue

    except KeyboardInterrupt:
        print("\n\n[INFO] Stopped by user.")

    finally:
        print(f"\n[STATS] Processed: {processed_count} severity scores")
        producer.flush()
        dlq_producer.flush()
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
