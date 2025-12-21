"""
Severity Score Aggregator Microservice

Consumes from 'pothole.depth.v1' and 'pothole.raw.events.v1' (for surface_area_cm2),
aggregates by event_id, calculates severity score, and produces
to 'pothole.severity.score.v1'.

NOTE: As of schema v2, surface_area_cm2 is computed at the edge device
and included in the raw events. The pothole.surface.area.v1 topic is deprecated.
"""

import time
import threading
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, Optional, Any

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

# ============================================================================
# CONFIGURATION
# ============================================================================
# Kafka
DEPTH_TOPIC = "pothole.depth.v1"
RAW_EVENTS_TOPIC = "pothole.raw.events.v1"  # For surface_area_cm2
OUTPUT_TOPIC = "pothole.severity.score.v1"
CONSUMER_GROUP_ID = "severity-aggregator-group"
BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"

# Aggregation settings
AGGREGATION_TIMEOUT_SECONDS = 600  # Max time to wait for both estimates
CLEANUP_INTERVAL_SECONDS = 3000  # How often to check for stale entries

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
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

# Raw events schema (for extracting surface_area_cm2)
RAW_EVENT_SCHEMA_STR = """
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
    {"name": "severity_score", "type": "double"},
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["MINOR", "MODERATE", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


# ============================================================================
# SEVERITY CALCULATION
# ============================================================================
def map_area_to_discrete(surface_area_cm2: float) -> int:
    """
    Map surface area (cm²) to discrete value 1-10.

    Thresholds:
    - 0 <= area < 300 → 1
    - 300 <= area < 700 → 2
    - 700 <= area < 1200 → 3
    - 1200 <= area < 2500 → 4
    - 2500 <= area < 5000 → 5
    - 5000 <= area < 10000 → 6
    - 10000 <= area < 15000 → 7
    - 15000 <= area < 25000 → 8
    - 25000 <= area < 45000 → 9
    - 45000 <= area → 10
    """
    if surface_area_cm2 < 300:
        return 1
    elif surface_area_cm2 < 700:
        return 2
    elif surface_area_cm2 < 1200:
        return 3
    elif surface_area_cm2 < 2500:
        return 4
    elif surface_area_cm2 < 5000:
        return 5
    elif surface_area_cm2 < 10000:
        return 6
    elif surface_area_cm2 < 15000:
        return 7
    elif surface_area_cm2 < 25000:
        return 8
    elif surface_area_cm2 < 45000:
        return 9
    else:
        return 10


def map_depth_to_discrete(depth_cm: float) -> int:
    """
    Map depth (cm) to discrete value 1-10.

    Thresholds:
    - 0 <= depth < 1 → 1
    - 1 <= depth < 2.5 → 2
    - 2.5 <= depth < 5 → 3
    - 5 <= depth < 7.5 → 4
    - 7.5 <= depth < 10 → 5
    - 10 <= depth < 12.5 → 6
    - 12.5 <= depth < 15 → 7
    - 15 <= depth < 20 → 8
    - 20 <= depth < 25 → 9
    - 25 <= depth → 10
    """
    if depth_cm < 1:
        return 1
    elif depth_cm < 2.5:
        return 2
    elif depth_cm < 5:
        return 3
    elif depth_cm < 7.5:
        return 4
    elif depth_cm < 10:
        return 5
    elif depth_cm < 12.5:
        return 6
    elif depth_cm < 15:
        return 7
    elif depth_cm < 20:
        return 8
    elif depth_cm < 25:
        return 9
    else:
        return 10


def calculate_severity_score(depth_cm: float, surface_area_cm2: float) -> float:
    """
    Calculate severity score based on depth and surface area.

    Formula: severity = 0.7 * area_discrete + 0.3 * depth_discrete

    Both area and depth are first mapped to discrete values 1-10,
    then combined with weights (area: 0.7, depth: 0.3).

    Returns a score from 1.0 to 10.0
    """
    area_discrete = map_area_to_discrete(surface_area_cm2)
    depth_discrete = map_depth_to_discrete(depth_cm)

    # Weighted combination: area weight: 0.7, depth weight: 0.3
    severity_score = (0.7 * area_discrete) + (0.3 * depth_discrete)

    return round(severity_score, 2)


def get_severity_level(severity_score: float) -> str:
    """
    Convert severity score (1-10) to categorical level.

    Thresholds:
    - MINOR: 1.0 - 3.25
    - MODERATE: 3.25 - 5.5
    - HIGH: 5.5 - 7.75
    - CRITICAL: 7.75 - 10.0
    """
    if severity_score < 3.25:
        return "MINOR"
    elif severity_score < 5.5:
        return "MODERATE"
    elif severity_score < 7.75:
        return "HIGH"
    else:
        return "CRITICAL"


# ============================================================================
# AGGREGATION STORE
# ============================================================================
class AggregationStore:
    """
    Thread-safe store for aggregating depth and surface area estimates.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._store: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "depth": None,
                "surface_area": None,
                "created_at": time.time(),
            }
        )

    def add_depth(self, event_id: str, depth_cm: float, confidence: Optional[float]):
        """Add depth estimate for an event."""
        with self._lock:
            self._store[event_id]["depth"] = {
                "depth_cm": depth_cm,
                "confidence": confidence,
            }
            return self._check_complete(event_id)

    def add_surface_area(
        self, event_id: str, surface_area_cm2: float, confidence: Optional[float]
    ):
        """Add surface area estimate for an event."""
        with self._lock:
            self._store[event_id]["surface_area"] = {
                "surface_area_cm2": surface_area_cm2,
                "confidence": confidence,
            }
            return self._check_complete(event_id)

    def _check_complete(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Check if both estimates are available.
        Returns combined data if complete, None otherwise.
        """
        entry = self._store[event_id]

        if entry["depth"] is not None and entry["surface_area"] is not None:
            # Both available - return combined data and remove from store
            result = {
                "event_id": event_id,
                "depth_cm": entry["depth"]["depth_cm"],
                "surface_area_cm2": entry["surface_area"]["surface_area_cm2"],
            }
            del self._store[event_id]
            return result

        return None

    def cleanup_stale(self, max_age_seconds: float) -> int:
        """Remove entries older than max_age_seconds. Returns count removed."""
        current_time = time.time()
        removed = 0

        with self._lock:
            stale_keys = [
                k
                for k, v in self._store.items()
                if current_time - v["created_at"] > max_age_seconds
            ]

            for key in stale_keys:
                del self._store[key]
                removed += 1

        return removed

    def size(self) -> int:
        """Return number of pending aggregations."""
        with self._lock:
            return len(self._store)


# ============================================================================
# KAFKA SETUP
# ============================================================================
def create_consumers():
    """Create Kafka consumers for both input topics."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    # Single consumer subscribing to multiple topics
    consumer = Consumer(consumer_conf)
    consumer.subscribe([DEPTH_TOPIC, RAW_EVENTS_TOPIC])
    print(f"[SUCCESS] Subscribed to {DEPTH_TOPIC} and {RAW_EVENTS_TOPIC}")

    return consumer


def create_producer():
    """Create Kafka producer."""
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    return Producer(producer_conf)


def create_deserializers():
    """Create Avro deserializers for both input schemas."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    depth_deserializer = AvroDeserializer(
        schema_registry_client,
        DEPTH_ESTIMATE_SCHEMA_STR,
        lambda obj, ctx: obj,
    )

    raw_event_deserializer = AvroDeserializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        lambda obj, ctx: obj,
    )

    return depth_deserializer, raw_event_deserializer


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


# ============================================================================
# MAIN LOOP
# ============================================================================
def main():
    print("=" * 70)
    print("SEVERITY SCORE AGGREGATOR")
    print("=" * 70)

    # Setup Kafka
    consumer = create_consumers()
    producer = create_producer()
    depth_deserializer, raw_event_deserializer = create_deserializers()
    severity_serializer = create_serializer()

    # Initialize aggregation store
    store = AggregationStore()
    last_cleanup = time.time()

    print(f"\n[INFO] Consuming from: {DEPTH_TOPIC}, {RAW_EVENTS_TOPIC}")
    print(f"[INFO] Producing to: {OUTPUT_TOPIC}")
    print(f"[INFO] Aggregation timeout: {AGGREGATION_TIMEOUT_SECONDS}s")
    print(f"[INFO] Press Ctrl+C to stop.\n")

    depth_count = 0
    surface_count = 0
    severity_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            # Periodic cleanup of stale entries
            if time.time() - last_cleanup > CLEANUP_INTERVAL_SECONDS:
                removed = store.cleanup_stale(AGGREGATION_TIMEOUT_SECONDS)
                if removed > 0:
                    print(f"[CLEANUP] Removed {removed} stale entries")
                last_cleanup = time.time()

            if msg is None:
                continue

            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            combined_data = None

            try:
                if topic == DEPTH_TOPIC:
                    # Deserialize depth estimate
                    record = depth_deserializer(
                        msg.value(),
                        SerializationContext(DEPTH_TOPIC, MessageField.VALUE),
                    )

                    if record:
                        event_id = record["event_id"]
                        depth_cm = record["depth_cm"]
                        confidence = record.get("confidence")
                        depth_count += 1

                        print(
                            f"[DEPTH #{depth_count}] event_id={event_id}, depth={depth_cm}cm"
                        )

                        # Add to store and check if complete
                        combined_data = store.add_depth(event_id, depth_cm, confidence)

                elif topic == RAW_EVENTS_TOPIC:
                    # Deserialize raw event to extract surface_area_cm2
                    record = raw_event_deserializer(
                        msg.value(),
                        SerializationContext(RAW_EVENTS_TOPIC, MessageField.VALUE),
                    )

                    if record:
                        event_id = record["event_id"]
                        surface_area_cm2 = record["surface_area_cm2"]
                        surface_count += 1

                        print(
                            f"[SURFACE #{surface_count}] event_id={event_id}, area={surface_area_cm2}cm² (from raw event)"
                        )

                        # Add to store and check if complete
                        combined_data = store.add_surface_area(
                            event_id, surface_area_cm2, None
                        )

                # If we have both estimates, calculate severity
                if combined_data:
                    event_id = combined_data["event_id"]
                    depth_cm = combined_data["depth_cm"]
                    surface_area_cm2 = combined_data["surface_area_cm2"]

                    # Calculate severity
                    severity_score = calculate_severity_score(
                        depth_cm, surface_area_cm2
                    )
                    severity_level = get_severity_level(severity_score)
                    calculated_at = int(datetime.now(timezone.utc).timestamp() * 1000)

                    severity_count += 1
                    print(f"\n[SEVERITY #{severity_count}] event_id={event_id}")
                    print(f"  → depth={depth_cm}cm, area={surface_area_cm2}cm²")
                    print(f"  → score={severity_score}, level={severity_level}\n")

                    # Create output record
                    severity_record = {
                        "event_id": event_id,
                        "depth_cm": depth_cm,
                        "surface_area_cm2": surface_area_cm2,
                        "severity_score": severity_score,
                        "severity_level": severity_level,
                        "calculated_at": calculated_at,
                    }

                    # Serialize and produce
                    serialized_value = severity_serializer(
                        severity_record,
                        SerializationContext(OUTPUT_TOPIC, MessageField.VALUE),
                    )

                    producer.produce(
                        topic=OUTPUT_TOPIC,
                        key=event_id,
                        value=serialized_value,
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                continue

    except KeyboardInterrupt:
        print("\n\n[INFO] Stopped by user.")

    finally:
        print(
            f"\n[STATS] Processed: {depth_count} depth, {surface_count} surface, {severity_count} severity"
        )
        print(f"[STATS] Pending aggregations: {store.size()}")
        print("[INFO] Flushing producer...")
        producer.flush()
        print("[INFO] Closing consumer...")
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
