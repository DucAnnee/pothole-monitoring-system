"""
Severity Score Aggregator Microservice

Consumes from 'pothole.depth.v1' and 'pothole.surface.area.v1',
aggregates by event_id, calculates severity score, and produces
to 'pothole.severity.score.v1'.
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
SURFACE_AREA_TOPIC = "pothole.surface.area.v1"
OUTPUT_TOPIC = "pothole.severity.score.v1"
CONSUMER_GROUP_ID = "severity-aggregator-group"
BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"

# Aggregation settings
AGGREGATION_TIMEOUT_SECONDS = 60  # Max time to wait for both estimates
CLEANUP_INTERVAL_SECONDS = 30  # How often to check for stale entries

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

SURFACE_AREA_SCHEMA_STR = """
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
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


# ============================================================================
# SEVERITY CALCULATION
# ============================================================================
def calculate_severity_score(depth_cm: float, surface_area_cm2: float) -> float:
    """
    Calculate severity score based on depth and surface area.
    
    Formula considers:
    - Depth: Deeper potholes are more dangerous (weighted heavily)
    - Surface area: Larger potholes affect more vehicles
    
    Returns a score from 0.0 to 1.0
    """
    # Normalize depth (assume max dangerous depth is ~15cm)
    depth_normalized = min(depth_cm / 15.0, 1.0)
    
    # Normalize surface area (assume max dangerous area is ~5000 cm²)
    area_normalized = min(surface_area_cm2 / 5000.0, 1.0)
    
    # Weighted combination: depth is more critical than area
    # depth weight: 0.7, area weight: 0.3
    severity_score = (0.7 * depth_normalized) + (0.3 * area_normalized)
    
    return round(min(max(severity_score, 0.0), 1.0), 4)


def get_severity_level(severity_score: float) -> str:
    """
    Convert severity score to categorical level.
    
    Thresholds:
    - LOW: 0.0 - 0.25
    - MEDIUM: 0.25 - 0.50
    - HIGH: 0.50 - 0.75
    - CRITICAL: 0.75 - 1.0
    """
    if severity_score < 0.25:
        return "LOW"
    elif severity_score < 0.50:
        return "MEDIUM"
    elif severity_score < 0.75:
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
        self._store: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "depth": None,
            "surface_area": None,
            "created_at": time.time(),
        })
    
    def add_depth(self, event_id: str, depth_cm: float, confidence: Optional[float]):
        """Add depth estimate for an event."""
        with self._lock:
            self._store[event_id]["depth"] = {
                "depth_cm": depth_cm,
                "confidence": confidence,
            }
            return self._check_complete(event_id)
    
    def add_surface_area(self, event_id: str, surface_area_cm2: float, confidence: Optional[float]):
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
                k for k, v in self._store.items()
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
    consumer.subscribe([DEPTH_TOPIC, SURFACE_AREA_TOPIC])
    print(f"[SUCCESS] Subscribed to {DEPTH_TOPIC} and {SURFACE_AREA_TOPIC}")
    
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
    
    surface_area_deserializer = AvroDeserializer(
        schema_registry_client,
        SURFACE_AREA_SCHEMA_STR,
        lambda obj, ctx: obj,
    )
    
    return depth_deserializer, surface_area_deserializer


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
    depth_deserializer, surface_area_deserializer = create_deserializers()
    severity_serializer = create_serializer()
    
    # Initialize aggregation store
    store = AggregationStore()
    last_cleanup = time.time()
    
    print(f"\n[INFO] Consuming from: {DEPTH_TOPIC}, {SURFACE_AREA_TOPIC}")
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
                        SerializationContext(DEPTH_TOPIC, MessageField.VALUE)
                    )
                    
                    if record:
                        event_id = record["event_id"]
                        depth_cm = record["depth_cm"]
                        confidence = record.get("confidence")
                        depth_count += 1
                        
                        print(f"[DEPTH #{depth_count}] event_id={event_id}, depth={depth_cm}cm")
                        
                        # Add to store and check if complete
                        combined_data = store.add_depth(event_id, depth_cm, confidence)
                
                elif topic == SURFACE_AREA_TOPIC:
                    # Deserialize surface area estimate
                    record = surface_area_deserializer(
                        msg.value(),
                        SerializationContext(SURFACE_AREA_TOPIC, MessageField.VALUE)
                    )
                    
                    if record:
                        event_id = record["event_id"]
                        surface_area_cm2 = record["surface_area_cm2"]
                        confidence = record.get("confidence")
                        surface_count += 1
                        
                        print(f"[SURFACE #{surface_count}] event_id={event_id}, area={surface_area_cm2}cm²")
                        
                        # Add to store and check if complete
                        combined_data = store.add_surface_area(event_id, surface_area_cm2, confidence)
                
                # If we have both estimates, calculate severity
                if combined_data:
                    event_id = combined_data["event_id"]
                    depth_cm = combined_data["depth_cm"]
                    surface_area_cm2 = combined_data["surface_area_cm2"]
                    
                    # Calculate severity
                    severity_score = calculate_severity_score(depth_cm, surface_area_cm2)
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
                        SerializationContext(OUTPUT_TOPIC, MessageField.VALUE)
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
        print(f"\n[STATS] Processed: {depth_count} depth, {surface_count} surface, {severity_count} severity")
        print(f"[STATS] Pending aggregations: {store.size()}")
        print("[INFO] Flushing producer...")
        producer.flush()
        print("[INFO] Closing consumer...")
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
