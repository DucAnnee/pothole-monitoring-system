"""
Surface Area Estimation Simulator

⚠️ DEPRECATED: This service is deprecated as of the schema v2 update.
Surface area is now computed at the edge device and included directly
in the pothole.raw.events.v1 topic as 'surface_area_cm2'.

This file is kept for reference and backward compatibility with older
data that may still flow through pothole.surface.area.v1 topic.

---
Original description:
Consumes raw events from 'pothole.raw.events.v1', simulates surface area
calculation from the image/polygon, and produces results to 'pothole.surface.area.v1'.
"""

import warnings
warnings.warn(
    "surface_area_estimator.py is deprecated. "
    "Surface area is now computed at edge and included in raw events.",
    DeprecationWarning
)

import time
import json
import random
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from minio import Minio
from minio.error import S3Error

# ============================================================================
# CONFIGURATION
# ============================================================================
# Kafka
SOURCE_TOPIC = "pothole.raw.events.v1"
OUTPUT_TOPIC = "pothole.surface.area.v1"
CONSUMER_GROUP_ID = "surface-area-estimator-group"
BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"

# MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "warehouse"

# Simulated surface area range (cm²)
AREA_MIN_CM2 = 50.0
AREA_MAX_CM2 = 5000.0

# ============================================================================
# AVRO SCHEMAS
# Note: Updated to match new schema but this service is deprecated
# ============================================================================
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


# ============================================================================
# MINIO CLIENT
# ============================================================================
def connect_minio(max_retries=5, delay=2):
    """Initialize MinIO client with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to MinIO (attempt {attempt})...")
            client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            
            # Verify bucket exists
            if client.bucket_exists(BUCKET):
                print(f"[SUCCESS] Connected to MinIO bucket: {BUCKET}")
                return client
            else:
                print(f"[WARN] Bucket {BUCKET} does not exist")
                
        except S3Error as e:
            print(f"[ERROR] MinIO S3 error: {e}")
        except Exception as e:
            print(f"[ERROR] MinIO connection failed: {e}")
        
        if attempt < max_retries:
            print(f"[INFO] Retrying in {delay} seconds...")
            time.sleep(delay)
    
    print("[FATAL] Could not connect to MinIO after multiple attempts.")
    return None


def download_image_from_minio(client, s3_path):
    """Download image from MinIO. Returns image bytes or None."""
    try:
        # Parse s3://bucket/path format
        if s3_path.startswith("s3://"):
            path = s3_path[5:]  # Remove "s3://"
            parts = path.split("/", 1)
            bucket = parts[0]
            object_name = parts[1] if len(parts) > 1 else ""
        else:
            return None
        
        response = client.get_object(bucket, object_name)
        image_bytes = response.read()
        response.close()
        response.release_conn()
        
        return image_bytes
        
    except Exception as e:
        print(f"[ERROR] Failed to download image: {e}")
        return None


# ============================================================================
# SURFACE AREA ESTIMATION SIMULATION
# ============================================================================
def calculate_polygon_area_geo(polygon_json):
    """
    Calculate approximate area from GeoJSON polygon.
    Uses simple shoelace formula with lat/lon to meters conversion.
    Returns area in cm² or None if calculation fails.
    """
    try:
        polygon = json.loads(polygon_json)
        
        if polygon.get("type") != "Polygon":
            return None
        
        coords = polygon.get("coordinates", [[]])[0]
        if len(coords) < 3:
            return None
        
        # Convert to approximate meters (at ~10° latitude, 1° ≈ 111km)
        # This is a rough approximation
        lat_to_m = 111320  # meters per degree latitude
        lon_to_m = 111320 * 0.985  # approximate at ~10°N
        
        # Convert coordinates to meters relative to first point
        ref_lon, ref_lat = coords[0]
        points_m = []
        for lon, lat in coords:
            x = (lon - ref_lon) * lon_to_m
            y = (lat - ref_lat) * lat_to_m
            points_m.append((x, y))
        
        # Shoelace formula
        n = len(points_m)
        area = 0.0
        for i in range(n):
            j = (i + 1) % n
            area += points_m[i][0] * points_m[j][1]
            area -= points_m[j][0] * points_m[i][1]
        area = abs(area) / 2.0
        
        # Convert m² to cm²
        area_cm2 = area * 10000
        
        return area_cm2
        
    except Exception as e:
        print(f"[WARN] Could not calculate polygon area: {e}")
        return None


def simulate_surface_area_estimation(image_bytes, polygon_json):
    """
    Simulate surface area estimation from image and polygon.
    In production, this would use image segmentation + depth mapping.
    Returns (surface_area_cm2, confidence)
    """
    # Simulate processing time (100-300ms)
    time.sleep(random.uniform(0.1, 0.3))
    
    # Try to calculate from polygon first
    polygon_area = calculate_polygon_area_geo(polygon_json)
    
    if polygon_area and polygon_area > 0:
        # Add some noise to the polygon-based estimate
        surface_area_cm2 = polygon_area * random.uniform(0.8, 1.2)
        confidence = random.uniform(0.80, 0.95)
    else:
        # Fallback: generate realistic surface area values
        # Most potholes are 200-2000 cm² (roughly 15cm x 15cm to 45cm x 45cm)
        if random.random() < 0.7:
            # Common case: small to medium
            surface_area_cm2 = random.gauss(800, 300)
        else:
            # Less common: larger potholes
            surface_area_cm2 = random.gauss(2000, 500)
        
        confidence = random.uniform(0.70, 0.90)
    
    # Clamp to valid range
    surface_area_cm2 = max(AREA_MIN_CM2, min(AREA_MAX_CM2, surface_area_cm2))
    
    return round(surface_area_cm2, 2), round(confidence, 4)


# ============================================================================
# KAFKA SETUP
# ============================================================================
def create_consumer():
    """Create Kafka consumer."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([SOURCE_TOPIC])
    print(f"[SUCCESS] Subscribed to {SOURCE_TOPIC}")
    return consumer


def create_producer():
    """Create Kafka producer."""
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    return Producer(producer_conf)


def create_deserializer():
    """Create Avro deserializer for raw events."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    return AvroDeserializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        lambda obj, ctx: obj,
    )


def create_serializer():
    """Create Avro serializer for surface area estimates."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    return AvroSerializer(
        schema_registry_client,
        SURFACE_AREA_SCHEMA_STR,
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
    print("SURFACE AREA ESTIMATION SIMULATOR")
    print("=" * 70)
    
    # Connect to MinIO
    minio_client = connect_minio()
    if minio_client is None:
        return
    
    # Setup Kafka
    consumer = create_consumer()
    producer = create_producer()
    deserializer = create_deserializer()
    serializer = create_serializer()
    
    print(f"\n[INFO] Consuming from: {SOURCE_TOPIC}")
    print(f"[INFO] Producing to: {OUTPUT_TOPIC}")
    print(f"[INFO] Press Ctrl+C to stop.\n")
    
    message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue
            
            try:
                # Deserialize the raw event
                raw_event = deserializer(
                    msg.value(),
                    SerializationContext(SOURCE_TOPIC, MessageField.VALUE)
                )
                
                if raw_event is None:
                    continue
                
                event_id = raw_event["event_id"]
                image_path = raw_event["image_path"]
                pothole_polygon = raw_event.get("pothole_polygon", "{}")
                message_count += 1
                
                print(f"\n[RECEIVED #{message_count}] event_id={event_id}")
                
                # Download image from MinIO
                image_bytes = download_image_from_minio(minio_client, image_path)
                
                if image_bytes is None:
                    print(f"[WARN] Could not download image, using polygon-only estimation")
                    image_bytes = b""
                
                # Simulate surface area estimation
                surface_area_cm2, confidence = simulate_surface_area_estimation(
                    image_bytes, pothole_polygon
                )
                processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                
                print(f"[ESTIMATED] surface_area={surface_area_cm2}cm², confidence={confidence}")
                
                # Create output record
                surface_area_estimate = {
                    "event_id": event_id,
                    "surface_area_cm2": surface_area_cm2,
                    "confidence": confidence,
                    "processed_at": processed_at,
                }
                
                # Serialize and produce
                serialized_value = serializer(
                    surface_area_estimate,
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
        print("[INFO] Flushing producer...")
        producer.flush()
        print("[INFO] Closing consumer...")
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
