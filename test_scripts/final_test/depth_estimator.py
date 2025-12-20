"""
Depth Estimation Simulator

Consumes raw events from 'pothole.raw.events.v1', simulates depth estimation
from the image, and produces results to 'pothole.depth.v1'.
"""

import time
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
OUTPUT_TOPIC = "pothole.depth.v1"
CONSUMER_GROUP_ID = "depth-estimator-group"
BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"

# MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "warehouse"

# Simulated depth range (cm)
DEPTH_MIN_CM = 1.0
DEPTH_MAX_CM = 15.0

# ============================================================================
# AVRO SCHEMAS
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
    {"name": "image_path", "type": "string"},
    {"name": "pothole_polygon", "type": "string"},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null}
  ]
}
"""

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
# DEPTH ESTIMATION SIMULATION
# ============================================================================
def simulate_depth_estimation(image_bytes):
    """
    Simulate depth estimation from image.
    In production, this would call an ML model.
    Returns (depth_cm, confidence)
    """
    # Simulate processing time (50-200ms)
    time.sleep(random.uniform(0.05, 0.2))
    
    # Generate realistic depth values
    # Most potholes are 2-8cm deep, with some outliers
    if random.random() < 0.7:
        # Common case: shallow to medium
        depth_cm = random.gauss(5.0, 2.0)
    else:
        # Less common: deeper potholes
        depth_cm = random.gauss(10.0, 3.0)
    
    # Clamp to valid range
    depth_cm = max(DEPTH_MIN_CM, min(DEPTH_MAX_CM, depth_cm))
    
    # Confidence based on image size (simulated)
    confidence = random.uniform(0.75, 0.98)
    
    return round(depth_cm, 2), round(confidence, 4)


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
    """Create Avro serializer for depth estimates."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    return AvroSerializer(
        schema_registry_client,
        DEPTH_ESTIMATE_SCHEMA_STR,
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
    print("DEPTH ESTIMATION SIMULATOR")
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
                message_count += 1
                
                print(f"\n[RECEIVED #{message_count}] event_id={event_id}")
                
                # Download image from MinIO
                image_bytes = download_image_from_minio(minio_client, image_path)
                
                if image_bytes is None:
                    print(f"[WARN] Could not download image, using placeholder estimation")
                    image_bytes = b""  # Use empty bytes for simulation
                
                # Simulate depth estimation
                depth_cm, confidence = simulate_depth_estimation(image_bytes)
                processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                
                print(f"[ESTIMATED] depth={depth_cm}cm, confidence={confidence}")
                
                # Create output record
                depth_estimate = {
                    "event_id": event_id,
                    "depth_cm": depth_cm,
                    "confidence": confidence,
                    "processed_at": processed_at,
                }
                
                # Serialize and produce
                serialized_value = serializer(
                    depth_estimate,
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
