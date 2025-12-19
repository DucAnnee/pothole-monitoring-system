import json
import base64
import time
from uuid import uuid4
from datetime import datetime, timezone
from io import BytesIO
import random

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from minio import Minio
from minio.error import S3Error

# ============================================================================
# CONFIGURATION
# ============================================================================
TOPIC = "pothole.raw.events.v1"

BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"

SCHEMA_REGISTRY_URL = "http://localhost:8082"

# MinIO config
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "warehouse"
MINIO_PREFIX = "raw_images"

# Image source
IMAGE_PATH = "./HCM-HN-01_frame_000001.jpg"

# HCM City approximate bounds for GPS simulation
HCM_LAT_MIN, HCM_LAT_MAX = 10.7, 10.9
HCM_LON_MIN, HCM_LON_MAX = 106.6, 106.8

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


# ============================================================================
# MinIO Functions
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
            
            # Ensure bucket exists
            if not client.bucket_exists(BUCKET):
                client.make_bucket(BUCKET)
                print(f"[SUCCESS] Created MinIO bucket: {BUCKET}")
            else:
                print(f"[SUCCESS] Connected to MinIO bucket: {BUCKET}")
            
            return client
            
        except S3Error as e:
            print(f"[ERROR] MinIO S3 error: {e}")
        except Exception as e:
            print(f"[ERROR] MinIO connection failed: {e}")
        
        if attempt < max_retries:
            print(f"[INFO] Retrying in {delay} seconds...")
            time.sleep(delay)
    
    print("[FATAL] Could not connect to MinIO after multiple attempts.")
    return None


def upload_image_to_minio(client, image_bytes, event_id):
    """Upload image to MinIO and return S3 path."""
    object_name = f"{MINIO_PREFIX}/{event_id}.jpg"
    
    try:
        client.put_object(
            bucket_name=BUCKET,
            object_name=object_name,
            data=BytesIO(image_bytes),
            length=len(image_bytes),
            content_type="image/jpeg",
        )
        
        s3_path = f"s3://{BUCKET}/{object_name}"
        print(f"[SUCCESS] Uploaded image to {s3_path}")
        return s3_path
        
    except Exception as e:
        print(f"[ERROR] Failed uploading to MinIO: {e}")
        return None


# ============================================================================
# Kafka Functions
# ============================================================================
def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"✅ Record delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


def create_kafka_producer(max_retries=5, delay=2):
    """Create Kafka producer with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Kafka (attempt {attempt})...")
            producer_conf = {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
            }
            producer = Producer(producer_conf)
            print("[SUCCESS] Connected to Kafka brokers.")
            return producer
            
        except Exception as e:
            print(f"[ERROR] Kafka connection failed: {e}")
            if attempt < max_retries:
                print(f"[INFO] Retrying in {delay} seconds...")
                time.sleep(delay)
    
    print("[FATAL] Could not connect to Kafka after multiple attempts.")
    return None


def create_schema_registry_client(max_retries=5, delay=2):
    """Create Schema Registry client with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Schema Registry (attempt {attempt})...")
            schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
            client = SchemaRegistryClient(schema_registry_conf)
            print("[SUCCESS] Connected to Schema Registry.")
            return client
            
        except Exception as e:
            print(f"[ERROR] Schema Registry connection failed: {e}")
            if attempt < max_retries:
                print(f"[INFO] Retrying in {delay} seconds...")
                time.sleep(delay)
    
    print("[FATAL] Could not connect to Schema Registry after multiple attempts.")
    return None


# ============================================================================
# Simulation Functions
# ============================================================================
def generate_random_hcm_gps():
    """Generate random GPS coordinates within HCM City bounds."""
    lat = random.uniform(HCM_LAT_MIN, HCM_LAT_MAX)
    lon = random.uniform(HCM_LON_MIN, HCM_LON_MAX)
    return lat, lon


def load_image():
    """Load the test image from disk."""
    try:
        with open(IMAGE_PATH, "rb") as f:
            return f.read()
    except Exception as e:
        print(f"[FATAL] Failed to load image from {IMAGE_PATH}: {e}")
        return None


# ============================================================================
# Main Loop
# ============================================================================
def main():
    print("=" * 70)
    print("POTHOLE DETECTION EDGE DEVICE SIMULATOR")
    print("=" * 70)
    
    # Load image once (reuse for all simulated captures)
    image_bytes = load_image()
    if image_bytes is None:
        return
    
    # Connect to MinIO
    minio_client = connect_minio()
    if minio_client is None:
        return
    
    # Connect to Kafka
    producer = create_kafka_producer()
    if producer is None:
        return
    
    # Connect to Schema Registry
    schema_registry_client = create_schema_registry_client()
    if schema_registry_client is None:
        return
    
    # Create Avro serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        lambda obj, ctx: obj,  # dict passthrough
    )
    
    vehicle_id = f"vehicle-{uuid4().hex[:8]}"
    print(f"\n[INFO] Simulating vehicle: {vehicle_id}")
    print(f"[INFO] Sending 1 event per minute. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            # Generate event data
            event_id = str(uuid4())
            gps_lat, gps_lon = generate_random_hcm_gps()
            timestamp = datetime.now(timezone.utc).isoformat()
            
            print(f"\n{'='*70}")
            print(f"[EVENT] ID: {event_id}")
            print(f"[GPS] Lat: {gps_lat:.6f}, Lon: {gps_lon:.6f}")
            print(f"[TIME] {timestamp}")
            
            # Upload image to MinIO
            s3_path = upload_image_to_minio(minio_client, image_bytes, event_id)
            if s3_path is None:
                print("[WARN] Skipping Kafka publish due to MinIO failure.")
                time.sleep(60)
                continue
            
            # Create Avro record
            raw_event = {
                "event_id": event_id,
                "vehicle_id": vehicle_id,
                "timestamp": timestamp,
                "gps_lat": gps_lat,
                "gps_lon": gps_lon,
                "gps_accuracy": random.uniform(5.0, 15.0),  # meters
                "image_path": s3_path,
                "detection_confidence": random.uniform(0.85, 0.99),
            }
            
            # Serialize with Avro + Schema Registry
            serialized_value = avro_serializer(
                raw_event,
                SerializationContext(TOPIC, MessageField.VALUE),
            )
            
            # Produce to Kafka
            producer.produce(
                topic=TOPIC,
                key=vehicle_id,
                value=serialized_value,
                on_delivery=delivery_report,
            )
            
            producer.poll(0)
            producer.flush()
            
            print(f"[INFO] Waiting 60 seconds for next event...")
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n\n[INFO] Stopped by user.")
    
    finally:
        print("[INFO] Flushing Kafka producer...")
        producer.flush()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()