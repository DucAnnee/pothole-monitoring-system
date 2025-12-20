import json
import time
from uuid import uuid4
from datetime import datetime, timezone
from io import BytesIO
import random
import argparse
import os
import glob

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from minio import Minio
from minio.error import S3Error

from config_loader import load_config

# ============================================================================
# AVRO SCHEMA (Updated to v1)
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


# ============================================================================
# MinIO Functions
# ============================================================================
def connect_minio(config, max_retries=None, delay=None):
    """Initialize MinIO client with retry logic."""
    minio_config = config.config["minio"]
    max_retries = max_retries or minio_config.get("max_retries", 5)
    delay = delay or minio_config.get("retry_delay", 2)

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to MinIO (attempt {attempt})...")
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False),
            )

            # Ensure bucket exists
            bucket = minio_config["bucket"]
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"[SUCCESS] Created MinIO bucket: {bucket}")
            else:
                print(f"[SUCCESS] Connected to MinIO bucket: {bucket}")

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


def upload_image_to_minio(client, image_path, event_id, config, prefix):
    """
    Upload image to MinIO and return S3 path.

    Args:
        client: MinIO client instance
        image_path: Path to image file on disk
        event_id: Unique event identifier
        config: Configuration object
        prefix: MinIO prefix (e.g., "raw_images" or "bev_images")

    Returns:
        S3 path string or None on failure
    """
    minio_config = config.config["minio"]
    bucket = minio_config["bucket"]
    object_name = f"{prefix}/{event_id}.jpg"

    try:
        # Read image file
        with open(image_path, "rb") as f:
            image_data = f.read()

        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=BytesIO(image_data),
            length=len(image_data),
            content_type="image/jpeg",
        )

        s3_path = f"s3://{bucket}/{object_name}"
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


def create_kafka_producer(config, max_retries=None, delay=None):
    """Create Kafka producer with retry logic."""
    kafka_config = config.config["kafka"]
    max_retries = max_retries or kafka_config.get("max_retries", 5)
    delay = delay or kafka_config.get("retry_delay", 2)

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Kafka (attempt {attempt})...")
            producer_conf = {
                "bootstrap.servers": kafka_config["bootstrap_servers"],
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


def create_schema_registry_client(config, max_retries=None, delay=None):
    """Create Schema Registry client with retry logic."""
    kafka_config = config.config["kafka"]
    max_retries = max_retries or kafka_config.get("max_retries", 5)
    delay = delay or kafka_config.get("retry_delay", 2)

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Schema Registry (attempt {attempt})...")
            schema_registry_conf = {"url": kafka_config["schema_registry_url"]}
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
# Data Processing Functions
# ============================================================================
def generate_random_hcm_gps(config):
    """Generate random GPS coordinates within HCM City bounds."""
    gps_config = config.config["gps"]
    lat = random.uniform(gps_config["lat_min"], gps_config["lat_max"])
    lon = random.uniform(gps_config["lon_min"], gps_config["lon_max"])
    return lat, lon


def load_enriched_data(json_path):
    """
    Load enriched JSON data from surface area estimation output.

    Args:
        json_path: Path to enriched JSON file

    Returns:
        List of enriched mask data
    """
    try:
        with open(json_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load JSON from {json_path}: {e}")
        return None


def find_image_json_pairs(input_dir):
    """
    Find all image and enriched JSON pairs in the input directory.

    Args:
        input_dir: Directory containing images and _enriched.json files

    Returns:
        List of tuples (image_path, json_path, bev_image_path)
    """
    pairs = []

    # Find all enriched JSON files
    json_files = glob.glob(os.path.join(input_dir, "*_enriched.json"))

    for json_path in json_files:
        base_name = os.path.basename(json_path).replace("_enriched.json", "")

        # Look for corresponding image
        image_path = None
        for ext in [".jpg", ".jpeg", ".png"]:
            candidate = os.path.join(input_dir, base_name + ext)
            if os.path.exists(candidate):
                image_path = candidate
                break

        # Look for BEV image
        bev_image_path = os.path.join(input_dir, base_name + "_bev.jpg")
        if not os.path.exists(bev_image_path):
            bev_image_path = None

        if image_path:
            pairs.append((image_path, json_path, bev_image_path))
        else:
            print(f"[WARN] No image found for {json_path}")

    return pairs


# ============================================================================
# Main Loop
# ============================================================================
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Pothole Detection Edge Device Uploader"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing processed images and enriched JSON files",
    )
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    print("=" * 70)
    print("POTHOLE DETECTION EDGE DEVICE UPLOADER")
    print("=" * 70)

    # Find all image-JSON pairs
    image_json_pairs = find_image_json_pairs(args.input_dir)
    if not image_json_pairs:
        print(f"[FATAL] No image-JSON pairs found in {args.input_dir}")
        return

    print(f"[INFO] Found {len(image_json_pairs)} image-JSON pairs to upload")

    # Connect to MinIO
    minio_client = connect_minio(config)
    if minio_client is None:
        return

    # Connect to Kafka
    producer = create_kafka_producer(config)
    if producer is None:
        return

    # Connect to Schema Registry
    schema_registry_client = create_schema_registry_client(config)
    if schema_registry_client is None:
        return

    # Create Avro serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
    )

    vehicle_id = f"vehicle-{uuid4().hex[:8]}"
    kafka_config = config.config["kafka"]
    uploader_config = config.config["uploader"]
    upload_interval = uploader_config.get("upload_interval", 10)
    topic = kafka_config["topic"]

    print(f"\n[INFO] Simulating vehicle: {vehicle_id}")
    print(
        f"[INFO] Uploading events every {upload_interval} seconds. Press Ctrl+C to stop.\n"
    )

    try:
        # Continuously cycle through the available image-JSON pairs
        pair_index = 0

        while True:
            # Get current image-JSON pair (cycle through)
            image_path, json_path, bev_image_path = image_json_pairs[pair_index]
            pair_index = (pair_index + 1) % len(image_json_pairs)

            # Load enriched data
            enriched_data_list = load_enriched_data(json_path)
            if not enriched_data_list:
                print("[WARN] Skipping due to JSON load failure.")
                time.sleep(upload_interval)
                continue

            # Process each mask in the enriched data
            for mask_data in enriched_data_list:
                # Generate event data
                event_id = str(uuid4())
                gps_lat, gps_lon = generate_random_hcm_gps(config)

                # Parse timestamp from JSON or use current time
                try:
                    timestamp_dt = datetime.fromisoformat(mask_data["timestamp"])
                    timestamp_ms = int(timestamp_dt.timestamp() * 1000)
                except (KeyError, ValueError):
                    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                print(f"\n{'='*70}")
                print(f"[EVENT] ID: {event_id}")
                print(f"[GPS] Lat: {gps_lat:.6f}, Lon: {gps_lon:.6f}")
                print(
                    f"[TIME] {datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()}"
                )
                print(f"[IMAGE] {os.path.basename(image_path)}")

                # Upload raw image to MinIO
                raw_s3_path = upload_image_to_minio(
                    minio_client, image_path, event_id, config, "raw_images"
                )
                if raw_s3_path is None:
                    print("[WARN] Skipping event due to raw image upload failure.")
                    continue

                # Upload BEV image to MinIO (if available)
                bev_s3_path = None
                if bev_image_path:
                    bev_s3_path = upload_image_to_minio(
                        minio_client, bev_image_path, event_id, config, "bev_images"
                    )

                # Extract mask data
                original_mask = mask_data.get("coordinates", [])
                bev_mask = mask_data.get("bev_mask", None)
                surface_area_cm2 = mask_data.get("area_cm2", 0.0)
                detection_confidence = mask_data.get("conf", None)

                # Create Avro record with updated schema
                raw_event = {
                    "event_id": event_id,
                    "vehicle_id": vehicle_id,
                    "timestamp": timestamp_ms,
                    "gps_lat": gps_lat,
                    "gps_lon": gps_lon,
                    "gps_accuracy": random.uniform(5.0, 15.0),  # meters
                    "raw_image_path": raw_s3_path,
                    "bev_image_path": bev_s3_path,
                    "original_mask": original_mask,
                    "bev_mask": bev_mask,
                    "surface_area_cm2": surface_area_cm2,
                    "detection_confidence": detection_confidence,
                }

                # Serialize with Avro + Schema Registry
                serialized_value = avro_serializer(
                    raw_event,
                    SerializationContext(topic, MessageField.VALUE),
                )

                # Produce to Kafka
                producer.produce(
                    topic=topic,
                    key=vehicle_id,
                    value=serialized_value,
                    on_delivery=delivery_report,
                )

                producer.poll(0)

                print(f"[INFO] Event queued for delivery")

            # Flush after processing all masks from one image
            producer.flush()

            print(f"[INFO] Waiting {upload_interval} seconds for next upload...")
            time.sleep(upload_interval)

    except KeyboardInterrupt:
        print("\n\n[INFO] Stopped by user.")

    finally:
        print("[INFO] Flushing Kafka producer...")
        producer.flush()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
