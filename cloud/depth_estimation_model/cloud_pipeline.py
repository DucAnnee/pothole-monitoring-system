"""
Cloud Depth Estimation Pipeline

Consumes raw events from 'pothole.raw.events.v1', performs depth estimation
using Depth-Anything-V2 model on BEV images, and produces results to 'pothole.depth.v1'.
"""

import sys
import os
import time
import numpy as np
from datetime import datetime, timezone
from io import BytesIO

import cv2
import torch
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from minio import Minio
from minio.error import S3Error
from PIL import Image

# Import Depth Anything V2 model
sys.path.append(os.path.join(os.path.dirname(__file__), "Depth-Anything-V2"))
from depth_anything_v2.dpt import DepthAnythingV2

# Import configuration loader
from config_loader import ConfigLoader


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
    {"name": "raw_image_path", "type": "string"},
    {"name": "bev_image_path", "type": ["null", "string"], "default": null},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "bev_mask", "type": ["null", {"type": "array", "items": {"type": "array", "items": "double"}}], "default": null},
    {"name": "surface_area_cm2", "type": "double"},
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
# DEPTH ESTIMATION CLASS
# ============================================================================
class DepthEstimator:
    """Wrapper class for Depth-Anything-V2 model."""

    def __init__(self, config: ConfigLoader):
        """Initialize depth estimator with configuration."""
        self.config = config
        self.model = None
        self.device = None
        self.depth_min_cm = config.get_depth_min_cm()
        self.depth_max_cm = config.get_depth_max_cm()
        self.confidence_min = config.get_confidence_min()
        self.confidence_max = config.get_confidence_max()
        self.input_size = config.get_model_input_size()

        self._load_model()

    def _load_model(self):
        """Load Depth-Anything-V2 model."""
        print("[INFO] Loading Depth-Anything-V2 model...")

        # Determine device
        device_config = self.config.get_model_device()
        if device_config == "auto":
            if torch.cuda.is_available():
                self.device = "cuda"
            elif torch.backends.mps.is_available():
                self.device = "mps"
            else:
                self.device = "cpu"
        else:
            self.device = device_config

        print(f"[INFO] Using device: {self.device}")

        # Get model configuration
        model_type = self.config.get_model_type()
        model_config = self.config.get_model_config()

        if not model_config:
            raise ValueError(f"Invalid model type: {model_type}")

        # Create model
        self.model = DepthAnythingV2(**model_config)

        # Load model weights
        model_path = self.config.get_model_path()

        # Handle relative path from cloud directory
        if not os.path.isabs(model_path):
            cloud_dir = os.path.dirname(os.path.abspath(__file__))
            model_path = os.path.join(cloud_dir, model_path)

        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found: {model_path}")

        print(f"[INFO] Loading model weights from: {model_path}")
        state_dict = torch.load(model_path, map_location=self.device)
        self.model.load_state_dict(state_dict)

        # Move model to device and set to eval mode
        self.model = self.model.to(self.device).eval()

        print(
            f"[SUCCESS] Model loaded successfully (type: {model_type}, device: {self.device})"
        )

    def estimate_depth(self, image_bytes):
        """
        Estimate depth from image bytes.

        Args:
            image_bytes: Image data as bytes

        Returns:
            tuple: (depth_cm, confidence)
        """
        try:
            # Convert bytes to numpy array
            image_array = np.frombuffer(image_bytes, dtype=np.uint8)
            image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

            if image is None:
                raise ValueError("Failed to decode image")

            # Run depth estimation using the model
            with torch.no_grad():
                depth_map = self.model.infer_image(image, self.input_size)

            # Calculate mean depth from the depth map
            # Focus on the center region where the pothole is likely to be
            h, w = depth_map.shape
            center_h, center_w = h // 2, w // 2
            roi_size = min(h, w) // 4

            # Extract region of interest (ROI)
            roi = depth_map[
                max(0, center_h - roi_size) : min(h, center_h + roi_size),
                max(0, center_w - roi_size) : min(w, center_w + roi_size),
            ]

            # Calculate statistics
            mean_depth = float(np.mean(roi))
            std_depth = float(np.std(roi))

            # Normalize depth to cm range
            # Assuming the depth map represents relative depth (0-255 or similar)
            # We need to map this to actual depth in cm
            # This is a simplified mapping - in production, you'd need calibration
            depth_normalized = (mean_depth - depth_map.min()) / (
                depth_map.max() - depth_map.min()
            )
            depth_cm = self.depth_min_cm + depth_normalized * (
                self.depth_max_cm - self.depth_min_cm
            )

            # Estimate confidence based on depth consistency
            # Lower std = higher confidence
            max_std = 50.0  # Arbitrary threshold
            confidence = 1.0 - min(std_depth / max_std, 1.0)
            confidence = self.confidence_min + confidence * (
                self.confidence_max - self.confidence_min
            )

            return round(depth_cm, 2), round(confidence, 4)

        except Exception as e:
            print(f"[ERROR] Depth estimation failed: {e}")
            # Return fallback values
            return 5.0, 0.5


# ============================================================================
# MINIO CLIENT
# ============================================================================
def connect_minio(config: ConfigLoader, max_retries=None, delay=None):
    """Initialize MinIO client with retry logic."""
    if max_retries is None:
        max_retries = config.get_minio_max_retries()
    if delay is None:
        delay = config.get_minio_retry_delay()

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to MinIO (attempt {attempt})...")
            client = Minio(
                config.get_minio_endpoint(),
                access_key=config.get_minio_access_key(),
                secret_key=config.get_minio_secret_key(),
                secure=config.get_minio_secure(),
            )

            # Verify bucket exists
            bucket = config.get_minio_bucket()
            if client.bucket_exists(bucket):
                print(f"[SUCCESS] Connected to MinIO bucket: {bucket}")
                return client
            else:
                print(f"[WARN] Bucket {bucket} does not exist")

        except S3Error as e:
            print(f"[ERROR] MinIO S3 error: {e}")
        except Exception as e:
            print(f"[ERROR] MinIO connection failed: {e}")

        if attempt < max_retries:
            print(f"[INFO] Retrying in {delay} seconds...")
            time.sleep(delay)

    print("[FATAL] Could not connect to MinIO after multiple attempts.")
    return None


def download_image_from_minio(client, s3_path, bucket):
    """Download image from MinIO. Returns image bytes or None."""
    try:
        # Parse s3://bucket/path format
        if s3_path.startswith("s3://"):
            path = s3_path[5:]  # Remove "s3://"
            parts = path.split("/", 1)
            bucket_name = parts[0]
            object_name = parts[1] if len(parts) > 1 else ""
        else:
            bucket_name = bucket
            object_name = s3_path

        response = client.get_object(bucket_name, object_name)
        image_bytes = response.read()
        response.close()
        response.release_conn()

        return image_bytes

    except Exception as e:
        print(f"[ERROR] Failed to download image from {s3_path}: {e}")
        return None


# ============================================================================
# KAFKA SETUP
# ============================================================================
def create_consumer(config: ConfigLoader):
    """Create Kafka consumer."""
    consumer_conf = {
        "bootstrap.servers": config.get_kafka_bootstrap_servers(),
        "group.id": config.get_kafka_consumer_group_id(),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_conf)
    source_topic = config.get_kafka_source_topic()
    consumer.subscribe([source_topic])
    print(f"[SUCCESS] Subscribed to {source_topic}")
    return consumer


def create_producer(config: ConfigLoader):
    """Create Kafka producer."""
    producer_conf = {
        "bootstrap.servers": config.get_kafka_bootstrap_servers(),
    }
    return Producer(producer_conf)


def create_deserializer(config: ConfigLoader):
    """Create Avro deserializer for raw events."""
    schema_registry_conf = {"url": config.get_kafka_schema_registry_url()}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    return AvroDeserializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        lambda obj, ctx: obj,
    )


def create_serializer(config: ConfigLoader):
    """Create Avro serializer for depth estimates."""
    schema_registry_conf = {"url": config.get_kafka_schema_registry_url()}
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
    print("CLOUD DEPTH ESTIMATION PIPELINE - Depth-Anything-V2")
    print("=" * 70)

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    config = ConfigLoader(config_path)

    print(f"[INFO] Configuration loaded from: {config_path}")

    # Initialize Depth Estimator
    depth_estimator = DepthEstimator(config)

    # Connect to MinIO
    minio_client = connect_minio(config)
    if minio_client is None:
        return

    # Setup Kafka
    consumer = create_consumer(config)
    producer = create_producer(config)
    deserializer = create_deserializer(config)
    serializer = create_serializer(config)

    source_topic = config.get_kafka_source_topic()
    output_topic = config.get_kafka_output_topic()

    print(f"\n[INFO] Consuming from: {source_topic}")
    print(f"[INFO] Producing to: {output_topic}")
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
                    msg.value(), SerializationContext(source_topic, MessageField.VALUE)
                )

                if raw_event is None:
                    continue

                event_id = raw_event["event_id"]
                image_path = raw_event["raw_image_path"]
                surface_area = raw_event.get("surface_area_cm2")
                detection_conf = raw_event.get("detection_confidence")
                message_count += 1

                print(f"\n[RECEIVED #{message_count}] event_id={event_id}")
                print(
                    f"[INFO] Surface area: {surface_area:.2f} cm², Detection confidence: {detection_conf:.4f}"
                )

                # Try to get BEV image first
                image_bytes = None
                bev_path = raw_event.get("bev_image_path")

                if config.get_use_bev_image() and bev_path is not None:
                    print(f"[INFO] Attempting to download BEV image: {bev_path}")
                    image_bytes = download_image_from_minio(
                        minio_client, bev_path, config.get_minio_bucket()
                    )

                # Fallback to regular image if BEV not found
                if image_bytes is None and config.get_fallback_to_regular_image():
                    print(f"[INFO] BEV not found, using regular image: {image_path}")
                    image_bytes = download_image_from_minio(
                        minio_client, image_path, config.get_minio_bucket()
                    )

                if image_bytes is None:
                    print(f"[ERROR] Could not download any image for event {event_id}")
                    continue

                print(f"[INFO] Image downloaded, size: {len(image_bytes)} bytes")

                # Perform depth estimation using Depth-Anything-V2
                start_time = time.time()
                depth_cm, confidence = depth_estimator.estimate_depth(image_bytes)
                elapsed_time = time.time() - start_time

                print(
                    f"[ESTIMATED] depth={depth_cm}cm, confidence={confidence:.4f} (took {elapsed_time:.3f}s)"
                )

                # Create output record
                processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                depth_estimate = {
                    "event_id": event_id,
                    "depth_cm": depth_cm,
                    "confidence": confidence,
                    "processed_at": processed_at,
                }

                # Serialize and produce
                serialized_value = serializer(
                    depth_estimate,
                    SerializationContext(output_topic, MessageField.VALUE),
                )

                producer.produce(
                    topic=output_topic,
                    key=event_id,
                    value=serialized_value,
                    on_delivery=delivery_report,
                )
                producer.poll(0)

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                import traceback

                traceback.print_exc()
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
