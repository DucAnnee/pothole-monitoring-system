"""
Unified processing and uploading unit.
Handles surface area estimation and cloud upload with persistent storage fallback.
"""

import json
import os
import time
import cv2
import numpy as np
from uuid import uuid4
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional, List
import random
import queue
import threading

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from minio import Minio
from minio.error import S3Error

from surface_area import PotholeAreaEstimator
from data_models import DetectionData, EnrichedData


# ============================================================================
# AVRO SCHEMA
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


class ProcessingUploader:
    """Unified processing and uploading unit."""

    def __init__(self, config, vehicle_id: str):
        """
        Initialize the processing and uploading unit.

        Args:
            config: Configuration object
            vehicle_id: Unique vehicle identifier
        """
        self.config = config
        self.vehicle_id = vehicle_id

        # Initialize surface area estimator
        self.estimator = PotholeAreaEstimator(
            trapezoid_coords=config.get_trapezoid_coords(),
            rectangle_coords=config.get_rectangle_coords(),
            calibration_path=config.get_calibration_path(),
        )

        # Persistent storage directory
        self.storage_dir = Path("persistent_storage")
        self.storage_dir.mkdir(exist_ok=True)
        self.images_dir = self.storage_dir / "images"
        self.images_dir.mkdir(exist_ok=True)
        self.metadata_dir = self.storage_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)

        # Cloud connection status
        self.minio_client: Optional[Minio] = None
        self.kafka_producer: Optional[Producer] = None
        self.avro_serializer: Optional[AvroSerializer] = None
        self.is_online = False

        # Statistics
        self.stats = {"processed": 0, "uploaded": 0, "stored": 0, "failed": 0}

        # Initialize connections
        self._initialize_connections()

    def _initialize_connections(self):
        """Initialize MinIO and Kafka connections."""
        try:
            # Connect to MinIO
            self.minio_client = self._connect_minio()

            # Connect to Kafka
            self.kafka_producer = self._create_kafka_producer()

            # Create Schema Registry client and serializer
            if self.kafka_producer:
                schema_client = self._create_schema_registry_client()
                if schema_client:
                    self.avro_serializer = AvroSerializer(
                        schema_client,
                        RAW_EVENT_SCHEMA_STR,
                    )

            # Check if both connections are successful
            self.is_online = (
                self.minio_client is not None
                and self.kafka_producer is not None
                and self.avro_serializer is not None
            )

            if self.is_online:
                print("[SUCCESS] All cloud connections established")
            else:
                print("[WARN] Operating in offline mode - will store to disk")

        except Exception as e:
            print(f"[ERROR] Failed to initialize connections: {e}")
            self.is_online = False

    def _connect_minio(self) -> Optional[Minio]:
        """Connect to MinIO."""
        minio_config = self.config.config["minio"]
        try:
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False),
            )

            bucket = minio_config["bucket"]
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)

            print(f"[SUCCESS] Connected to MinIO bucket: {bucket}")
            return client
        except Exception as e:
            print(f"[ERROR] MinIO connection failed: {e}")
            return None

    def _create_kafka_producer(self) -> Optional[Producer]:
        """Create Kafka producer."""
        kafka_config = self.config.config["kafka"]
        try:
            producer_conf = {
                "bootstrap.servers": kafka_config["bootstrap_servers"],
            }
            producer = Producer(producer_conf)
            print("[SUCCESS] Connected to Kafka brokers")
            return producer
        except Exception as e:
            print(f"[ERROR] Kafka connection failed: {e}")
            return None

    def _create_schema_registry_client(self) -> Optional[SchemaRegistryClient]:
        """Create Schema Registry client."""
        kafka_config = self.config.config["kafka"]
        try:
            schema_registry_conf = {"url": kafka_config["schema_registry_url"]}
            client = SchemaRegistryClient(schema_registry_conf)
            print("[SUCCESS] Connected to Schema Registry")
            return client
        except Exception as e:
            print(f"[ERROR] Schema Registry connection failed: {e}")
            return None

    def process_detection(self, detection: DetectionData) -> List[EnrichedData]:
        """
        Process detection data and enrich with surface area.

        Args:
            detection: Detection data from inference

        Returns:
            List of enriched data (one per mask)
        """
        enriched_list = []

        for mask in detection.masks:
            try:
                # Convert mask coordinates to numpy array
                pothole_mask = np.array(mask.coordinates)

                # Compute surface area and BEV
                area_cm2, bev_img, H, bev_mask = self.estimator.compute_pothole_area(
                    detection.frame, pothole_mask
                )

                # Create enriched data
                enriched = EnrichedData(
                    event_id=str(uuid4()),
                    frame_id=detection.frame_id,
                    timestamp=detection.timestamp,
                    frame=detection.frame,
                    bev_frame=bev_img,
                    conf=mask.conf,
                    coordinates=mask.coordinates,
                    bev_mask=bev_mask.tolist() if bev_mask is not None else None,
                    area_cm2=round(area_cm2, 2),
                )

                enriched_list.append(enriched)

            except Exception as e:
                print(f"[ERROR] Failed to process mask: {e}")
                continue

        return enriched_list

    def upload_to_cloud(self, enriched: EnrichedData) -> bool:
        """
        Upload enriched data to cloud (MinIO + Kafka).

        Args:
            enriched: Enriched data to upload

        Returns:
            True if successful, False otherwise
        """
        if not self.is_online:
            return False

        try:
            # Generate random GPS coordinates
            gps_lat, gps_lon = self._generate_random_gps()

            # Upload raw image to MinIO
            raw_s3_path = self._upload_image_to_minio(
                enriched.frame, enriched.event_id, "raw_images"
            )
            if not raw_s3_path:
                return False

            # Upload BEV image to MinIO (if available)
            bev_s3_path = None
            if enriched.bev_frame is not None:
                bev_s3_path = self._upload_image_to_minio(
                    enriched.bev_frame, enriched.event_id, "bev_images"
                )

            # Create Avro record
            timestamp_ms = int(enriched.timestamp.timestamp() * 1000)
            raw_event = {
                "event_id": enriched.event_id,
                "vehicle_id": self.vehicle_id,
                "timestamp": timestamp_ms,
                "gps_lat": gps_lat,
                "gps_lon": gps_lon,
                "gps_accuracy": random.uniform(5.0, 15.0),
                "raw_image_path": raw_s3_path,
                "bev_image_path": bev_s3_path,
                "original_mask": enriched.coordinates,
                "bev_mask": enriched.bev_mask,
                "surface_area_cm2": enriched.area_cm2,
                "detection_confidence": enriched.conf,
            }

            # Serialize and produce to Kafka
            topic = self.config.config["kafka"]["topic"]
            serialized_value = self.avro_serializer(
                raw_event,
                SerializationContext(topic, MessageField.VALUE),
            )

            self.kafka_producer.produce(
                topic=topic,
                key=self.vehicle_id,
                value=serialized_value,
                on_delivery=self._delivery_report,
            )

            self.kafka_producer.poll(0)

            print(
                f"[UPLOAD] Event {enriched.event_id} queued (area: {enriched.area_cm2:.2f} cm²)"
            )
            return True

        except Exception as e:
            print(f"[ERROR] Upload failed: {e}")
            return False

    def store_to_disk(self, enriched: EnrichedData):
        """
        Store enriched data to persistent storage.

        Args:
            enriched: Enriched data to store
        """
        try:
            # Save images
            raw_img_path = self.images_dir / f"{enriched.event_id}_raw.jpg"
            cv2.imwrite(
                str(raw_img_path), cv2.cvtColor(enriched.frame, cv2.COLOR_RGB2BGR)
            )

            if enriched.bev_frame is not None:
                bev_img_path = self.images_dir / f"{enriched.event_id}_bev.jpg"
                cv2.imwrite(
                    str(bev_img_path),
                    cv2.cvtColor(enriched.bev_frame, cv2.COLOR_RGB2BGR),
                )

            # Save metadata
            metadata = {
                "event_id": enriched.event_id,
                "frame_id": enriched.frame_id,
                "timestamp": enriched.timestamp.isoformat(),
                "conf": enriched.conf,
                "coordinates": enriched.coordinates,
                "bev_mask": enriched.bev_mask,
                "area_cm2": enriched.area_cm2,
            }

            metadata_path = self.metadata_dir / f"{enriched.event_id}.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            print(f"[STORED] Event {enriched.event_id} saved to disk")
            self.stats["stored"] += 1

        except Exception as e:
            print(f"[ERROR] Failed to store to disk: {e}")
            self.stats["failed"] += 1

    def process_persistent_storage(self):
        """Process and upload all data from persistent storage."""
        metadata_files = list(self.metadata_dir.glob("*.json"))

        if not metadata_files:
            print("[INFO] No pending data in persistent storage")
            return

        print(f"[INFO] Processing {len(metadata_files)} stored events...")

        for metadata_path in metadata_files:
            try:
                # Load metadata
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)

                event_id = metadata["event_id"]

                # Load images
                raw_img_path = self.images_dir / f"{event_id}_raw.jpg"
                bev_img_path = self.images_dir / f"{event_id}_bev.jpg"

                if not raw_img_path.exists():
                    print(f"[WARN] Missing raw image for {event_id}")
                    continue

                frame = cv2.imread(str(raw_img_path))
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

                bev_frame = None
                if bev_img_path.exists():
                    bev_frame = cv2.imread(str(bev_img_path))
                    bev_frame = cv2.cvtColor(bev_frame, cv2.COLOR_BGR2RGB)

                # Reconstruct enriched data
                enriched = EnrichedData(
                    event_id=event_id,
                    frame_id=metadata["frame_id"],
                    timestamp=datetime.fromisoformat(metadata["timestamp"]),
                    frame=frame,
                    bev_frame=bev_frame,
                    conf=metadata["conf"],
                    coordinates=metadata["coordinates"],
                    bev_mask=metadata.get("bev_mask"),
                    area_cm2=metadata["area_cm2"],
                )

                # Try to upload
                if self.upload_to_cloud(enriched):
                    # Delete files on success
                    raw_img_path.unlink()
                    if bev_img_path.exists():
                        bev_img_path.unlink()
                    metadata_path.unlink()
                    print(f"[CLEANUP] Deleted stored event {event_id}")
                else:
                    print(f"[WARN] Failed to upload stored event {event_id}")
                    break  # Stop processing if upload fails

            except Exception as e:
                print(f"[ERROR] Failed to process stored event: {e}")
                continue

    def _upload_image_to_minio(
        self, image: np.ndarray, event_id: str, prefix: str
    ) -> Optional[str]:
        """Upload image to MinIO."""
        if not self.minio_client:
            return None

        try:
            # Convert RGB to BGR and encode as JPEG
            image_bgr = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
            _, buffer = cv2.imencode(".jpg", image_bgr)
            image_bytes = buffer.tobytes()

            minio_config = self.config.config["minio"]
            bucket = minio_config["bucket"]
            object_name = f"{prefix}/{event_id}.jpg"

            self.minio_client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=BytesIO(image_bytes),
                length=len(image_bytes),
                content_type="image/jpeg",
            )

            return f"s3://{bucket}/{object_name}"

        except Exception as e:
            print(f"[ERROR] MinIO upload failed: {e}")
            return None

    def _generate_random_gps(self):
        """Generate random GPS coordinates."""
        gps_config = self.config.config["gps"]
        lat = random.uniform(gps_config["lat_min"], gps_config["lat_max"])
        lon = random.uniform(gps_config["lon_min"], gps_config["lon_max"])
        return lat, lon

    def _delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        else:
            self.stats["uploaded"] += 1

    def flush(self):
        """Flush Kafka producer."""
        if self.kafka_producer:
            self.kafka_producer.flush()

    def print_stats(self):
        """Print statistics."""
        print(f"\n{'='*70}")
        print("PROCESSING & UPLOAD STATISTICS")
        print(f"{'='*70}")
        print(f"Processed:  {self.stats['processed']}")
        print(f"Uploaded:   {self.stats['uploaded']}")
        print(f"Stored:     {self.stats['stored']}")
        print(f"Failed:     {self.stats['failed']}")
        print(f"{'='*70}\n")
