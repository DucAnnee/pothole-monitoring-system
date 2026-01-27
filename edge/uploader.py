"""
Uploading unit.
Handles cloud upload with persistent local storage fallback.
"""

import json
import cv2
import numpy as np
from uuid import uuid4
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, List
import random

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from minio import Minio

from data_models import DetectionData, BundledData

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
    {"name": "gps_acc", "type": ["null", "double"], "default": null},
    {"name": "image_path", "type": "string"},
    {"name": "mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "confidence", "type": ["null", "double"], "default": null}
  ]
}
"""

# ============================================================================
# UPLOADER WORKER
# ============================================================================


class Uploader:
    """Uploading unit"""

    def __init__(self, config, vehicle_id: str):
        """
        Initialize the uploading unit.

        Args:
            config: Configuration object
            vehicle_id: Unique vehicle identifier
        """

        self.config = config
        self.vehicle_id = vehicle_id

        # local storage directory
        self.storage_dir = Path("local_storage")
        self.storage_dir.mkdir(exist_ok=True)
        self.images_dir = self.storage_dir / "images"
        self.images_dir.mkdir(exist_ok=True)
        self.metadata_dir = self.storage_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)

        # cloud connection status
        self.minio_client: Optional[Minio]
        self.kafka_producer: Optional[Producer]
        self.avro_serializer: Optional[AvroSerializer]
        self.is_online = False

        # stats
        self.stats = {"processed": 0, "uploaded": 0, "stored": 0, "failed": 0}

        # connections init
        self._initialize_connections()

    def _initialize_connections(self):
        """Initialize MinIO and Kafka connections"""
        try:
            # connect to MinIO
            self.minio_client = self._connect_minio()

            # connect to Kafka
            self.kafka_producer = self._create_kafka_producer()

            # create Schema Registry client and serializer
            if self.kafka_producer:
                schema_client = self._create_schema_registry_client()
                if schema_client:
                    self.avro_serializer = AvroSerializer(
                        schema_client,  # type: ignore
                        RAW_EVENT_SCHEMA_STR,
                    )

            # check if both connections are successful
            self.is_online = (
                self.minio_client is not None
                and self.kafka_producer is not None
                and self.avro_serializer is not None
            )

            if self.is_online:
                print("[SUCCESS] All cloud connections established")
            else:
                print(
                    "[WARN] Operating in offline mode, storing processed data to disk"
                )

        except Exception as e:
            print(f"[ERROR] Failed to initialize connections: {e}")
            self.is_online = False

    def _connect_minio(self) -> Optional[Minio]:
        """Connect to MinIO"""
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
        """Create Kafka producer"""
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
        """Create Schema Registry client"""
        kafka_config = self.config.config["kafka"]
        try:
            schema_registry_conf = {"url": kafka_config["schema_registry_url"]}
            client = SchemaRegistryClient(schema_registry_conf)
            print("[SUCCESS] Connected to Schema Registry")
            return client

        except Exception as e:
            print(f"[ERROR] Schema Registry connection failed: {e}")
            return None

    def process_detection(self, detection: DetectionData) -> List[BundledData]:
        """
        Bundle segementation mask(s) in a detection instance.

        Args:
            detection: Detection data from inference

        Returns:
            List of bundled data (one per mask)
        """
        bundled_list = []

        for mask in detection.masks:
            try:
                # bundle data
                bundled = BundledData(
                    event_id=str(uuid4()),
                    frame_id=detection.frame_id,
                    timestamp=detection.timestamp,
                    frame=detection.frame,
                    conf=mask.conf,
                    coordinates=mask.coordinates,
                )
                bundled_list.append(bundled)

            except Exception as e:
                print(f"[ERROR] Failed to process mask: {e}")
                continue

        return bundled_list

    def upload_to_cloud(self, bundled: BundledData) -> bool:
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
            # generate random GPS coordinates
            gps_lat, gps_lon = self._generate_random_gps()

            # upload raw image to MinIO
            raw_s3_path = self._upload_image_to_minio(
                bundled.frame, bundled.event_id, "raw_images"
            )
            if not raw_s3_path:
                return False

            timestamp_ms = int(bundled.timestamp.timestamp() * 1000)
            raw_event = {
                "event_id": bundled.event_id,
                "vehicle_id": self.vehicle_id,
                "timestamp": timestamp_ms,
                "gps_lat": gps_lat,
                "gps_lon": gps_lon,
                "gps_acc": random.uniform(5.0, 15.0),
                "image_path": raw_s3_path,
                "mask": bundled.coordinates,
                "confidence": bundled.conf,
            }

            # Serialize and produce to Kafka
            topic = self.config.config["kafka"]["topic"]

            serialized_value = self.avro_serializer(  # type: ignore
                raw_event,
                SerializationContext(topic, MessageField.VALUE),
            )

            self.kafka_producer.produce(  # type: ignore
                topic=topic,
                key=self.vehicle_id,
                value=serialized_value,
                on_delivery=self._delivery_report,
            )

            self.kafka_producer.poll(0)  # type: ignore

            print(f"[UPLOAD] Event {bundled.event_id} queued")
            return True

        except Exception as e:
            print(f"[ERROR] Upload failed: {e}")
            return False

    def _generate_random_gps(self):
        """Generate random GPS coordinates"""
        gps_config = self.config.config["gps"]
        lat = random.uniform(gps_config["lat_min"], gps_config["lat_max"])
        lon = random.uniform(gps_config["lon_min"], gps_config["lon_max"])
        return lat, lon

    def _delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            self.stats["uploaded"] += 1

    def _upload_image_to_minio(
        self, image: np.ndarray, event_id: str, prefix: str
    ) -> Optional[str]:
        """Upload image to MinIO"""
        if not self.minio_client:
            return None

        try:
            # convert RGB to BGR and encode as JPEG
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

    def store_to_disk(self, bundled: BundledData):
        """
        Store bundled data to local storage.

        Args:
            bundled: Bundled data to store
        """
        try:
            # save images
            img_path = self.images_dir / f"{bundled.event_id}.jpg"
            cv2.imwrite(str(img_path), cv2.cvtColor(bundled.frame, cv2.COLOR_RGB2BGR))

            # save metadata
            metadata = {
                "event_id": bundled.event_id,
                "frame_id": bundled.frame_id,
                "timestamp": bundled.timestamp.isoformat(),
                "conf": bundled.conf,
                "coordinates": bundled.coordinates,
            }

            metadata_path = self.metadata_dir / f"{bundled.event_id}.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            print(f"[STORED] Event {bundled.event_id} saved to disk")
            self.stats["stored"] += 1
        except Exception as e:
            print(f"[ERROR] Failed to store to disk: {e}")
            self.stats["failed"] += 1

    def process_local_storage(self):
        """Process and upload all data from local storage"""
        metadata_files = list(self.metadata_dir.glob("*.json"))

        if not metadata_files:
            print("[INFO] No pending data in local storage")
            return

        print(f"[INFO] Processing {len(metadata_files)} stored events...")

        for metadata_path in metadata_files:
            try:
                # load metadata
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)

                event_id = metadata["event_id"]

                # load images
                img_path = self.images_dir / f"{event_id}.jpg"

                if not img_path.exists():
                    print(f"[WARN] Missing image for {event_id}")
                    continue

                frame = cv2.imread(str(img_path))
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)  # type: ignore

                # reconstruct bundled data
                bundled = BundledData(
                    event_id=event_id,
                    frame_id=metadata["frame_id"],
                    timestamp=datetime.fromisoformat(metadata["timestamp"]),
                    frame=frame,
                    conf=metadata["conf"],
                    coordinates=metadata["coordinates"],
                )

                # try upload
                if self.upload_to_cloud(bundled):
                    # delete files on success
                    img_path.unlink()
                    metadata_path.unlink()
                    print(f"[CLEANUP] Deleted stored event {event_id}")
                else:
                    print(f"[WARN] Failed to upload stored event {event_id}")
                    break  # stop processing if upload fails

            except Exception as e:
                print(f"[ERROR] Failed to process stored event: {e}")
                continue

    def flush(self):
        """Flush Kafka producer"""
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
