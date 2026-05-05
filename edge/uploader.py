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
from typing import Any, List, Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from minio import Minio

from data_models import DetectionData, BundledData
from gps_provider import build_gps_provider
from pipeline_logger import get_pipeline_logger, log_event

# ============================================================================
# AVRO SCHEMA
# ============================================================================
RAW_EVENT_SCHEMA_STR = """
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v2",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "raw_image_object_key", "type": "string"},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null}
  ]
}
"""

# ============================================================================
# UPLOADER WORKER
# ============================================================================


class Uploader:
    """Uploading unit"""

    def __init__(
        self,
        config,
        vehicle_id: str,
        device_id: str | None = None,
        gps_provider: Any = None,
    ):
        """
        Initialize the uploading unit.

        Args:
            config: Configuration object
            vehicle_id: Unique vehicle identifier
            device_id: Stable edge device identifier
            gps_provider: Provider used to read GPS fixes
        """

        self.config = config
        self.vehicle_id = vehicle_id
        self.device_id = device_id
        self.gps_provider = gps_provider or build_gps_provider(config)
        self.logger = get_pipeline_logger("uploader")

        # local storage directory
        self.storage_dir = Path("local_storage")
        self.storage_dir.mkdir(exist_ok=True)
        self.images_dir = self.storage_dir / "images"
        self.images_dir.mkdir(exist_ok=True)
        self.metadata_dir = self.storage_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)

        # cloud connection status
        self.minio_client: Optional[Minio] = None
        self.kafka_producer: Optional[Producer] = None
        self.avro_serializer: Optional[AvroSerializer] = None
        self.is_online = False

        # stats
        self.stats = {"processed": 0, "uploaded": 0, "stored": 0, "failed": 0}

        # connections init
        self._initialize_connections()

    def _initialize_connections(self) -> None:
        """Initialize MinIO and Kafka connections"""
        self.minio_client = None
        self.kafka_producer = None
        self.avro_serializer = None
        self.is_online = False

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
                log_event(self.logger, "cloud_connections_ready")
            else:
                self.logger.warning(
                    "Operating in offline mode, storing processed data to disk"
                )

        except Exception as e:
            self.logger.error("Failed to initialize connections: %s", e)
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

            log_event(self.logger, "minio_connected", bucket=bucket)
            return client

        except Exception as e:
            self.logger.error("MinIO connection failed: %s", e)
            return None

    def _create_kafka_producer(self) -> Optional[Producer]:
        """Create Kafka producer"""
        kafka_config = self.config.config["kafka"]
        try:
            producer_conf = {
                "bootstrap.servers": kafka_config["bootstrap_servers"],
            }
            producer = Producer(producer_conf)
            log_event(self.logger, "kafka_connected")
            return producer

        except Exception as e:
            self.logger.error("Kafka connection failed: %s", e)
            return None

    def _create_schema_registry_client(self) -> Optional[SchemaRegistryClient]:
        """Create Schema Registry client"""
        kafka_config = self.config.config["kafka"]
        try:
            schema_registry_conf = {"url": kafka_config["schema_registry_url"]}
            client = SchemaRegistryClient(schema_registry_conf)
            log_event(self.logger, "schema_registry_connected")
            return client

        except Exception as e:
            self.logger.error("Schema Registry connection failed: %s", e)
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
                self.logger.error("Failed to process mask: %s", e)
                continue

        return bundled_list

    def upload_to_cloud(self, bundled: BundledData, gps: Any = None) -> bool:
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
            # upload raw image to MinIO
            raw_s3_path = self._upload_image_to_minio(
                bundled.frame, bundled.event_id, "raw_images"
            )
            if not raw_s3_path:
                return False

            raw_event = self._build_raw_event(bundled, raw_s3_path, gps=gps)

            # Serialize and produce to Kafka
            topic = self.config.config["kafka"]["topic"]

            serialized_value = self.avro_serializer(  # type: ignore
                raw_event,
                SerializationContext(topic, MessageField.VALUE),
            )

            delivery_errors: list[str] = []

            def delivery_report(err, msg):
                if err is not None:
                    delivery_errors.append(str(err))
                self._delivery_report(err, msg)

            self.kafka_producer.produce(  # type: ignore
                topic=topic,
                key=self.vehicle_id,
                value=serialized_value,
                on_delivery=delivery_report,
            )

            timeout = float(self.config.config["kafka"].get("delivery_timeout", 10))
            pending = self.kafka_producer.flush(timeout)  # type: ignore
            if pending > 0:
                self.logger.error(
                    "Kafka delivery timed out for %s; %s message(s) still pending",
                    bundled.event_id,
                    pending,
                )
                return False
            if delivery_errors:
                self.logger.error(
                    "Kafka delivery failed for %s: %s",
                    bundled.event_id,
                    "; ".join(delivery_errors),
                )
                return False

            log_event(
                self.logger,
                "upload_delivered",
                event_id=bundled.event_id,
                frame_id=bundled.frame_id,
            )
            return True

        except Exception as e:
            self.logger.error("Upload failed: %s", e)
            return False

    def _gps_raw_fields(self, gps: Any = None) -> dict[str, float | None]:
        if gps is None:
            gps = self.gps_provider.read()
        if hasattr(gps, "to_raw_event_fields"):
            return gps.to_raw_event_fields()
        return {
            "gps_lat": gps["gps_lat"],
            "gps_lon": gps["gps_lon"],
            "gps_accuracy": gps.get("gps_accuracy"),
        }

    def _build_raw_event(
        self,
        bundled: BundledData,
        raw_s3_path: str,
        gps: Any = None,
    ) -> dict[str, Any]:
        timestamp_ms = int(bundled.timestamp.timestamp() * 1000)
        return {
            "event_id": bundled.event_id,
            "vehicle_id": self.vehicle_id,
            "timestamp": timestamp_ms,
            **self._gps_raw_fields(gps),
            "raw_image_object_key": raw_s3_path,
            "original_mask": bundled.coordinates,
            "detection_confidence": bundled.conf,
        }

    def _delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            self.logger.error("Kafka delivery failed: %s", err)
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
            encoded, buffer = cv2.imencode(".jpg", image_bgr)
            if not encoded:
                self.logger.error("JPEG encoding failed for event %s", event_id)
                return None
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
            self.logger.error("MinIO upload failed: %s", e)
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
            image_written = cv2.imwrite(
                str(img_path),
                cv2.cvtColor(bundled.frame, cv2.COLOR_RGB2BGR),
            )
            if not image_written:
                raise OSError(f"Could not write local image: {img_path}")

            # save metadata
            metadata = {
                "event_id": bundled.event_id,
                "frame_id": bundled.frame_id,
                "timestamp": bundled.timestamp.isoformat(),
                "conf": bundled.conf,
                "coordinates": bundled.coordinates,
                "vehicle_id": self.vehicle_id,
                "device_id": self.device_id,
                "gps": self._gps_raw_fields(),
            }

            metadata_path = self.metadata_dir / f"{bundled.event_id}.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            log_event(
                self.logger,
                "event_stored_local",
                event_id=bundled.event_id,
                frame_id=bundled.frame_id,
            )
            self.stats["stored"] += 1
        except Exception as e:
            self.logger.error("Failed to store to disk: %s", e)
            self.stats["failed"] += 1

    def process_local_storage(self):
        """Process and upload all data from local storage"""
        metadata_files = list(self.metadata_dir.glob("*.json"))

        if not metadata_files:
            log_event(self.logger, "local_storage_empty")
            return

        log_event(
            self.logger,
            "local_storage_replay",
            stored_events=len(metadata_files),
        )

        for metadata_path in metadata_files:
            try:
                # load metadata
                with open(metadata_path, "r") as f:
                    metadata = json.load(f)

                event_id = metadata["event_id"]

                # load images
                img_path = self.images_dir / f"{event_id}.jpg"

                if not img_path.exists():
                    self.logger.warning("Missing image for %s", event_id)
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
                if self.upload_to_cloud(bundled, gps=metadata.get("gps")):
                    # delete files on success
                    img_path.unlink()
                    metadata_path.unlink()
                    log_event(self.logger, "stored_event_deleted", event_id=event_id)
                else:
                    self.logger.warning("Failed to upload stored event %s", event_id)
                    break  # stop processing if upload fails

            except Exception as e:
                self.logger.error("Failed to process stored event: %s", e)
                continue

    def flush(self):
        """Flush Kafka producer"""
        if self.kafka_producer:
            self.kafka_producer.flush()

    def print_stats(self):
        """Print statistics."""
        log_event(
            self.logger,
            "upload_stats",
            processed=self.stats["processed"],
            uploaded=self.stats["uploaded"],
            stored=self.stats["stored"],
            failed=self.stats["failed"],
        )
