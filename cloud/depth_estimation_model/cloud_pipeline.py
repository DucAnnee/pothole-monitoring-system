"""
Cloud Depth Estimation Pipeline

Consumes surface area events from 'pothole.surface.area.v2', performs depth
estimation via Triton Inference Server (Depth-Anything-V2 ONNX), and produces
results to 'pothole.depth.v1'.

Preprocessing: exact 518×518 resize + ImageNet normalize (BGR→RGB, CHW float32).
Batching: client-side accumulator flushes on count >= max_batch_size OR elapsed >= batch_timeout_ms.
Triton also applies server-side dynamic batching independently.
"""

import os
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import cv2
import numpy as np
import tritonclient.grpc as tritongrpc
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from minio import Minio
from minio.error import S3Error

from config_loader import ConfigLoader

try:
    from cloud.shared.dlq import send_to_dlq
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from shared.dlq import send_to_dlq

DLQ_TOPIC = "pothole.depth.dlq.v1"
SERVICE_NAME = "depth-estimation"


# ============================================================================
# AVRO SCHEMAS
# ============================================================================
SURFACE_AREA_SCHEMA_STR = """
{
  "type": "record",
  "name": "SurfaceAreaEstimate",
  "namespace": "pothole.surface.v2",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "raw_image_object_key", "type": "string"},
    {"name": "bev_object_key", "type": "string"},
    {"name": "bev_mask", "type": "string"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "confidence", "type": ["null", "double"], "default": null},
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
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
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "processed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


# ============================================================================
# PREPROCESSING
# ============================================================================
class ImagePreprocessor:
    """Decode image bytes → [3, 518, 518] float32 CHW tensor (ImageNet normalized)."""

    MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    STD  = np.array([0.229, 0.224, 0.225], dtype=np.float32)

    def preprocess(self, image_bytes: bytes, input_size: int = 518) -> np.ndarray:
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)  # BGR uint8

        if img is None:
            raise ValueError("Failed to decode image bytes")

        img = cv2.resize(img, (input_size, input_size))
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img = img.astype(np.float32) / 255.0
        img = (img - self.MEAN) / self.STD   # HWC normalized
        return img.transpose(2, 0, 1)        # CHW float32


# ============================================================================
# TRITON CLIENT
# ============================================================================
class TritonDepthClient:
    """gRPC client for depth_anything_v2 model on Triton."""

    def __init__(self, host: str, grpc_port: int, model_name: str, model_version: str, timeout_seconds: int):
        url = f"{host}:{grpc_port}"
        self.client = tritongrpc.InferenceServerClient(url=url)
        self.model_name = model_name
        self.model_version = model_version
        self.timeout = float(timeout_seconds)

    def is_model_ready(self) -> bool:
        try:
            return self.client.is_model_ready(self.model_name, self.model_version)
        except Exception:
            return False

    def infer_batch(self, batch: np.ndarray) -> np.ndarray:
        """
        Args:
            batch: [N, 3, 518, 518] float32
        Returns:
            depth_maps: [N, 518, 518] float32
        """
        infer_input = tritongrpc.InferInput("input", list(batch.shape), "FP32")
        infer_input.set_data_from_numpy(batch)
        output = tritongrpc.InferRequestedOutput("depth_map")
        response = self.client.infer(
            model_name=self.model_name,
            model_version=self.model_version,
            inputs=[infer_input],
            outputs=[output],
            client_timeout=self.timeout,
        )
        return response.as_numpy("depth_map")  # [N, 518, 518]


def wait_for_triton_ready(
    client: TritonDepthClient,
    retries: int = 20,
    delay_seconds: float = 5.0,
) -> bool:
    """Block until Triton reports the model as ready. Returns False if unreachable after retries."""
    print(f"[INFO] Waiting for Triton model '{client.model_name}' to be ready ...")
    for attempt in range(1, retries + 1):
        if client.is_model_ready():
            print(f"[INFO] Triton model {client.model_name} is READY")
            return True
        print(f"[INFO] Triton not ready (attempt {attempt}/{retries}), retrying in {delay_seconds}s ...")
        time.sleep(delay_seconds)
    print(f"[WARN] Triton unavailable after {retries} attempts — running in FALLBACK mode (depth=5.0cm, confidence=0.5)")
    return False


# ============================================================================
# BATCH ACCUMULATOR
# ============================================================================
@dataclass
class PendingEvent:
    msg: object        # raw confluent_kafka Message
    event: dict        # deserialized surface area event
    image_bytes: bytes


class BatchAccumulator:
    def __init__(self, max_batch_size: int, batch_timeout_ms: int):
        self.max_batch_size = max_batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self._buffer: List[PendingEvent] = []
        self._deadline: float = self._next_deadline()

    def _next_deadline(self) -> float:
        return time.monotonic() + self.batch_timeout_ms / 1000.0

    def add(self, pending: PendingEvent) -> None:
        self._buffer.append(pending)

    def should_flush(self) -> bool:
        return (
            len(self._buffer) >= self.max_batch_size
            or (bool(self._buffer) and time.monotonic() >= self._deadline)
        )

    def flush(self) -> List[PendingEvent]:
        batch = self._buffer[:]
        self._buffer.clear()
        self._deadline = self._next_deadline()
        return batch

    def is_empty(self) -> bool:
        return len(self._buffer) == 0


# ============================================================================
# POSTPROCESSING
# ============================================================================
def postprocess_depth_map(
    depth_map: np.ndarray,
    depth_min_cm: float,
    depth_max_cm: float,
    confidence_min: float,
    confidence_max: float,
) -> tuple:
    """Center-ROI depth normalization. Returns (depth_cm, confidence)."""
    h, w = depth_map.shape
    roi_size = min(h, w) // 4
    ch, cw = h // 2, w // 2
    roi = depth_map[ch - roi_size:ch + roi_size, cw - roi_size:cw + roi_size]

    mean_d = float(np.mean(roi))
    std_d  = float(np.std(roi))

    dmin, dmax = float(depth_map.min()), float(depth_map.max())
    if dmax - dmin < 1e-6:
        return 5.0, 0.5

    depth_norm = (mean_d - dmin) / (dmax - dmin)
    depth_cm = depth_min_cm + depth_norm * (depth_max_cm - depth_min_cm)
    confidence = 1.0 - min(std_d / 50.0, 1.0)
    confidence = confidence_min + confidence * (confidence_max - confidence_min)

    return round(depth_cm, 2), round(confidence, 4)


# ============================================================================
# MINIO
# ============================================================================
def connect_minio(config: ConfigLoader) -> Optional[Minio]:
    max_retries = config.get_minio_max_retries()
    delay = config.get_minio_retry_delay()

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to MinIO (attempt {attempt}) ...")
            client = Minio(
                config.get_minio_endpoint(),
                access_key=config.get_minio_access_key(),
                secret_key=config.get_minio_secret_key(),
                secure=config.get_minio_secure(),
            )
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
            print(f"[INFO] Retrying in {delay}s ...")
            time.sleep(delay)

    print("[FATAL] Could not connect to MinIO after multiple attempts.")
    return None


def download_image_from_minio(client: Minio, s3_path: str, bucket: str) -> Optional[bytes]:
    try:
        if s3_path.startswith("s3://"):
            path = s3_path[5:]
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


def classify_surface_event_failure(event: dict) -> str | None:
    if not event.get("raw_image_object_key") and not event.get("bev_object_key"):
        return "missing_raw_and_bev_image_object_key"
    if event.get("surface_area_cm2") is None:
        return "missing_surface_area_cm2"
    return None


# ============================================================================
# KAFKA
# ============================================================================
def create_consumer(config: ConfigLoader) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": config.get_kafka_bootstrap_servers(),
        "group.id": config.get_kafka_consumer_group_id(),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    source_topic = config.get_kafka_source_topic()
    consumer.subscribe([source_topic])
    print(f"[SUCCESS] Subscribed to {source_topic}")
    return consumer


def create_producer(config: ConfigLoader) -> Producer:
    return Producer({"bootstrap.servers": config.get_kafka_bootstrap_servers()})


def create_deserializer(config: ConfigLoader) -> AvroDeserializer:
    client = SchemaRegistryClient({"url": config.get_kafka_schema_registry_url()})
    return AvroDeserializer(client, SURFACE_AREA_SCHEMA_STR, lambda obj, ctx: obj)


def create_serializer(config: ConfigLoader) -> AvroSerializer:
    client = SchemaRegistryClient({"url": config.get_kafka_schema_registry_url()})
    return AvroSerializer(client, DEPTH_ESTIMATE_SCHEMA_STR, lambda obj, ctx: obj)


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[DELIVERED] {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_and_flush(producer: Producer, topic: str, key: str, value: bytes, timeout: int = 30) -> None:
    delivery_error = {"error": None}

    def callback(err, msg):
        delivery_report(err, msg)
        if err is not None:
            delivery_error["error"] = err

    producer.produce(topic=topic, key=key, value=value, on_delivery=callback)
    remaining = producer.flush(timeout)
    if remaining > 0:
        raise TimeoutError(f"Timed out delivering message to {topic}")
    if delivery_error["error"] is not None:
        raise RuntimeError(f"Failed delivering message to {topic}: {delivery_error['error']}")


# ============================================================================
# BATCH PROCESSING
# ============================================================================
def _process_batch(
    batch: List[PendingEvent],
    preprocessor: ImagePreprocessor,
    triton_client: TritonDepthClient,
    producer: Producer,
    consumer: Consumer,
    serializer: AvroSerializer,
    config: ConfigLoader,
    triton_available: bool = True,
) -> None:
    try:
        output_topic = config.get_kafka_output_topic()
        processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)

        if triton_available:
            input_size = config.get_model_input_size()
            tensors = np.stack([
                preprocessor.preprocess(e.image_bytes, input_size) for e in batch
            ])  # [N, 3, 518, 518]
            depth_maps = triton_client.infer_batch(tensors)  # [N, 518, 518]
            depth_results = [
                postprocess_depth_map(
                    dm,
                    config.get_depth_min_cm(),
                    config.get_depth_max_cm(),
                    config.get_confidence_min(),
                    config.get_confidence_max(),
                )
                for dm in depth_maps
            ]
        else:
            depth_results = [(5.0, 0.5)] * len(batch)

        for pending, (depth_cm, confidence) in zip(batch, depth_results):
            record = {
                "event_id": pending.event["event_id"],
                "depth_cm": depth_cm,
                "confidence": confidence,
                "surface_area_cm2": pending.event["surface_area_cm2"],
                "processed_at": processed_at,
            }
            serialized = serializer(record, SerializationContext(output_topic, MessageField.VALUE))
            produce_and_flush(producer, output_topic, record["event_id"], serialized)
            consumer.commit(message=pending.msg)

        print(f"[INFO] Batch of {len(batch)} processed and committed")

    except Exception:
        print(f"[ERROR] Batch processing failed — events uncommitted, will replay on restart")
        traceback.print_exc()


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("=" * 70)
    print("CLOUD DEPTH ESTIMATION PIPELINE - Triton / Depth-Anything-V2")
    print("=" * 70)

    config_path = os.environ.get(
        "DEPTH_SERVICE_CONFIG",
        os.environ.get(
            "POTHOLE_CONFIG_PATH",
            os.path.join(os.path.dirname(__file__), "config.yaml"),
        ),
    )
    config = ConfigLoader(config_path)
    print(f"[INFO] Configuration loaded from: {config_path}")

    preprocessor = ImagePreprocessor()

    triton_client = TritonDepthClient(
        host=config.get_triton_host(),
        grpc_port=config.get_triton_grpc_port(),
        model_name=config.get_triton_model_name(),
        model_version=config.get_triton_model_version(),
        timeout_seconds=config.get_triton_timeout_seconds(),
    )
    triton_available = wait_for_triton_ready(triton_client)

    minio_client = connect_minio(config)
    if minio_client is None:
        return

    consumer = create_consumer(config)
    producer = create_producer(config)
    dlq_producer = create_producer(config)
    deserializer = create_deserializer(config)
    serializer = create_serializer(config)

    accumulator = BatchAccumulator(
        max_batch_size=config.get_batching_max_batch_size(),
        batch_timeout_ms=config.get_batching_batch_timeout_ms(),
    )

    source_topic = config.get_kafka_source_topic()
    output_topic = config.get_kafka_output_topic()
    print(f"\n[INFO] Consuming from: {source_topic}")
    print(f"[INFO] Producing to:   {output_topic}")
    print(f"[INFO] Batch size: {config.get_batching_max_batch_size()}, "
          f"timeout: {config.get_batching_batch_timeout_ms()}ms")
    print(f"[INFO] Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(timeout=0.05)

            if msg is not None and not msg.error():
                surface_event = None
                try:
                    surface_event = deserializer(
                        msg.value(), SerializationContext(source_topic, MessageField.VALUE)
                    )
                except Exception as e:
                    print(f"[ERROR] Deserialization failed: {e}")
                    send_to_dlq(
                        dlq_producer,
                        DLQ_TOPIC,
                        source_topic,
                        SERVICE_NAME,
                        e,
                        msg.value(),
                    )
                    consumer.commit(message=msg)

                if surface_event is not None:
                    event_id = surface_event.get("event_id")
                    failure_reason = classify_surface_event_failure(surface_event)
                    if failure_reason is not None:
                        print(f"[ERROR] Malformed surface event {event_id}: {failure_reason}")
                        send_to_dlq(
                            dlq_producer,
                            DLQ_TOPIC,
                            source_topic,
                            SERVICE_NAME,
                            ValueError(failure_reason),
                            surface_event,
                            key=event_id,
                        )
                        consumer.commit(message=msg)
                        continue

                    bev_key  = surface_event.get("bev_object_key", "")
                    raw_key  = surface_event["raw_image_object_key"]

                    image_bytes = None
                    if bev_key:
                        image_bytes = download_image_from_minio(
                            minio_client, bev_key, config.get_minio_bucket()
                        )
                    if image_bytes is None:
                        image_bytes = download_image_from_minio(
                            minio_client, raw_key, config.get_minio_bucket()
                        )

                    if image_bytes is not None:
                        accumulator.add(PendingEvent(msg, surface_event, image_bytes))
                    else:
                        print(f"[ERROR] Could not download any image for event {event_id} - skipping")
                        send_to_dlq(
                            dlq_producer,
                            DLQ_TOPIC,
                            source_topic,
                            SERVICE_NAME,
                            "could_not_download_image",
                            surface_event,
                            key=event_id,
                        )
                        consumer.commit(message=msg)

            elif msg is not None and msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")

            if accumulator.should_flush():
                batch = accumulator.flush()
                _process_batch(batch, preprocessor, triton_client, producer, consumer, serializer, config, triton_available)

    except KeyboardInterrupt:
        print("\n\n[INFO] Stopped by user.")

    finally:
        print("[INFO] Flushing producer ...")
        producer.flush()
        dlq_producer.flush()
        print("[INFO] Closing consumer ...")
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
