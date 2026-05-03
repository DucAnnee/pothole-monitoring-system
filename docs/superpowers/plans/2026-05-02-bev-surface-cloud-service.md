# BEV Surface Area Cloud Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move BEV transformation + pothole surface area estimation from the edge device into a new cloud microservice (`bev_surface_service`), update all downstream Kafka schemas and service code accordingly.

**Architecture:** New `cloud/bev_surface_service/` consumes `pothole.raw.events.v2`, runs `PotholeAreaEstimator`, uploads BEV image to MinIO, publishes `pothole.surface.area.v2`. Depth service changes to consume `surface.area.v2` (BEV image guaranteed) and passes `surface_area_cm2` through in `pothole.depth.v1`. Severity service drops its two-topic join and becomes a single-topic consumer on `depth.v1`.

**Tech Stack:** Python 3.11, `confluent-kafka` (Avro/Schema Registry), `minio`, `opencv-python`, `numpy`, PyArrow, Trino, existing `PotholeAreaEstimator` logic from edge.

---

## File Map

| Action | Path | What changes |
|--------|------|-------------|
| Modify | `KAFKA-CONF.md` | Add `raw_image_object_key` to `surface.area.v2`; add `surface_area_cm2` to `depth.v1`; update topic table |
| Modify | `edge/uploader.py` | New Avro schema (v2 namespace + field names); topic `v1` → `v2` |
| Modify | `edge/config.yaml` | `kafka.topic` → `pothole.raw.events.v2` |
| Create | `cloud/bev_surface_service/pothole_area_estimator.py` | Copy from edge (file-based calibration kept) |
| Create | `cloud/bev_surface_service/config.py` | YAML config loader (same `${VAR:default}` pattern) |
| Create | `cloud/bev_surface_service/bev_processor.py` | MinIO download + `PotholeAreaEstimator` + MinIO upload |
| Create | `cloud/bev_surface_service/bev_surface_service.py` | Kafka consumer/producer main loop |
| Create | `cloud/bev_surface_service/config.yaml` | Service configuration |
| Create | `cloud/bev_surface_service/CLAUDE.md` | Service documentation |
| Modify | `cloud/depth_estimation_model/cloud_pipeline.py` | Consume `surface.area.v2`; extend `depth.v1` schema with `surface_area_cm2`; pass through in output |
| Modify | `cloud/depth_estimation_model/config.yaml` | `source_topic` → `pothole.surface.area.v2`; remove `use_bev_image`/`fallback_to_regular_image` |
| Modify | `cloud/depth_estimation_model/config_loader.py` | Remove `get_use_bev_image()`, `get_fallback_to_regular_image()` |
| Modify | `cloud/severity_calculation_service/severity_aggregator.py` | Single-topic consumer on `depth.v1`; remove `AggregationStore` join |
| Modify | `cloud/severity_calculation_service/config.yaml` | Remove `raw_events_topic`; remove `aggregation` section |
| Modify | `cloud/severity_calculation_service/config_loader.py` | Remove `raw_events_topic` property + its validation |
| Modify | `cloud/final_enrichment_service/final_enrichment_service.py` | Update raw event Avro schema (v2 fields); rename `raw_image_path` → `raw_image_object_key` in DDL + UPSERT |
| Modify | `cloud/etl_service/etl_microservice.py` | Update raw events schema/DDL to v2; add `surface.area.v2` topic mapping |

---

## Task 1: Update KAFKA-CONF.md schema contracts

**Files:**
- Modify: `KAFKA-CONF.md`

- [ ] **Step 1: Add `raw_image_object_key` passthrough to `surface.area.v2` schema**

In `KAFKA-CONF.md`, replace the `pothole.surface.area.v2` schema block with:

```json
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
```

- [ ] **Step 2: Extend `pothole.depth.v1` schema with `surface_area_cm2`**

Replace the `pothole.depth.v1` schema block with:

```json
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
```

- [ ] **Step 3: Commit**

```bash
git add KAFKA-CONF.md
git commit -m "docs: update Avro schemas - surface.area.v2 passthrough + depth.v1 surface_area_cm2"
```

---

## Task 2: Update edge — Avro schema v2 + topic

**Files:**
- Modify: `edge/uploader.py`
- Modify: `edge/config.yaml`

- [ ] **Step 1: Update `RAW_EVENT_SCHEMA_STR` in `edge/uploader.py`**

Replace the `RAW_EVENT_SCHEMA_STR` constant (lines 27–44) with:

```python
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
```

- [ ] **Step 2: Update `upload_to_cloud()` in `edge/uploader.py` to use v2 field names**

Replace the `raw_event` dict construction inside `upload_to_cloud()` (the dict starting with `"event_id": bundled.event_id`) with:

```python
raw_event = {
    "event_id": bundled.event_id,
    "vehicle_id": self.vehicle_id,
    "timestamp": timestamp_ms,
    "gps_lat": gps_lat,
    "gps_lon": gps_lon,
    "gps_accuracy": random.uniform(5.0, 15.0),
    "raw_image_object_key": raw_s3_path,
    "original_mask": bundled.coordinates,
    "detection_confidence": bundled.conf,
}
```

- [ ] **Step 3: Update Kafka topic in `edge/config.yaml`**

Change line:
```yaml
  topic: "pothole.raw.events.v1"
```
to:
```yaml
  topic: "pothole.raw.events.v2"
```

- [ ] **Step 4: Commit**

```bash
git add edge/uploader.py edge/config.yaml
git commit -m "feat(edge): update Avro schema and Kafka topic to v2"
```

---

## Task 3: Create `cloud/bev_surface_service/`

**Files:**
- Create: `cloud/bev_surface_service/pothole_area_estimator.py`
- Create: `cloud/bev_surface_service/config.py`
- Create: `cloud/bev_surface_service/bev_processor.py`
- Create: `cloud/bev_surface_service/bev_surface_service.py`
- Create: `cloud/bev_surface_service/config.yaml`
- Create: `cloud/bev_surface_service/CLAUDE.md`

- [ ] **Step 1: Copy `PotholeAreaEstimator` from edge**

Copy `edge/surface_area/pothole_area_estimator.py` verbatim to `cloud/bev_surface_service/pothole_area_estimator.py`. No changes needed — the class is self-contained.

```bash
cp edge/surface_area/pothole_area_estimator.py cloud/bev_surface_service/pothole_area_estimator.py
```

- [ ] **Step 2: Write `cloud/bev_surface_service/config.py`**

```python
"""Configuration loader for BEV Surface Service."""

from typing import Any, Dict
import yaml
import os


class ConfigLoader:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        self.config_path = config_path
        self.config: Dict[str, Any] = self._load_config()
        self._validate_config()

    def _load_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)
        return self._substitute_env_vars(config)

    def _substitute_env_vars(self, config) -> Any:
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            var = config[2:-1]
            if ":" in var:
                name, default = var.split(":", 1)
                return os.environ.get(name, default)
            return os.environ.get(var, config)
        return config

    def _validate_config(self):
        for section in ("kafka", "minio", "bev"):
            if section not in self.config:
                raise ValueError(f"Missing required config section: {section}")

    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.config["kafka"]["bootstrap_servers"]

    @property
    def schema_registry_url(self) -> str:
        return self.config["kafka"]["schema_registry_url"]

    @property
    def kafka_consumer_group_id(self) -> str:
        return self.config["kafka"]["consumer_group_id"]

    @property
    def kafka_source_topic(self) -> str:
        return self.config["kafka"]["source_topic"]

    @property
    def kafka_output_topic(self) -> str:
        return self.config["kafka"]["output_topic"]

    @property
    def minio_endpoint(self) -> str:
        return self.config["minio"]["endpoint"]

    @property
    def minio_access_key(self) -> str:
        return self.config["minio"]["access_key"]

    @property
    def minio_secret_key(self) -> str:
        return self.config["minio"]["secret_key"]

    @property
    def minio_bucket(self) -> str:
        return self.config["minio"]["bucket"]

    @property
    def minio_secure(self) -> bool:
        return self.config["minio"].get("secure", False)

    @property
    def trapezoid_coords(self) -> list:
        return self.config["bev"]["trapezoid_coords"]

    @property
    def rectangle_coords(self) -> list:
        return self.config["bev"]["rectangle_coords"]

    @property
    def calibration_file(self) -> str:
        return self.config["bev"]["calibration_file"]
```

- [ ] **Step 3: Write `cloud/bev_surface_service/bev_processor.py`**

```python
"""BEV image processing: download raw image, run PotholeAreaEstimator, upload BEV."""

import json
import numpy as np
import cv2
from io import BytesIO
from typing import Optional, Tuple

from minio import Minio
from minio.error import S3Error

from pothole_area_estimator import PotholeAreaEstimator


class BEVProcessor:
    def __init__(self, minio_client: Minio, bucket: str, estimator: PotholeAreaEstimator):
        self.minio = minio_client
        self.bucket = bucket
        self.estimator = estimator

    def process(self, event_id: str, raw_image_object_key: str, original_mask: list) -> Tuple[Optional[str], Optional[str], float, float]:
        """
        Download raw image, compute BEV + surface area, upload BEV image.

        Returns:
            (bev_object_key, bev_mask_json, surface_area_cm2, confidence)
            On failure: (None, None, 0.0, 0.0)
        """
        image_bytes = self._download(raw_image_object_key)
        if image_bytes is None:
            return None, None, 0.0, 0.0

        try:
            image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR)
            if image is None:
                raise ValueError("Failed to decode image bytes")
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            mask_np = np.array(original_mask, dtype=np.float32)
            area_cm2, bev_img, _H, bev_mask_np = self.estimator.compute_pothole_area(image_rgb, mask_np)

            bev_object_key = f"bev_images/{event_id}.jpg"
            self._upload_bev(bev_img, bev_object_key)

            bev_mask_json = json.dumps(bev_mask_np.tolist())
            return bev_object_key, bev_mask_json, float(area_cm2), 1.0

        except Exception as e:
            print(f"[ERROR] BEV processing failed for {event_id}: {e}")
            return None, None, 0.0, 0.0

    def _download(self, object_key: str) -> Optional[bytes]:
        """Download object from MinIO. Strips s3://bucket/ prefix if present."""
        key = object_key
        if object_key.startswith("s3://"):
            parts = object_key[5:].split("/", 1)
            key = parts[1] if len(parts) > 1 else ""
        try:
            resp = self.minio.get_object(self.bucket, key)
            data = resp.read()
            resp.close()
            resp.release_conn()
            return data
        except Exception as e:
            print(f"[ERROR] MinIO download failed ({key}): {e}")
            return None

    def _upload_bev(self, bev_img_rgb: np.ndarray, object_key: str):
        """Encode BEV image as JPEG and upload to MinIO."""
        bev_bgr = cv2.cvtColor(bev_img_rgb, cv2.COLOR_RGB2BGR)
        _, buf = cv2.imencode(".jpg", bev_bgr)
        data = buf.tobytes()
        self.minio.put_object(
            bucket_name=self.bucket,
            object_name=object_key,
            data=BytesIO(data),
            length=len(data),
            content_type="image/jpeg",
        )
```

- [ ] **Step 4: Write failing unit test for `BEVProcessor.process()`**

Create `cloud/bev_surface_service/test_bev_processor.py`:

```python
"""Unit tests for BEVProcessor."""

import json
import numpy as np
import pytest
from unittest.mock import MagicMock, patch

from bev_processor import BEVProcessor


def make_processor(estimator_result=None):
    minio = MagicMock()
    minio.get_object.return_value.__enter__ = lambda s: s
    minio.get_object.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate download returning a valid 100x100 white JPEG
    import cv2, numpy as np
    img = np.ones((100, 100, 3), dtype=np.uint8) * 255
    _, buf = cv2.imencode(".jpg", img)
    mock_resp = MagicMock()
    mock_resp.read.return_value = buf.tobytes()
    mock_resp.close = MagicMock()
    mock_resp.release_conn = MagicMock()
    minio.get_object.return_value = mock_resp

    estimator = MagicMock()
    bev_img = np.ones((100, 100, 3), dtype=np.uint8) * 128
    bev_mask = np.array([[10, 10], [20, 10], [20, 20], [10, 20]], dtype=np.float32)
    if estimator_result is None:
        estimator.compute_pothole_area.return_value = (500.0, bev_img, np.eye(3), bev_mask)
    else:
        estimator.compute_pothole_area.return_value = estimator_result

    return BEVProcessor(minio, "warehouse", estimator), minio, estimator


def test_process_success_returns_bev_key_and_area():
    proc, minio, _ = make_processor()
    bev_key, bev_mask_json, area, confidence = proc.process(
        "evt-1", "raw_images/evt-1.jpg", [[10.0, 10.0], [20.0, 10.0]]
    )
    assert bev_key == "bev_images/evt-1.jpg"
    assert area == 500.0
    assert confidence == 1.0
    assert json.loads(bev_mask_json) is not None
    minio.put_object.assert_called_once()


def test_process_download_failure_returns_zeros():
    proc, minio, _ = make_processor()
    minio.get_object.side_effect = Exception("connection refused")
    bev_key, bev_mask_json, area, confidence = proc.process(
        "evt-2", "raw_images/evt-2.jpg", [[10.0, 10.0]]
    )
    assert bev_key is None
    assert area == 0.0
    assert confidence == 0.0


def test_process_estimator_failure_returns_zeros():
    proc, minio, estimator = make_processor()
    estimator.compute_pothole_area.side_effect = Exception("homography failed")
    bev_key, bev_mask_json, area, confidence = proc.process(
        "evt-3", "raw_images/evt-3.jpg", [[10.0, 10.0]]
    )
    assert bev_key is None
    assert area == 0.0
    assert confidence == 0.0
```

- [ ] **Step 5: Run test to verify it fails (module not importable yet)**

```bash
cd cloud/bev_surface_service
python -m pytest test_bev_processor.py -v
```

Expected: `ModuleNotFoundError: No module named 'bev_processor'` or import errors.

- [ ] **Step 6: Verify tests pass after Step 3 is done**

```bash
cd cloud/bev_surface_service
python -m pytest test_bev_processor.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 7: Write `cloud/bev_surface_service/bev_surface_service.py`**

```python
"""
BEV Surface Area Service

Consumes pothole.raw.events.v2, runs BEV transformation + pothole area estimation,
uploads BEV image to MinIO, publishes pothole.surface.area.v2.
"""

import json
import numpy as np
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from minio import Minio

from config import ConfigLoader
from bev_processor import BEVProcessor
from pothole_area_estimator import PotholeAreaEstimator


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


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[DELIVERED] {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def main():
    print("=" * 70)
    print("BEV SURFACE AREA SERVICE")
    print("=" * 70)

    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    config = ConfigLoader(config_path)

    # Initialize PotholeAreaEstimator
    trapezoid = np.array(config.trapezoid_coords, dtype=np.float32)
    rectangle = np.array(config.rectangle_coords, dtype=np.float32)
    estimator = PotholeAreaEstimator(
        trapezoid_coords=trapezoid,
        rectangle_coords=rectangle,
        calibration_path=config.calibration_file,
    )

    # MinIO client
    minio_client = Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=config.minio_secure,
    )

    processor = BEVProcessor(minio_client, config.minio_bucket, estimator)

    # Kafka setup
    schema_registry = SchemaRegistryClient({"url": config.schema_registry_url})

    deserializer = AvroDeserializer(schema_registry, RAW_EVENT_SCHEMA_STR, lambda obj, ctx: obj)
    serializer = AvroSerializer(schema_registry, SURFACE_AREA_SCHEMA_STR, lambda obj, ctx: obj)

    consumer = Consumer({
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": config.kafka_consumer_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": config.kafka_bootstrap_servers})

    source_topic = config.kafka_source_topic
    output_topic = config.kafka_output_topic
    consumer.subscribe([source_topic])

    print(f"[INFO] Consuming from: {source_topic}")
    print(f"[INFO] Producing to: {output_topic}")
    print("[INFO] Press Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            try:
                raw_event = deserializer(
                    msg.value(), SerializationContext(source_topic, MessageField.VALUE)
                )
                if raw_event is None:
                    consumer.commit(message=msg)
                    continue

                event_id = raw_event["event_id"]
                raw_key = raw_event["raw_image_object_key"]
                mask = raw_event["original_mask"]
                count += 1

                print(f"\n[RECEIVED #{count}] event_id={event_id}")

                bev_key, bev_mask_json, area_cm2, confidence = processor.process(
                    event_id, raw_key, mask
                )

                # On MinIO failure (None bev_key means download/upload error), send to DLQ
                if bev_key is None and confidence == 0.0 and area_cm2 == 0.0:
                    # Estimator failed gracefully — still publish with confidence=0
                    bev_key = ""
                    bev_mask_json = "[]"

                processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                output = {
                    "event_id": event_id,
                    "raw_image_object_key": raw_key,
                    "bev_object_key": bev_key or "",
                    "bev_mask": bev_mask_json or "[]",
                    "surface_area_cm2": area_cm2,
                    "confidence": confidence,
                    "processed_at": processed_at,
                }

                serialized = serializer(
                    output, SerializationContext(output_topic, MessageField.VALUE)
                )
                producer.produce(
                    topic=output_topic,
                    key=event_id,
                    value=serialized,
                    on_delivery=delivery_report,
                )
                producer.poll(0)
                consumer.commit(message=msg)

                print(f"[PUBLISHED] event_id={event_id}, area={area_cm2:.1f}cm², confidence={confidence}")

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                import traceback
                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
    finally:
        producer.flush()
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Write `cloud/bev_surface_service/config.yaml`**

```yaml
kafka:
  source_topic: "pothole.raw.events.v2"
  output_topic: "pothole.surface.area.v2"
  consumer_group_id: "bev-surface-service-group"
  bootstrap_servers: "localhost:19092,localhost:29092,localhost:39092"
  schema_registry_url: "http://localhost:8082"

minio:
  endpoint: "localhost:9000"
  access_key: "${MINIO_ACCESS_KEY:minioadmin}"
  secret_key: "${MINIO_SECRET_KEY:minioadmin}"
  bucket: "warehouse"
  secure: false

bev:
  # Normalized trapezoid coords from edge config (must match camera mount)
  trapezoid_coords:
    - [0.4034, 0.5731]
    - [0.5081, 0.5796]
    - [0.6456, 0.7972]
    - [0.3268, 0.7843]
  # BEV rectangle in cm (real-world calibration)
  rectangle_coords:
    - [0, 1000]
    - [250, 1000]
    - [250, 0]
    - [0, 0]
  calibration_file: ".conf/camera_calibration.json"
```

- [ ] **Step 9: Write `cloud/bev_surface_service/CLAUDE.md`**

```markdown
# BEV Surface Area Service

## Overview

Consumes `pothole.raw.events.v2` → downloads raw image from MinIO → runs `PotholeAreaEstimator` (homography BEV transform) → uploads BEV image to MinIO → publishes `pothole.surface.area.v2`.

## Entry Point

```bash
python cloud/bev_surface_service/bev_surface_service.py
```

## Key Gotchas

- `PotholeAreaEstimator` requires `camera_calibration.json` — path set in `config.yaml` under `bev.calibration_file`
- `bev_mask` in Avro schema is `"string"` (JSON-encoded `[[x,y],...]`) not bytes
- On estimator failure: publishes `confidence=0.0`, `surface_area_cm2=0.0`, empty `bev_object_key` — depth service falls back to `raw_image_object_key`
- Manual Kafka offset commit after successful produce
```

- [ ] **Step 10: Commit**

```bash
git add cloud/bev_surface_service/
git commit -m "feat: add bev_surface_service - BEV transform + surface area estimation in cloud"
```

---

## Task 4: Update `depth_estimation_model`

**Files:**
- Modify: `cloud/depth_estimation_model/cloud_pipeline.py`
- Modify: `cloud/depth_estimation_model/config.yaml`
- Modify: `cloud/depth_estimation_model/config_loader.py`

- [ ] **Step 1: Replace `RAW_EVENT_SCHEMA_STR` with `SURFACE_AREA_SCHEMA_STR` in `cloud_pipeline.py`**

Replace the `RAW_EVENT_SCHEMA_STR` constant (lines 36–56) with:

```python
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
```

- [ ] **Step 2: Extend `DEPTH_ESTIMATE_SCHEMA_STR` in `cloud_pipeline.py` to include `surface_area_cm2`**

Replace the `DEPTH_ESTIMATE_SCHEMA_STR` constant (lines 58–70) with:

```python
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
```

- [ ] **Step 3: Update `create_deserializer()` in `cloud_pipeline.py`**

Replace the body of `create_deserializer()` (lines 300–309) — change `RAW_EVENT_SCHEMA_STR` to `SURFACE_AREA_SCHEMA_STR`:

```python
def create_deserializer(config: ConfigLoader):
    """Create Avro deserializer for surface area events."""
    schema_registry_conf = {"url": config.get_kafka_schema_registry_url()}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    return AvroDeserializer(
        schema_registry_client,
        SURFACE_AREA_SCHEMA_STR,
        lambda obj, ctx: obj,
    )
```

- [ ] **Step 4: Update the main processing loop in `cloud_pipeline.py`**

Replace the entire `try` block inside the `while True:` loop (lines 380–460) with:

```python
            try:
                surface_event = deserializer(
                    msg.value(), SerializationContext(source_topic, MessageField.VALUE)
                )

                if surface_event is None:
                    continue

                event_id = surface_event["event_id"]
                bev_key = surface_event["bev_object_key"]
                raw_key = surface_event["raw_image_object_key"]
                surface_area_cm2 = surface_event["surface_area_cm2"]
                bev_confidence = surface_event.get("confidence")
                message_count += 1

                print(f"\n[RECEIVED #{message_count}] event_id={event_id}")
                print(f"[INFO] surface_area={surface_area_cm2:.2f}cm², bev_confidence={bev_confidence}")

                # Use BEV image; fall back to raw if BEV processing failed
                image_bytes = None
                if bev_key:
                    image_bytes = download_image_from_minio(minio_client, bev_key, config.get_minio_bucket())

                if image_bytes is None:
                    print(f"[INFO] BEV not available, falling back to raw image: {raw_key}")
                    image_bytes = download_image_from_minio(minio_client, raw_key, config.get_minio_bucket())

                if image_bytes is None:
                    print(f"[ERROR] Could not download any image for event {event_id}")
                    continue

                print(f"[INFO] Image downloaded, size: {len(image_bytes)} bytes")

                start_time = time.time()
                depth_cm, confidence = depth_estimator.estimate_depth(image_bytes)
                elapsed_time = time.time() - start_time

                print(f"[ESTIMATED] depth={depth_cm}cm, confidence={confidence:.4f} (took {elapsed_time:.3f}s)")

                processed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                depth_estimate = {
                    "event_id": event_id,
                    "depth_cm": depth_cm,
                    "confidence": confidence,
                    "surface_area_cm2": surface_area_cm2,
                    "processed_at": processed_at,
                }

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
```

- [ ] **Step 5: Update `cloud/depth_estimation_model/config.yaml`**

Change `source_topic` and remove `use_bev_image`/`fallback_to_regular_image`:

```yaml
kafka:
  source_topic: "pothole.surface.area.v2"
  output_topic: "pothole.depth.v1"
  consumer_group_id: "depth-estimator-cloud-group"
  bootstrap_servers: "localhost:19092,localhost:29092,localhost:39092"
  schema_registry_url: "http://localhost:8082"
  max_retries: 5
  retry_delay: 2

minio:
  endpoint: "localhost:9000"
  access_key: "${MINIO_ACCESS_KEY:minioadmin}"
  secret_key: "${MINIO_SECRET_KEY:minioadmin}"
  bucket: "warehouse"
  secure: false
  max_retries: 5
  retry_delay: 2

depth_model:
  model_type: "vitl"
  model_path: "Depth-Anything-V2/depth_anything_v2_vitl.pth"
  input_size: 518
  device: "auto"
  model_configs:
    vits:
      encoder: "vits"
      features: 64
      out_channels: [48, 96, 192, 384]
    vitb:
      encoder: "vitb"
      features: 128
      out_channels: [96, 192, 384, 768]
    vitl:
      encoder: "vitl"
      features: 256
      out_channels: [256, 512, 1024, 1024]
    vitg:
      encoder: "vitg"
      features: 384
      out_channels: [1536, 1536, 1536, 1536]

processing:
  depth_min_cm: 1.0
  depth_max_cm: 50.0
  confidence_min: 0.75
  confidence_max: 0.98

logging:
  level: "INFO"
  show_progress: true
```

- [ ] **Step 6: Remove `get_use_bev_image()` and `get_fallback_to_regular_image()` from `cloud/depth_estimation_model/config_loader.py`**

Delete these two methods (lines 210–217):

```python
    def get_use_bev_image(self) -> bool:
        """Check if BEV image should be used"""
        return self.config["processing"].get("use_bev_image", True)

    def get_fallback_to_regular_image(self) -> bool:
        # TODO generate bev_image from original_image + homography
        """Check if fallback to regular image is enabled"""
        return self.config["processing"].get("fallback_to_regular_image", True)
```

- [ ] **Step 7: Commit**

```bash
git add cloud/depth_estimation_model/
git commit -m "feat(depth): consume surface.area.v2, pass through surface_area_cm2 in depth.v1"
```

---

## Task 5: Simplify `severity_calculation_service`

**Files:**
- Modify: `cloud/severity_calculation_service/severity_aggregator.py`
- Modify: `cloud/severity_calculation_service/config.yaml`
- Modify: `cloud/severity_calculation_service/config_loader.py`

- [ ] **Step 1: Update `DEPTH_ESTIMATE_SCHEMA_STR` in `severity_aggregator.py` to include `surface_area_cm2`**

Replace the `DEPTH_ESTIMATE_SCHEMA_STR` constant (lines 55–67) with:

```python
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
```

- [ ] **Step 2: Remove `RAW_EVENT_SCHEMA_STR` from `severity_aggregator.py`**

Delete the entire `RAW_EVENT_SCHEMA_STR` constant (lines 69–90) and the `SEVERITY_SCORE_SCHEMA_STR` is unchanged.

- [ ] **Step 3: Remove `AggregationStore` class from `severity_aggregator.py`**

Delete the entire `AggregationStore` class (lines 237–313).

- [ ] **Step 4: Replace `create_consumers()` and `create_deserializers()` in `severity_aggregator.py` with single-topic versions**

Replace `create_consumers()` (lines 318–332) with:

```python
def create_consumer():
    """Create Kafka consumer for depth topic."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([DEPTH_TOPIC])
    print(f"[SUCCESS] Subscribed to {DEPTH_TOPIC}")
    return consumer
```

Replace `create_deserializers()` (lines 344–361) with:

```python
def create_deserializer():
    """Create Avro deserializer for depth estimates."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    return AvroDeserializer(
        schema_registry_client,
        DEPTH_ESTIMATE_SCHEMA_STR,
        lambda obj, ctx: obj,
    )
```

- [ ] **Step 5: Rewrite `main()` in `severity_aggregator.py` — single-topic loop, no aggregation**

Replace the entire `main()` function (lines 386–535) with:

```python
def main():
    print("=" * 70)
    print("SEVERITY SCORE AGGREGATOR")
    print("=" * 70)

    consumer = create_consumer()
    producer = create_producer()
    depth_deserializer = create_deserializer()
    severity_serializer = create_serializer()

    print(f"\n[INFO] Consuming from: {DEPTH_TOPIC}")
    print(f"[INFO] Producing to: {OUTPUT_TOPIC}")
    print("[INFO] Press Ctrl+C to stop.\n")

    processed_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            try:
                record = depth_deserializer(
                    msg.value(),
                    SerializationContext(DEPTH_TOPIC, MessageField.VALUE),
                )
                if not record:
                    continue

                event_id = record["event_id"]
                depth_cm = record["depth_cm"]
                surface_area_cm2 = record["surface_area_cm2"]
                processed_count += 1

                severity_score = calculate_severity_score(depth_cm, surface_area_cm2)
                severity_level = get_severity_level(severity_score)
                calculated_at = int(datetime.now(timezone.utc).timestamp() * 1000)

                print(f"\n[SEVERITY #{processed_count}] event_id={event_id}")
                print(f"  → depth={depth_cm}cm, area={surface_area_cm2}cm²")
                print(f"  → score={severity_score}, level={severity_level}\n")

                severity_record = {
                    "event_id": event_id,
                    "depth_cm": depth_cm,
                    "surface_area_cm2": surface_area_cm2,
                    "severity_score": severity_score,
                    "severity_level": severity_level,
                    "calculated_at": calculated_at,
                }

                serialized_value = severity_serializer(
                    severity_record,
                    SerializationContext(OUTPUT_TOPIC, MessageField.VALUE),
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
        print(f"\n[STATS] Processed: {processed_count} severity scores")
        producer.flush()
        consumer.close()
        print("[INFO] Shutdown complete.")
```

- [ ] **Step 6: Update top-level config variables in `severity_aggregator.py`**

The module-level config loading (lines 28–50) reads `config.raw_events_topic` — remove that line. The updated block should be:

```python
config = ConfigLoader()

DEPTH_TOPIC = config.depth_topic
OUTPUT_TOPIC = config.output_topic
CONSUMER_GROUP_ID = config.kafka_group_id
BOOTSTRAP_SERVERS = config.kafka_bootstrap_servers
SCHEMA_REGISTRY_URL = config.schema_registry_url

AREA_WEIGHT = config.area_weight
DEPTH_WEIGHT = config.depth_weight
MIN_SCORE = config.min_score
MAX_SCORE = config.max_score
THRESHOLDS = config.thresholds

LOG_TIMEOUT_WARNINGS = config.log_timeout_warnings
```

- [ ] **Step 7: Remove unused imports from `severity_aggregator.py`**

Remove `threading` and `defaultdict` imports (no longer needed — `AggregationStore` is gone). Also remove the now-unused `AGGREGATION_TIMEOUT_SECONDS` and `CLEANUP_INTERVAL_SECONDS` variables.

- [ ] **Step 8: Update `cloud/severity_calculation_service/config.yaml`**

Remove `raw_events_topic` and the `aggregation` section:

```yaml
kafka:
  bootstrap_servers: "localhost:19092,localhost:29092,localhost:39092"
  schema_registry_url: "http://localhost:8082"
  consumer_group_id: "severity-aggregator-group"
  depth_topic: "pothole.depth.v1"
  output_topic: "pothole.severity.score.v1"
  max_retries: 5
  retry_delay: 2

severity:
  area_weight: 0.6
  depth_weight: 0.4
  min_score: 1
  max_score: 10
  thresholds:
    minor_max: 3.25
    moderate_max: 5.5
    high_max: 7.75

logging:
  level: "INFO"
  log_timeout_warnings: true
```

- [ ] **Step 9: Update `_validate_config()` in `config_loader.py` — remove `raw_events_topic` requirement**

Replace the `kafka_fields` list in `_validate_config()` (line 92) with:

```python
kafka_fields = [
    "bootstrap_servers",
    "schema_registry_url",
    "consumer_group_id",
    "depth_topic",
    "output_topic",
]
```

Also delete the `aggregation` validation block (lines 103–107) and remove the `aggregation` from `required_fields` (line 81):

```python
required_fields = [
    "kafka",
    "severity",
]
```

- [ ] **Step 10: Remove `raw_events_topic` and aggregation properties from `config_loader.py`**

Delete these property methods:
- `raw_events_topic` property (lines 133–135)
- `aggregation_timeout_seconds` property (lines 141–143)
- `cleanup_interval_seconds` property (lines 145–147)

- [ ] **Step 11: Commit**

```bash
git add cloud/severity_calculation_service/
git commit -m "feat(severity): simplify to single-topic consumer on depth.v1, remove AggregationStore join"
```

---

## Task 6: Update `final_enrichment_service` field names

**Files:**
- Modify: `cloud/final_enrichment_service/final_enrichment_service.py`

- [ ] **Step 1: Update `RAW_EVENT_AVRO_SCHEMA` in `final_enrichment_service.py`**

Replace `RAW_EVENT_AVRO_SCHEMA` (lines 93–113) with:

```python
RAW_EVENT_AVRO_SCHEMA = """
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
```

- [ ] **Step 2: Update the `potholes` DDL in `_initialize_tables()` — rename and remove image columns**

In the `potholes_ddl` string (around line 534), replace:
```sql
            raw_image_path VARCHAR COMMENT 'S3 path to raw perspective image (from most recent detection)',
            bev_image_path VARCHAR COMMENT 'S3 path to birds-eye view image (from most recent detection)',
```
with:
```sql
            raw_image_object_key VARCHAR COMMENT 'MinIO object key for raw image (from most recent detection)',
```

- [ ] **Step 3: Update field extraction in `enrich_and_upsert()` — lines ~793–794**

Replace:
```python
        raw_image_path = escape_sql_string(raw_event.get('raw_image_path')) if raw_event.get('raw_image_path') else None
        bev_image_path = escape_sql_string(raw_event.get('bev_image_path')) if raw_event.get('bev_image_path') else None
```
with:
```python
        raw_image_object_key = escape_sql_string(raw_event.get('raw_image_object_key')) if raw_event.get('raw_image_object_key') else None
```

- [ ] **Step 4: Update INSERT and UPDATE SQL in `enrich_and_upsert()` — replace all `raw_image_path`/`bev_image_path` occurrences**

In the INSERT SQL (around line 816–839), replace the two lines referencing `raw_image_path` and `bev_image_path` in the column list and VALUES:

Column list: replace `raw_image_path, bev_image_path,` with `raw_image_object_key,`

Values: replace:
```python
                    {f"'{raw_image_path}'" if raw_image_path else 'NULL'},
                    {f"'{bev_image_path}'" if bev_image_path else 'NULL'},
```
with:
```python
                    {f"'{raw_image_object_key}'" if raw_image_object_key else 'NULL'},
```

In the UPDATE SQL (around line 863–864), replace:
```python
                    raw_image_path = {f"'{raw_image_path}'" if raw_image_path else 'raw_image_path'},
                    bev_image_path = {f"'{bev_image_path}'" if bev_image_path else 'bev_image_path'},
```
with:
```python
                    raw_image_object_key = {f"'{raw_image_object_key}'" if raw_image_object_key else 'raw_image_object_key'},
```

- [ ] **Step 5: Update `RAW_EVENTS_TOPIC` reference in config**

Check `final_enrichment_service`'s `config_loader.py` or `config.yaml` for `raw_events_topic` — update to `pothole.raw.events.v2` if it still references `v1`.

```bash
grep -n "raw.events.v1" cloud/final_enrichment_service/config.yaml
```

Update the value to `"pothole.raw.events.v2"` if found.

- [ ] **Step 6: Commit**

```bash
git add cloud/final_enrichment_service/
git commit -m "feat(enrichment): update raw event schema to v2, rename raw_image_path to raw_image_object_key"
```

---

## Task 7: Update `etl_service`

**Files:**
- Modify: `cloud/etl_service/etl_microservice.py`

- [ ] **Step 1: Replace `RAW_EVENT_AVRO_SCHEMA` with v2 schema**

Replace `RAW_EVENT_AVRO_SCHEMA` (lines 61–81) with:

```python
RAW_EVENT_AVRO_SCHEMA = """
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
```

- [ ] **Step 2: Add `SURFACE_AREA_AVRO_SCHEMA` constant after `SEVERITY_SCORE_AVRO_SCHEMA`**

```python
SURFACE_AREA_AVRO_SCHEMA = """
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
```

- [ ] **Step 3: Replace `RAW_EVENTS_ARROW_SCHEMA` with v2 schema (lines 104–118)**

```python
RAW_EVENTS_ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("vehicle_id", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
    pa.field("gps_lat", pa.float64(), nullable=False),
    pa.field("gps_lon", pa.float64(), nullable=False),
    pa.field("gps_accuracy", pa.float64(), nullable=True),
    pa.field("raw_image_object_key", pa.string(), nullable=False),
    pa.field("original_mask", pa.list_(pa.list_(pa.float64())), nullable=False),
    pa.field("detection_confidence", pa.float64(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us"), nullable=False),
])
```

- [ ] **Step 4: Add `SURFACE_AREA_ARROW_SCHEMA` after the existing PyArrow schemas**

```python
SURFACE_AREA_ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("raw_image_object_key", pa.string(), nullable=False),
    pa.field("bev_object_key", pa.string(), nullable=False),
    pa.field("bev_mask", pa.string(), nullable=False),
    pa.field("surface_area_cm2", pa.float64(), nullable=False),
    pa.field("confidence", pa.float64(), nullable=True),
    pa.field("processed_at", pa.timestamp("us"), nullable=False),
    pa.field("ingested_at", pa.timestamp("us"), nullable=False),
])
```

- [ ] **Step 5: Replace `CREATE_RAW_EVENTS_SQL` with v2 DDL**

Replace `CREATE_RAW_EVENTS_SQL` (lines 134–157) with:

```python
CREATE_RAW_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.city.raw_events (
    event_id VARCHAR NOT NULL COMMENT 'Unique event identifier',
    vehicle_id VARCHAR NOT NULL COMMENT 'Vehicle identifier',
    created_at TIMESTAMP(3) NOT NULL COMMENT 'Event timestamp from device',

    gps_lat DOUBLE NOT NULL COMMENT 'Latitude',
    gps_lon DOUBLE NOT NULL COMMENT 'Longitude',
    gps_accuracy DOUBLE COMMENT 'GPS accuracy in meters',

    raw_image_object_key VARCHAR NOT NULL COMMENT 'MinIO object key (raw_images/{event_id}.jpg)',
    original_mask ARRAY(ARRAY(DOUBLE)) NOT NULL COMMENT 'Polygon mask from edge detection [[x,y], ...]',
    detection_confidence DOUBLE COMMENT 'Edge model confidence score',

    ingested_at TIMESTAMP(3) NOT NULL COMMENT 'When ingested into Iceberg'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
)
"""
```

- [ ] **Step 6: Add `CREATE_SURFACE_AREA_SQL` after `CREATE_SEVERITY_SCORES_SQL`**

```python
CREATE_SURFACE_AREA_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.city.surface_area_events (
    event_id VARCHAR NOT NULL COMMENT 'Unique event identifier',
    raw_image_object_key VARCHAR NOT NULL COMMENT 'MinIO key for raw image',
    bev_object_key VARCHAR NOT NULL COMMENT 'MinIO key for BEV image (bev_images/{event_id}.jpg)',
    bev_mask VARCHAR NOT NULL COMMENT 'JSON-encoded BEV mask polygon [[x,y], ...]',
    surface_area_cm2 DOUBLE NOT NULL COMMENT 'Estimated pothole surface area',
    confidence DOUBLE COMMENT 'Estimation confidence (0.0=failed, 1.0=success)',
    processed_at TIMESTAMP(3) NOT NULL COMMENT 'When BEV transform was computed'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(processed_at)']
)
"""
```

- [ ] **Step 7: Update `transform_raw_event()` function**

Replace `transform_raw_event()` (lines 194–215) with:

```python
def transform_raw_event(avro_record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw event from Kafka v2 to Iceberg schema."""
    now = datetime.now(timezone.utc)
    created_at = _convert_timestamp(avro_record['timestamp'])
    return {
        "event_id": avro_record['event_id'],
        "vehicle_id": avro_record['vehicle_id'],
        "created_at": created_at,
        "gps_lat": avro_record['gps_lat'],
        "gps_lon": avro_record['gps_lon'],
        "gps_accuracy": avro_record.get('gps_accuracy'),
        "raw_image_object_key": avro_record['raw_image_object_key'],
        "original_mask": avro_record['original_mask'],
        "detection_confidence": avro_record.get('detection_confidence'),
        "ingested_at": now.replace(tzinfo=None),
    }
```

- [ ] **Step 8: Add `transform_surface_area()` function after `transform_severity_score()`**

```python
def transform_surface_area(avro_record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform surface area event from Kafka to Iceberg schema."""
    now = datetime.now(timezone.utc)
    processed_at = _convert_timestamp(avro_record['processed_at'])
    return {
        "event_id": avro_record['event_id'],
        "raw_image_object_key": avro_record['raw_image_object_key'],
        "bev_object_key": avro_record['bev_object_key'],
        "bev_mask": avro_record['bev_mask'],
        "surface_area_cm2": avro_record['surface_area_cm2'],
        "confidence": avro_record.get('confidence'),
        "processed_at": processed_at,
        "ingested_at": now.replace(tzinfo=None),
    }
```

- [ ] **Step 9: Update `TOPIC_TABLE_MAPPINGS` — fix topic name and add surface area mapping**

Replace the `TOPIC_TABLE_MAPPINGS` list (lines 252–268) with:

```python
TOPIC_TABLE_MAPPINGS: List[TopicTableMapping] = [
    TopicTableMapping(
        kafka_topic="pothole.raw.events.v2",
        iceberg_table="raw_events",
        avro_schema=RAW_EVENT_AVRO_SCHEMA,
        arrow_schema=RAW_EVENTS_ARROW_SCHEMA,
        create_table_sql=CREATE_RAW_EVENTS_SQL,
        transform_fn=transform_raw_event,
    ),
    TopicTableMapping(
        kafka_topic="pothole.surface.area.v2",
        iceberg_table="surface_area_events",
        avro_schema=SURFACE_AREA_AVRO_SCHEMA,
        arrow_schema=SURFACE_AREA_ARROW_SCHEMA,
        create_table_sql=CREATE_SURFACE_AREA_SQL,
        transform_fn=transform_surface_area,
    ),
    TopicTableMapping(
        kafka_topic="pothole.severity.score.v1",
        iceberg_table="severity_scores",
        avro_schema=SEVERITY_SCORE_AVRO_SCHEMA,
        arrow_schema=SEVERITY_SCORES_ARROW_SCHEMA,
        create_table_sql=CREATE_SEVERITY_SCORES_SQL,
        transform_fn=transform_severity_score,
    ),
]
```

- [ ] **Step 10: Commit**

```bash
git add cloud/etl_service/etl_microservice.py
git commit -m "feat(etl): add surface_area_events table, update raw_events schema to v2"
```

---

## Task 8: Update cloud CLAUDE.md + documentation

**Files:**
- Modify: `cloud/CLAUDE.md`

- [ ] **Step 1: Update topic flow diagram in `cloud/CLAUDE.md`**

Replace the `## Topic Flow` section with:

```markdown
## Topic Flow

```
pothole.raw.events.v2  ──┬──> ETL (→ iceberg.city.raw_events)
                         └──> BEV Surface Service (→ pothole.surface.area.v2)

pothole.surface.area.v2 ─┬──> ETL (→ iceberg.city.surface_area_events)
                          └──> Depth Estimation (→ pothole.depth.v1)

pothole.depth.v1  ──────────> Severity Calculator (→ pothole.severity.score.v1)

pothole.severity.score.v1 ─┬──> ETL (→ iceberg.city.severity_scores)
                            └──> Final Enrichment

pothole.raw.events.v2 ─────> Final Enrichment (aggregates with severity)
```

Also add `BEV Surface` row to the services table:

```markdown
| BEV Surface | `bev_surface_service/bev_surface_service.py` | BEV transform + pothole area |
```

- [ ] **Step 2: Commit**

```bash
git add cloud/CLAUDE.md
git commit -m "docs(cloud): update CLAUDE.md for new BEV surface service and v2 topic flow"
```

---

## Verification Checklist

After all tasks are complete, verify end-to-end:

- [ ] `python cloud/bev_surface_service/bev_surface_service.py` starts without import errors
- [ ] Edge publishes to `pothole.raw.events.v2` (check Schema Registry has new schema)
- [ ] BEV service consumes `raw.events.v2`, produces to `surface.area.v2`
- [ ] Depth service consumes `surface.area.v2`, produces to `depth.v1` with `surface_area_cm2` field
- [ ] Severity service consumes `depth.v1` alone, no join, produces `severity.score.v1`
- [ ] Final enrichment consumes `raw.events.v2` + `severity.score.v1`, UPSERT to Iceberg succeeds
- [ ] ETL ingests all 3 topics into Iceberg tables (verify `surface_area_events` table created)
- [ ] `python -m pytest cloud/bev_surface_service/test_bev_processor.py -v` — 3 tests PASS
