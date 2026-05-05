# Cloud Failure Handling — DLQs and Quality Flags Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire DLQ publishing to all four cloud services on failure, propagate quality_flags through bronze/silver/gold Flink jobs, and add a `needs_review` flag to PostGIS with auto-trigger.

**Architecture:** A shared `DlqProducer` utility in `cloud/shared/` handles Kafka DLQ publishing. Each service catches its specific failure modes and publishes to the appropriate DLQ topic with a failure-reason header. Flink SQL jobs carry `quality_flags_json` through all medallion layers. PostGIS `current_road_defects` gains `needs_review` boolean populated by trigger when `quality_flags` is non-empty or confidence is below 0.7.

**Dependencies:** Avro v3 schema must be registered (Plan 1 complete) before quality_flags propagation in Flink is meaningful. DLQ topic creation is independent (the DLQ topics already exist from kafka-init).

**Tech Stack:** Python (confluent-kafka), Flink SQL, PostgreSQL trigger

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `cloud/shared/dlq_producer.py` | Shared DLQ Kafka producer with failure-reason header |
| Create | `tests/unit/test_dlq_producer.py` | Unit tests for DLQ producer |
| Modify | `cloud/bev_surface_service/bev_surface_service.py` | Publish to DLQ on BEV failure |
| Modify | `cloud/depth_estimation_model/cloud_pipeline.py` | Publish to DLQ on Triton failure |
| Modify | `cloud/final_enrichment_service/final_enrichment_service.py` | Publish to DLQ on aggregation timeout |
| Modify | `cloud/etl_service/etl_microservice.py` | Publish to DLQ on Iceberg write failure |
| Modify | `lakehouse/flink/sql/020_silver_materialization.sql` | Carry quality_flags through to silver |
| Modify | `lakehouse/flink/sql/030_gold_materialization.sql` | Aggregate quality_flags + set needs_review |
| Modify | `lakehouse/flink/sql/040_gold_to_postgis_projection.sql` | Pass needs_review to JDBC sink |
| Modify | `lakehouse/iceberg/020_silver_tables.sql` | quality_flags_json already present (verify) |
| Modify | `lakehouse/iceberg/030_gold_tables.sql` | Add needs_review BOOLEAN |
| Modify | `lakehouse/postgis/001_serving_schema.sql` | Add needs_review column + auto-review trigger |

---

### Task 1: Shared DLQ Producer Utility

**Files:**
- Create: `cloud/shared/dlq_producer.py`
- Create: `tests/unit/test_dlq_producer.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_dlq_producer.py
from unittest.mock import MagicMock, patch, call
import pytest

def test_dlq_producer_publish_calls_produce_with_headers():
    mock_producer = MagicMock()
    with patch("cloud.shared.dlq_producer.Producer", return_value=mock_producer):
        from cloud.shared.dlq_producer import DlqProducer
        dlq = DlqProducer(bootstrap_servers="localhost:9092")
        dlq.publish(
            dlq_topic="pothole.raw.events.dlq.v1",
            original_value=b"some-avro-bytes",
            original_key=b"event-123",
            failure_reason="BEV_FAILURE",
        )
    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args[1]
    assert call_kwargs["topic"] == "pothole.raw.events.dlq.v1"
    assert call_kwargs["value"] == b"some-avro-bytes"
    assert call_kwargs["key"] == b"event-123"
    headers = dict(call_kwargs["headers"])
    assert headers[b"failure-reason"] == b"BEV_FAILURE"

def test_dlq_producer_swallows_produce_exception(caplog):
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = Exception("broker unavailable")
    with patch("cloud.shared.dlq_producer.Producer", return_value=mock_producer):
        from cloud.shared.dlq_producer import DlqProducer
        dlq = DlqProducer(bootstrap_servers="localhost:9092")
        # Must not raise — DLQ failure should never crash the main service
        dlq.publish("pothole.raw.events.dlq.v1", b"val", b"key", "TEST_FAILURE")
```

- [ ] **Step 2: Run test — expect failure**

```
pytest tests/unit/test_dlq_producer.py -v
# Expected: ImportError
```

- [ ] **Step 3: Implement `cloud/shared/dlq_producer.py`**

```python
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class DlqProducer:
    """Publishes failed messages to a DLQ topic with a failure-reason header.
    Never raises — DLQ failure must not crash the calling service.
    """

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def publish(
        self,
        dlq_topic: str,
        original_value: bytes,
        original_key: bytes | None,
        failure_reason: str,
    ) -> None:
        try:
            self._producer.produce(
                topic=dlq_topic,
                value=original_value,
                key=original_key,
                headers={b"failure-reason": failure_reason.encode()},
            )
            self._producer.poll(0)
        except Exception as exc:
            logger.warning("DLQ publish failed (non-fatal): %s", exc)
```

- [ ] **Step 4: Run tests — expect pass**

```
pytest tests/unit/test_dlq_producer.py -v
# Expected: 2 passed
```

- [ ] **Step 5: Commit**

```bash
git add cloud/shared/dlq_producer.py tests/unit/test_dlq_producer.py
git commit -m "feat(cloud): add shared DlqProducer utility"
```

---

### Task 2: BEV Service DLQ on Failure

**Files:**
- Modify: `cloud/bev_surface_service/bev_surface_service.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_bev_dlq.py
from unittest.mock import MagicMock, patch

def test_bev_service_publishes_to_dlq_on_bev_failure():
    """BEV failure still produces zero-conf surface area event AND publishes raw to DLQ."""
    from cloud.bev_surface_service.bev_surface_service import BevSurfaceService

    mock_dlq = MagicMock()
    service = BevSurfaceService.__new__(BevSurfaceService)
    service._dlq_producer = mock_dlq
    service._produce_surface_area_event = MagicMock()

    raw_msg_bytes = b"raw-avro-payload"
    raw_event = {
        "event_id": "evt-001",
        "raw_image_object_key": "s3://warehouse/raw_images/evt-001.jpg",
        "original_mask": [[0.1, 0.2]],
        "vehicle_id": "vehicle-abc",
        "device_id": None,
        "timestamp": 1000000,
        "gps_lat": 10.8,
        "gps_lon": 106.7,
        "gps_accuracy": None,
        "detection_confidence": 0.9,
        "model_id": None,
        "quality_flags": [],
    }

    # Simulate BEV processing failure path
    service._handle_bev_failure(raw_event, raw_msg_bytes)

    mock_dlq.publish.assert_called_once_with(
        dlq_topic="pothole.raw.events.v3.dlq.v1",
        original_value=raw_msg_bytes,
        original_key=b"evt-001",
        failure_reason="BEV_FAILURE",
    )
    service._produce_surface_area_event.assert_called_once()
    call_args = service._produce_surface_area_event.call_args[0][0]
    assert call_args["confidence"] == 0.0
    assert call_args["surface_area_cm2"] == 0.0
    assert call_args["bev_object_key"] == ""
```

- [ ] **Step 2: Run test — expect failure**

```
pytest tests/unit/test_bev_dlq.py -v
# Expected: AttributeError — _handle_bev_failure not defined
```

- [ ] **Step 3: Add DLQ initialization and `_handle_bev_failure` to BEV service**

In `BevSurfaceService.__init__()`:

```python
from cloud.shared.dlq_producer import DlqProducer

# after kafka producer setup:
self._dlq_producer = DlqProducer(
    bootstrap_servers=config["kafka"]["bootstrap_servers"]
)
self._raw_dlq_topic = config["kafka"].get(
    "raw_dlq_topic", "pothole.raw.events.v3.dlq.v1"
)
```

Add method:

```python
def _handle_bev_failure(self, raw_event: dict, raw_msg_bytes: bytes) -> None:
    """On BEV failure: publish raw event to DLQ, still emit zero-conf surface event."""
    self._dlq_producer.publish(
        dlq_topic=self._raw_dlq_topic,
        original_value=raw_msg_bytes,
        original_key=raw_event["event_id"].encode(),
        failure_reason="BEV_FAILURE",
    )
    self._produce_surface_area_event({
        "event_id": raw_event["event_id"],
        "raw_image_object_key": raw_event["raw_image_object_key"],
        "bev_object_key": "",
        "bev_mask": "[]",
        "surface_area_cm2": 0.0,
        "confidence": 0.0,
        "processed_at": int(time.time() * 1000),
    })
```

In the main consumer loop, replace the existing BEV failure path (wherever `confidence=0.0` is set) with a call to `self._handle_bev_failure(raw_event, msg.value())`.

- [ ] **Step 4: Run tests — expect pass**

```
pytest tests/unit/test_bev_dlq.py -v
# Expected: 1 passed
```

- [ ] **Step 5: Commit**

```bash
git add cloud/bev_surface_service/ tests/unit/test_bev_dlq.py
git commit -m "feat(bev): publish to DLQ on BEV processing failure"
```

---

### Task 3: Depth Service DLQ on Triton Failure

**Files:**
- Modify: `cloud/depth_estimation_model/cloud_pipeline.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_depth_dlq.py
from unittest.mock import MagicMock

def test_depth_service_publishes_to_dlq_on_triton_failure():
    from cloud.depth_estimation_model.cloud_pipeline import DepthEstimationService

    mock_dlq = MagicMock()
    service = DepthEstimationService.__new__(DepthEstimationService)
    service._dlq_producer = mock_dlq
    service._depth_dlq_topic = "pothole.depth.dlq.v1"
    service._produce_depth_event = MagicMock()

    surface_event = {
        "event_id": "evt-002",
        "bev_object_key": "bev_images/evt-002.jpg",
        "raw_image_object_key": "raw_images/evt-002.jpg",
        "surface_area_cm2": 500.0,
    }
    msg_bytes = b"surface-avro"

    service._handle_depth_failure(surface_event, msg_bytes, reason="TRITON_UNAVAILABLE")

    mock_dlq.publish.assert_called_once_with(
        dlq_topic="pothole.depth.dlq.v1",
        original_value=msg_bytes,
        original_key=b"evt-002",
        failure_reason="TRITON_UNAVAILABLE",
    )
    # Does NOT produce a depth event on failure (depth failure stalls pipeline — needs DLQ handling)
    service._produce_depth_event.assert_not_called()
```

- [ ] **Step 2: Run test — expect failure**

```
pytest tests/unit/test_depth_dlq.py -v
# Expected: AttributeError
```

- [ ] **Step 3: Add DLQ to Depth service**

In `DepthEstimationService.__init__()`:

```python
from cloud.shared.dlq_producer import DlqProducer

self._dlq_producer = DlqProducer(bootstrap_servers=config["kafka"]["bootstrap_servers"])
self._depth_dlq_topic = config["kafka"].get("depth_dlq_topic", "pothole.depth.dlq.v1")
```

Add method:

```python
def _handle_depth_failure(self, surface_event: dict, msg_bytes: bytes, reason: str = "DEPTH_FAILURE") -> None:
    self._dlq_producer.publish(
        dlq_topic=self._depth_dlq_topic,
        original_value=msg_bytes,
        original_key=surface_event["event_id"].encode(),
        failure_reason=reason,
    )
```

In `_process_batch()`, in the `except` block that currently logs the error, add:

```python
except Exception as exc:
    logger.error("Depth estimation failed for batch: %s", exc)
    for surface_event, msg_bytes in batch_items:
        self._handle_depth_failure(surface_event, msg_bytes, reason="TRITON_ERROR")
```

Also call `_handle_depth_failure` when Triton is unavailable (`wait_for_triton_ready()` raises after retries).

- [ ] **Step 4: Run tests — expect pass**

```
pytest tests/unit/test_depth_dlq.py -v
# Expected: 1 passed
```

- [ ] **Step 5: Commit**

```bash
git add cloud/depth_estimation_model/ tests/unit/test_depth_dlq.py
git commit -m "feat(depth): publish to DLQ on Triton failure"
```

---

### Task 4: Final Enrichment DLQ on Aggregation Timeout

**Files:**
- Modify: `cloud/final_enrichment_service/final_enrichment_service.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_enrichment_dlq.py
from unittest.mock import MagicMock

def test_enrichment_publishes_to_dlq_on_aggregation_timeout():
    from cloud.final_enrichment_service.final_enrichment_service import FinalEnrichmentService

    mock_dlq = MagicMock()
    service = FinalEnrichmentService.__new__(FinalEnrichmentService)
    service._dlq_producer = mock_dlq
    service._raw_dlq_topic = "pothole.raw.events.v3.dlq.v1"

    stale_event = {
        "event_id": "evt-003",
        "_raw_msg_bytes": b"raw-avro",
    }
    service._handle_stale_aggregation(stale_event)

    mock_dlq.publish.assert_called_once_with(
        dlq_topic="pothole.raw.events.v3.dlq.v1",
        original_value=b"raw-avro",
        original_key=b"evt-003",
        failure_reason="SEVERITY_TIMEOUT",
    )
```

- [ ] **Step 2: Run test — expect failure**

```
pytest tests/unit/test_enrichment_dlq.py -v
# Expected: AttributeError
```

- [ ] **Step 3: Add DLQ to Final Enrichment**

In `FinalEnrichmentService.__init__()`:

```python
from cloud.shared.dlq_producer import DlqProducer

self._dlq_producer = DlqProducer(bootstrap_servers=config["kafka"]["bootstrap_servers"])
self._raw_dlq_topic = config["kafka"].get("raw_dlq_topic", "pothole.raw.events.v3.dlq.v1")
```

In `EventAggregationStore`, store raw message bytes alongside the raw event. When a raw half arrives, store `raw_msg_bytes` keyed by `event_id`:

```python
# In store.add_raw(event_id, raw_event, raw_msg_bytes):
self._store[event_id]["raw"] = raw_event
self._store[event_id]["_raw_msg_bytes"] = raw_msg_bytes
```

Add method to service:

```python
def _handle_stale_aggregation(self, stale_entry: dict) -> None:
    self._dlq_producer.publish(
        dlq_topic=self._raw_dlq_topic,
        original_value=stale_entry.get("_raw_msg_bytes", b""),
        original_key=stale_entry["event_id"].encode(),
        failure_reason="SEVERITY_TIMEOUT",
    )
```

In `cleanup_stale()`, for each purged entry, call `self._handle_stale_aggregation(entry)` before deleting.

- [ ] **Step 4: Run tests — expect pass**

```
pytest tests/unit/test_enrichment_dlq.py -v
# Expected: 1 passed
```

- [ ] **Step 5: Commit**

```bash
git add cloud/final_enrichment_service/ tests/unit/test_enrichment_dlq.py
git commit -m "feat(enrichment): publish to DLQ on aggregation timeout"
```

---

### Task 5: ETL Service DLQ on Iceberg Write Failure

**Files:**
- Modify: `cloud/etl_service/etl_microservice.py`

- [ ] **Step 1: Add DLQ map to ETL service**

In `etl_microservice.py`, add a DLQ topic map alongside the existing `TopicTableMapping`:

```python
DLQ_TOPIC_MAP = {
    "pothole.raw.events.v3":          "pothole.raw.events.v3.dlq.v1",
    "pothole.surface.area.v2":        "pothole.surface.area.dlq.v1",
    "pothole.severity.score.v1":      "pothole.severity.score.dlq.v1",
}
```

In `MultiBatchProcessor.__init__()`, initialize DLQ producer:

```python
from cloud.shared.dlq_producer import DlqProducer

self._dlq_producer = DlqProducer(bootstrap_servers=config["kafka"]["bootstrap_servers"])
```

In `_flush_topic_buffer()` (or equivalent), in the except block that currently routes to DLQ (check if this already exists — ETL's `MultiBatchProcessor` has DLQ logic per the cloud context):

```python
except Exception as exc:
    logger.error("Iceberg write failed for %s: %s", topic, exc)
    dlq_topic = DLQ_TOPIC_MAP.get(topic)
    if dlq_topic:
        for msg in failed_messages:
            self._dlq_producer.publish(
                dlq_topic=dlq_topic,
                original_value=msg.value(),
                original_key=msg.key(),
                failure_reason="ICEBERG_WRITE_FAILURE",
            )
```

Note: the cloud context says "MultiBatchProcessor ... DLQ on failure" — check if a DLQ producer is already wired. If so, only add the `failure_reason` header update.

- [ ] **Step 2: Commit**

```bash
git add cloud/etl_service/etl_microservice.py
git commit -m "feat(etl): wire failure-reason headers into DLQ publishing"
```

---

### Task 6: Flink Silver — Carry quality_flags Through

**Files:**
- Modify: `lakehouse/flink/sql/020_silver_materialization.sql`

- [ ] **Step 1: Update `silver.detections` INSERT to carry quality_flags**

```sql
INSERT INTO silver.detections
SELECT
  event_id AS detection_id,
  event_id,
  device_id,
  vehicle_id,
  event_time,
  gps_lat,
  gps_lon,
  gps_accuracy_m,
  raw_image_object_key,
  original_mask_json,
  detection_confidence,
  CAST(NULL AS STRING) AS model_id,
  CAST(NULL AS STRING) AS calibration_id,
  CAST(NULL AS BIGINT) AS h3_cell,
  COALESCE(quality_flags_json, '[]') AS quality_flags_json,   -- carry from bronze
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events;
```

- [ ] **Step 2: Update `silver.observations` INSERT to carry quality_flags**

```sql
INSERT INTO silver.observations
SELECT
  r.event_id AS observation_id,
  CONCAT('defect-', r.event_id) AS defect_id,
  r.event_id,
  r.event_time AS observed_at,
  r.gps_lat,
  r.gps_lon,
  d.depth_cm,
  COALESCE(a.surface_area_cm2, d.surface_area_cm2) AS surface_area_cm2,
  sev.severity_score,
  sev.severity_level,
  'reported' AS status,
  r.event_id AS evidence_id,
  COALESCE(r.quality_flags_json, '[]') AS quality_flags_json,   -- carry from bronze
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events r
LEFT JOIN bronze.surface_area_events a ON r.event_id = a.event_id
LEFT JOIN bronze.depth_estimation_events d ON r.event_id = d.event_id
LEFT JOIN bronze.severity_score_events sev ON r.event_id = sev.event_id;
```

- [ ] **Step 3: Commit**

```bash
git add lakehouse/flink/sql/020_silver_materialization.sql
git commit -m "feat(lakehouse): carry quality_flags through silver materialization"
```

---

### Task 7: Flink Gold — Aggregate quality_flags + needs_review

**Files:**
- Modify: `lakehouse/flink/sql/030_gold_materialization.sql`
- Modify: `lakehouse/iceberg/030_gold_tables.sql`

- [ ] **Step 1: Add `needs_review` column to gold DDL**

In `lakehouse/iceberg/030_gold_tables.sql`, add `needs_review BOOLEAN` to `gold.current_road_defects`:

```sql
CREATE TABLE IF NOT EXISTS iceberg.gold.current_road_defects (
  defect_id VARCHAR,
  -- ... existing columns ...
  needs_review BOOLEAN,          -- true when quality_flags non-empty or confidence < 0.7
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(first_seen_at)']);
```

- [ ] **Step 2: Update `030_gold_materialization.sql` INSERT**

```sql
INSERT INTO gold.current_road_defects
SELECT
  o.defect_id,
  'POTHOLE' AS defect_type,
  MAX(o.status) AS status,
  MAX(o.severity_score) AS severity_score,
  MAX(o.severity_level) AS severity_level,
  MAX(d.detection_confidence) AS confidence,
  MAX(o.quality_flags_json) AS quality_flags_json,
  MAX(o.gps_lat) AS latitude,
  MAX(o.gps_lon) AS longitude,
  CONCAT('POINT (', CAST(MAX(o.gps_lon) AS STRING), ' ', CAST(MAX(o.gps_lat) AS STRING), ')') AS geometry_wkt,
  CAST(NULL AS BIGINT) AS h3_cell,
  CAST(NULL AS STRING) AS road_segment_id,
  CAST(NULL AS STRING) AS district,
  CAST(NULL AS STRING) AS ward,
  MIN(o.observed_at) AS first_seen_at,
  MAX(o.observed_at) AS last_seen_at,
  COUNT(*) AS observation_count,
  MAX(e.raw_image_object_key) AS latest_raw_image_object_key,
  MAX(e.bev_object_key) AS latest_bev_object_key,
  -- needs_review: true if any quality flag set OR max confidence below threshold
  (MAX(o.quality_flags_json) <> '[]' OR MAX(d.detection_confidence) < 0.7) AS needs_review,
  CURRENT_TIMESTAMP AS updated_at
FROM silver.observations o
LEFT JOIN silver.detections d ON o.event_id = d.event_id
LEFT JOIN silver.defect_evidence e ON o.evidence_id = e.evidence_id
GROUP BY o.defect_id;
```

- [ ] **Step 3: Commit**

```bash
git add lakehouse/flink/sql/030_gold_materialization.sql \
        lakehouse/iceberg/030_gold_tables.sql
git commit -m "feat(lakehouse): add needs_review to gold and aggregate quality_flags"
```

---

### Task 8: Flink 040 — needs_review in JDBC Sink

**Files:**
- Modify: `lakehouse/flink/sql/040_gold_to_postgis_projection.sql`

- [ ] **Step 1: Add needs_review to JDBC sink DDL and INSERT**

```sql
CREATE TEMPORARY TABLE serving_current_road_defects_jdbc (
  defect_id STRING,
  defect_type STRING,
  status STRING,
  severity_score DOUBLE,
  severity_level STRING,
  confidence DOUBLE,
  quality_flags STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  road_segment_id STRING,
  district STRING,
  ward STRING,
  first_seen_at TIMESTAMP(6),
  last_seen_at TIMESTAMP(6),
  observation_count INT,
  latest_raw_image_object_key STRING,
  latest_bev_object_key STRING,
  needs_review BOOLEAN,                    -- NEW
  updated_at TIMESTAMP(6),
  PRIMARY KEY (defect_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgis:5432/postgis_serving',
  'table-name' = 'serving.current_road_defects_projection_inbox',
  'username' = 'serving',
  'password' = 'servingpassword'
);

INSERT INTO serving_current_road_defects_jdbc
SELECT
  defect_id, defect_type, status, severity_score, severity_level,
  confidence, quality_flags_json, longitude, latitude,
  road_segment_id, district, ward,
  first_seen_at, last_seen_at, observation_count,
  latest_raw_image_object_key, latest_bev_object_key,
  needs_review,                            -- NEW
  updated_at
FROM gold.current_road_defects;
```

- [ ] **Step 2: Commit**

```bash
git add lakehouse/flink/sql/040_gold_to_postgis_projection.sql
git commit -m "feat(lakehouse): pass needs_review through JDBC projection sink"
```

---

### Task 9: PostGIS Schema — needs_review + Auto-Review Trigger

**Files:**
- Modify: `lakehouse/postgis/001_serving_schema.sql`

- [ ] **Step 1: Add needs_review to PostGIS tables**

In `001_serving_schema.sql`, add `needs_review` to both `current_road_defects` and `current_road_defects_projection_inbox`:

```sql
-- In current_road_defects table definition:
needs_review BOOLEAN NOT NULL DEFAULT false,

-- In current_road_defects_projection_inbox table definition:
needs_review BOOLEAN NOT NULL DEFAULT false,
```

Add index:
```sql
CREATE INDEX IF NOT EXISTS current_road_defects_needs_review_idx
  ON serving.current_road_defects (needs_review)
  WHERE needs_review = true;
```

- [ ] **Step 2: Update projection trigger to pass needs_review + auto-create review tasks**

In the `apply_current_road_defect_projection()` trigger function, add `needs_review` to both the INSERT and the ON CONFLICT UPDATE:

```sql
-- In INSERT values:
NEW.needs_review,

-- In ON CONFLICT DO UPDATE SET:
needs_review = EXCLUDED.needs_review,
```

Add auto-review-task creation at the end of the trigger function, before `RETURN NEW`:

```sql
-- Auto-create review task when needs_review becomes true
IF NEW.needs_review AND (TG_OP = 'INSERT' OR NOT OLD.needs_review) THEN
  INSERT INTO serving.review_tasks (
    review_task_id, defect_id, status, priority, created_at, updated_at
  ) VALUES (
    gen_random_uuid()::text,
    NEW.defect_id,
    'pending',
    CASE
      WHEN NEW.severity_level = 'CRITICAL' THEN 'high'
      WHEN NEW.severity_level = 'HIGH' THEN 'medium'
      ELSE 'low'
    END,
    now(), now()
  )
  ON CONFLICT DO NOTHING;
END IF;
```

- [ ] **Step 3: Verify SQL is idempotent (test against running PostGIS)**

```bash
docker compose exec postgis-serving psql -U serving -d postgis_serving \
  -f /docker-entrypoint-initdb.d/001_serving_schema.sql
# Expected: no errors (CREATE TABLE IF NOT EXISTS + OR REPLACE FUNCTION)
```

- [ ] **Step 4: Commit**

```bash
git add lakehouse/postgis/001_serving_schema.sql
git commit -m "feat(postgis): add needs_review column and auto-create review tasks on flagged defects"
```
