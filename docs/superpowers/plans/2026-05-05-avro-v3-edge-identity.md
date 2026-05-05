# Avro v3 + Edge Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add stable device_id + persistent vehicle_id to the edge, define Avro v3 raw schema with device_id/model_id/quality_flags, register it, and update all consumers to v3.

**Architecture:** Edge gains a state file (`local_storage/.device_state.json`) for vehicle_id persistence and a `GpsProvider` abstraction. A new `pothole.raw.events.v3` topic is introduced; edge produces to v3; ETL/BEV/Final Enrichment migrate to consume v3; Flink 010 source DDL updated. Severity score Avro type promoted int→double as a schema evolution on the existing subject.

**Tech Stack:** Python (edge + cloud services), Confluent Schema Registry REST API, Flink SQL, pytest

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `edge/device_state.py` | Load/persist vehicle_id + device_id from disk |
| Create | `edge/gps_provider.py` | GpsProvider ABC + SimulatedGpsProvider |
| Create | `edge/avro_schemas/raw_events_v3.json` | Avro v3 schema definition |
| Create | `edge/avro_schemas/severity_score_v2.json` | Severity schema with double severity_score |
| Modify | `edge/data_models.py` | Add device_id, model_id, quality_flags to BundledData |
| Modify | `edge/config_loader.py` | Add device_id field |
| Modify | `edge/config.yaml` | Add device_id default |
| Modify | `edge/main.py` | Load device_state, pass device_id + GPS provider |
| Modify | `edge/uploader.py` | Publish to v3 topic with new fields |
| Modify | `cloud/bev_surface_service/bev_surface_service.py` | Consume v3 topic |
| Modify | `cloud/etl_service/etl_microservice.py` | Consume v3 topic, update bronze schema |
| Modify | `cloud/final_enrichment_service/final_enrichment_service.py` | Consume v3 topic |
| Modify | `cloud/severity_calculation_service/severity_aggregator.py` | Produce double severity_score |
| Modify | `lakehouse/flink/sql/010_kafka_to_bronze.sql` | Add device_id, quality_flags to kafka_raw_events source |
| Modify | `KAFKA-CONF.md` | Document v3 schema |
| Create | `tests/contract/test_avro_v3_schemas.py` | Schema registration + compatibility tests |

---

### Task 1: Persistent Device State

**Files:**
- Create: `edge/device_state.py`
- Create: `tests/unit/test_device_state.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_device_state.py
import json
import pytest
from pathlib import Path
from unittest.mock import patch

def test_load_creates_state_file_on_first_run(tmp_path):
    state_path = tmp_path / ".device_state.json"
    from edge.device_state import load_device_state
    state = load_device_state(state_path)
    assert state_path.exists()
    assert state["vehicle_id"].startswith("vehicle-")
    assert len(state["vehicle_id"]) > 8

def test_load_returns_same_vehicle_id_on_second_run(tmp_path):
    state_path = tmp_path / ".device_state.json"
    from edge.device_state import load_device_state
    first = load_device_state(state_path)
    second = load_device_state(state_path)
    assert first["vehicle_id"] == second["vehicle_id"]

def test_load_respects_existing_state(tmp_path):
    state_path = tmp_path / ".device_state.json"
    state_path.write_text(json.dumps({"vehicle_id": "vehicle-abc123"}))
    from edge.device_state import load_device_state
    state = load_device_state(state_path)
    assert state["vehicle_id"] == "vehicle-abc123"
```

- [ ] **Step 2: Run test — expect failure**

```
pytest tests/unit/test_device_state.py -v
# Expected: ImportError or ModuleNotFoundError
```

- [ ] **Step 3: Implement `edge/device_state.py`**

```python
import json
from pathlib import Path
from uuid import uuid4

_DEFAULT_STATE_PATH = Path(__file__).parent / "local_storage" / ".device_state.json"


def load_device_state(path: Path = _DEFAULT_STATE_PATH) -> dict:
    """Load or create persistent device state. Returns dict with vehicle_id."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {"vehicle_id": f"vehicle-{uuid4().hex[:8]}"}
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    return state
```

- [ ] **Step 4: Run tests — expect pass**

```
pytest tests/unit/test_device_state.py -v
# Expected: 3 passed
```

- [ ] **Step 5: Commit**

```bash
git add edge/device_state.py tests/unit/test_device_state.py
git commit -m "feat(edge): add persistent device state for stable vehicle_id"
```

---

### Task 2: GPS Provider Abstraction

**Files:**
- Create: `edge/gps_provider.py`
- Create: `tests/unit/test_gps_provider.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_gps_provider.py
def test_simulated_gps_returns_reading_within_bounds():
    from edge.gps_provider import SimulatedGpsProvider
    p = SimulatedGpsProvider(lat_min=10.7, lat_max=10.9, lon_min=106.6, lon_max=106.8)
    reading = p.read()
    assert 10.7 <= reading.lat <= 10.9
    assert 106.6 <= reading.lon <= 106.8
    assert reading.accuracy_m is None

def test_gps_provider_is_abstract():
    from edge.gps_provider import GpsProvider
    import inspect
    assert inspect.isabstract(GpsProvider)
```

- [ ] **Step 2: Run test — expect failure**

```
pytest tests/unit/test_gps_provider.py -v
# Expected: ImportError
```

- [ ] **Step 3: Implement `edge/gps_provider.py`**

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
import random


@dataclass
class GpsReading:
    lat: float
    lon: float
    accuracy_m: float | None = None


class GpsProvider(ABC):
    @abstractmethod
    def read(self) -> GpsReading:
        ...


class SimulatedGpsProvider(GpsProvider):
    def __init__(self, lat_min: float, lat_max: float, lon_min: float, lon_max: float):
        self._lat_min = lat_min
        self._lat_max = lat_max
        self._lon_min = lon_min
        self._lon_max = lon_max

    def read(self) -> GpsReading:
        return GpsReading(
            lat=random.uniform(self._lat_min, self._lat_max),
            lon=random.uniform(self._lon_min, self._lon_max),
        )
```

- [ ] **Step 4: Run tests — expect pass**

```
pytest tests/unit/test_gps_provider.py -v
# Expected: 2 passed
```

- [ ] **Step 5: Commit**

```bash
git add edge/gps_provider.py tests/unit/test_gps_provider.py
git commit -m "feat(edge): add GPS provider abstraction with simulated provider"
```

---

### Task 3: Device ID Config + BundledData Update

**Files:**
- Modify: `edge/config.yaml`
- Modify: `edge/config_loader.py`
- Modify: `edge/data_models.py`

- [ ] **Step 1: Add device_id to `edge/config.yaml`**

Add under the top-level config (before or after `model_type`):

```yaml
# Stable device identifier. Override with EDGE_DEVICE_ID env var.
device_id: "${EDGE_DEVICE_ID:edge-device-001}"
```

- [ ] **Step 2: Add device_id to `edge/config_loader.py`**

Find the config dataclass (or dict-based loader). Add `device_id: str` field. If `ConfigLoader` returns a plain dict, ensure `config["device_id"]` is accessible. Example for a dataclass-based loader:

```python
@dataclass
class EdgeConfig:
    # ... existing fields ...
    device_id: str = "edge-device-001"
```

If it's dict-based, no change needed — the YAML key is read automatically.

- [ ] **Step 3: Add device_id, model_id, quality_flags to `edge/data_models.py`**

Find `BundledData` dataclass and add fields:

```python
@dataclass
class BundledData:
    event_id: str
    vehicle_id: str
    device_id: str                          # NEW
    timestamp: float
    gps_lat: float
    gps_lon: float
    gps_accuracy: float | None
    raw_image_object_key: str
    original_mask: list
    detection_confidence: float | None
    image_bytes: bytes
    model_id: str | None = None             # NEW
    quality_flags: list[str] = None         # NEW

    def __post_init__(self):
        if self.quality_flags is None:
            self.quality_flags = []
```

- [ ] **Step 4: Run existing tests — expect pass (no regressions)**

```
pytest tests/ -v -k "not e2e"
# Expected: all existing tests pass
```

- [ ] **Step 5: Commit**

```bash
git add edge/config.yaml edge/config_loader.py edge/data_models.py
git commit -m "feat(edge): add device_id and quality_flags fields to config and data models"
```

---

### Task 4: Wire device_id into EdgePipeline and Uploader

**Files:**
- Modify: `edge/main.py`
- Modify: `edge/uploader.py`

- [ ] **Step 1: Update `edge/main.py` to load device state and GPS provider**

In `EdgePipeline.__init__()`:

```python
from edge.device_state import load_device_state
from edge.gps_provider import SimulatedGpsProvider

class EdgePipeline:
    def __init__(self, config_path: str = "config.yaml", video_path: str | None = None):
        self.config = load_config(config_path)
        
        # Load persistent device state (replaces uuid4 vehicle_id)
        state = load_device_state()
        self.vehicle_id = state["vehicle_id"]
        self.device_id = self.config.get("device_id", "edge-device-001")
        
        # GPS provider (simulated for now)
        gps_cfg = self.config.get("gps", {})
        self.gps_provider = SimulatedGpsProvider(
            lat_min=gps_cfg.get("lat_min", 10.7),
            lat_max=gps_cfg.get("lat_max", 10.9),
            lon_min=gps_cfg.get("lon_min", 106.6),
            lon_max=gps_cfg.get("lon_max", 106.8),
        )
        # ... rest of __init__ unchanged ...
```

Remove the old `self.vehicle_id = f"vehicle-{uuid4().hex[:8]}"` line.
Remove the old `_generate_random_gps()` call in the inference worker and replace with `self.gps_provider.read()`.

- [ ] **Step 2: Update `edge/uploader.py` to build v3 BundledData**

In `Uploader.process_detection()`, update BundledData construction:

```python
bundled = BundledData(
    event_id=str(uuid4()),
    vehicle_id=self._vehicle_id,
    device_id=self._device_id,          # NEW
    timestamp=time.time(),
    gps_lat=gps.lat,                    # from GpsReading
    gps_lon=gps.lon,
    gps_accuracy=gps.accuracy_m,
    raw_image_object_key=f"s3://warehouse/raw_images/{event_id}.jpg",
    original_mask=mask,
    detection_confidence=confidence,
    image_bytes=image_bytes,
    model_id=self._active_model_id,     # NEW (str | None)
    quality_flags=[],                   # NEW
)
```

Uploader constructor must accept `device_id` and `active_model_id`:

```python
class Uploader:
    def __init__(self, config: dict, vehicle_id: str, device_id: str, active_model_id: str | None = None):
        self._vehicle_id = vehicle_id
        self._device_id = device_id
        self._active_model_id = active_model_id
        # ... rest unchanged ...
```

Update the Uploader instantiation in `main.py`:

```python
self.uploader = Uploader(
    config=self.config,
    vehicle_id=self.vehicle_id,
    device_id=self.device_id,
    active_model_id=getattr(self.runtime_model, "model_id", None),
)
```

- [ ] **Step 3: Run existing tests — expect pass**

```
pytest tests/ -v -k "not e2e"
# Expected: no regressions
```

- [ ] **Step 4: Commit**

```bash
git add edge/main.py edge/uploader.py
git commit -m "feat(edge): wire device_id and persistent vehicle_id into uploader"
```

---

### Task 5: Define and Register Avro v3 Schema

**Files:**
- Create: `edge/avro_schemas/raw_events_v3.json`
- Create: `edge/avro_schemas/severity_score_v2.json`
- Create: `tests/contract/test_avro_v3_schemas.py`

- [ ] **Step 1: Create `edge/avro_schemas/raw_events_v3.json`**

```json
{
  "type": "record",
  "name": "RawEventV3",
  "namespace": "pothole.raw.v3",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "device_id", "type": ["null", "string"], "default": null},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "raw_image_object_key", "type": "string"},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null},
    {"name": "model_id", "type": ["null", "string"], "default": null},
    {"name": "quality_flags", "type": {"type": "array", "items": "string"}, "default": []}
  ]
}
```

- [ ] **Step 2: Create `edge/avro_schemas/severity_score_v2.json`** (severity_score as double)

```json
{
  "type": "record",
  "name": "SeverityScoreV2",
  "namespace": "pothole.severity.v2",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "severity_score", "type": "double"},
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["MINOR", "MODERATE", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

- [ ] **Step 3: Write contract test**

```python
# tests/contract/test_avro_v3_schemas.py
import json
import os
import pytest
import requests
from pathlib import Path

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8082")

SCHEMA_DIR = Path(__file__).parent.parent.parent / "edge" / "avro_schemas"

def _register_schema(subject: str, schema_dict: dict) -> int:
    payload = {"schema": json.dumps(schema_dict)}
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        json=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    assert resp.status_code in (200, 409), f"Registration failed: {resp.text}"
    return resp.json().get("id", -1)

@pytest.mark.integration
def test_raw_events_v3_schema_registers():
    schema = json.loads((SCHEMA_DIR / "raw_events_v3.json").read_text())
    schema_id = _register_schema("pothole.raw.events.v3-value", schema)
    assert schema_id > 0

@pytest.mark.integration
def test_severity_score_v2_schema_registers():
    schema = json.loads((SCHEMA_DIR / "severity_score_v2.json").read_text())
    schema_id = _register_schema("pothole.severity.score.v2-value", schema)
    assert schema_id > 0

@pytest.mark.integration
def test_v3_schema_has_required_new_fields():
    schema = json.loads((SCHEMA_DIR / "raw_events_v3.json").read_text())
    field_names = {f["name"] for f in schema["fields"]}
    assert "device_id" in field_names
    assert "model_id" in field_names
    assert "quality_flags" in field_names
```

- [ ] **Step 4: Run schema tests against running Schema Registry**

```
SCHEMA_REGISTRY_URL=http://localhost:8082 pytest tests/contract/test_avro_v3_schemas.py -v -m integration
# Expected: 3 passed (requires docker compose up)
```

- [ ] **Step 5: Commit**

```bash
git add edge/avro_schemas/ tests/contract/test_avro_v3_schemas.py
git commit -m "feat(schema): define and register Avro v3 raw events and severity v2 schemas"
```

---

### Task 6: Edge Produces to v3 Topic

**Files:**
- Modify: `edge/uploader.py`
- Modify: `edge/config.yaml`

- [ ] **Step 1: Update `edge/config.yaml` topic name**

Change:
```yaml
kafka:
  topic: "pothole.raw.events.v2"
```
To:
```yaml
kafka:
  topic: "pothole.raw.events.v3"
```

- [ ] **Step 2: Update `edge/uploader.py` to use v3 Avro schema**

Find where `AvroSerializer` is initialized. Change schema string source to load from `edge/avro_schemas/raw_events_v3.json`:

```python
import json
from pathlib import Path

_SCHEMA_DIR = Path(__file__).parent / "avro_schemas"

def _load_schema(filename: str) -> str:
    return (Path(__file__).parent / "avro_schemas" / filename).read_text()

# In Uploader.__init__() where AvroSerializer is created:
schema_str = _load_schema("raw_events_v3.json")
self._avro_serializer = AvroSerializer(
    schema_registry_client=self._schema_registry_client,
    schema_str=schema_str,
    to_dict=lambda obj, ctx: obj,
)
```

- [ ] **Step 3: Update Avro record construction in `_build_avro_record()`**

Add new fields to the dict passed to the serializer:

```python
def _build_avro_record(self, bundled: BundledData) -> dict:
    return {
        "event_id": bundled.event_id,
        "vehicle_id": bundled.vehicle_id,
        "device_id": bundled.device_id,           # NEW
        "timestamp": int(bundled.timestamp * 1000),
        "gps_lat": bundled.gps_lat,
        "gps_lon": bundled.gps_lon,
        "gps_accuracy": bundled.gps_accuracy,
        "raw_image_object_key": bundled.raw_image_object_key,
        "original_mask": bundled.original_mask,
        "detection_confidence": bundled.detection_confidence,
        "model_id": bundled.model_id,             # NEW
        "quality_flags": bundled.quality_flags,   # NEW
    }
```

- [ ] **Step 4: Run edge smoke test (requires docker compose up)**

```
python edge/main.py --video /dev/null 2>&1 | head -20
# Expected: starts up, connects to Kafka, no schema errors
```

- [ ] **Step 5: Commit**

```bash
git add edge/uploader.py edge/config.yaml
git commit -m "feat(edge): produce to pothole.raw.events.v3 with device_id and quality_flags"
```

---

### Task 7: Update Cloud Services to Consume v3

**Files:**
- Modify: `cloud/bev_surface_service/bev_surface_service.py`
- Modify: `cloud/etl_service/etl_microservice.py`
- Modify: `cloud/final_enrichment_service/final_enrichment_service.py`

- [ ] **Step 1: Update BEV service consumer topic**

In `cloud/bev_surface_service/config.yaml`:
```yaml
kafka:
  input_topic: "pothole.raw.events.v3"
```

In `bev_surface_service.py`, update `AvroDeserializer` schema string to v3:

```python
RAW_SCHEMA_STR = open(
    Path(__file__).parent.parent.parent / "edge" / "avro_schemas" / "raw_events_v3.json"
).read()
```

`device_id`, `model_id`, `quality_flags` will now be present in deserialized events. No other changes needed — BEV only uses `event_id`, `raw_image_object_key`, `original_mask`.

- [ ] **Step 2: Update ETL service topic mapping for v3**

In `cloud/etl_service/etl_microservice.py`, find `TopicTableMapping` for `pothole.raw.events.v2` and update to `pothole.raw.events.v3`. Update the Avro schema reference and PyArrow schema to include `device_id`, `model_id`, `quality_flags`:

```python
# In the raw events TopicTableMapping PyArrow schema:
pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("vehicle_id", pa.string(), nullable=False),
    pa.field("device_id", pa.string(), nullable=True),      # NEW
    pa.field("event_time", pa.timestamp("us"), nullable=False),
    pa.field("gps_lat", pa.float64(), nullable=False),
    pa.field("gps_lon", pa.float64(), nullable=False),
    pa.field("gps_accuracy_m", pa.float64(), nullable=True),
    pa.field("raw_image_object_key", pa.string(), nullable=False),
    pa.field("original_mask_json", pa.string(), nullable=False),
    pa.field("detection_confidence", pa.float64(), nullable=True),
    pa.field("model_id", pa.string(), nullable=True),       # NEW
    pa.field("ingested_at", pa.timestamp("us"), nullable=False),
])
```

Update the transform function to pass `device_id` and `model_id` through (map `None` to `None`).

- [ ] **Step 3: Update Final Enrichment consumer topic**

In `cloud/final_enrichment_service/config.yaml`:
```yaml
kafka:
  raw_topic: "pothole.raw.events.v3"
```

Update `AvroDeserializer` schema reference same as BEV step. The `device_id`, `model_id` fields will be in the raw half of aggregated events — pass them through to the Trino UPSERT if desired (nice-to-have for now; not breaking if ignored).

- [ ] **Step 4: Update Severity Calculator to produce double severity_score**

In `cloud/severity_calculation_service/severity_aggregator.py`, change the Avro schema reference to `severity_score_v2.json` and ensure `severity_score` is emitted as `float`:

```python
SEVERITY_SCHEMA_STR = open(
    Path(__file__).parent.parent.parent / "edge" / "avro_schemas" / "severity_score_v2.json"
).read()

# In score computation, ensure float output:
severity_score = float(min(10, max(1, math.ceil(raw_score))))
```

Update the output topic in config if needed (topic stays `pothole.severity.score.v1` — schema subject changes to `v2` but topic name is unchanged).

- [ ] **Step 5: Run all cloud service unit tests**

```
pytest tests/ -v -k "not e2e and not integration"
# Expected: no regressions
```

- [ ] **Step 6: Commit**

```bash
git add cloud/bev_surface_service/ cloud/etl_service/ \
        cloud/final_enrichment_service/ cloud/severity_calculation_service/
git commit -m "feat(cloud): migrate services to pothole.raw.events.v3 and severity double type"
```

---

### Task 8: Update Flink 010 Source DDL for v3

**Files:**
- Modify: `lakehouse/flink/sql/010_kafka_to_bronze.sql`
- Modify: `KAFKA-CONF.md`

- [ ] **Step 1: Update `kafka_raw_events` source DDL in `010_kafka_to_bronze.sql`**

Replace the `kafka_raw_events` temporary table definition:

```sql
CREATE TEMPORARY TABLE kafka_raw_events (
  event_id STRING,
  vehicle_id STRING,
  device_id STRING,
  event_time TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  gps_accuracy_m DOUBLE,
  raw_image_object_key STRING,
  original_mask_json STRING,
  detection_confidence DOUBLE,
  model_id STRING,
  quality_flags ARRAY<STRING>,
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.raw.events.v3',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-raw-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);
```

Update the corresponding INSERT to pass through the new fields:

```sql
INSERT INTO bronze.raw_detection_events
SELECT
  event_id, vehicle_id, device_id, event_time,
  gps_lat, gps_lon, gps_accuracy_m,
  raw_image_object_key, original_mask_json, detection_confidence,
  model_id,
  CAST(quality_flags AS STRING),   -- serialize array to JSON string for VARCHAR column
  kafka_topic, kafka_partition, kafka_offset,
  CURRENT_TIMESTAMP AS ingested_at,
  CAST(NULL AS STRING) AS payload_json
FROM kafka_raw_events;
```

Note: `bronze.raw_detection_events.payload_json` was the catch-all; `model_id` and `quality_flags` now have dedicated columns. Verify `lakehouse/iceberg/010_bronze_tables.sql` has `model_id VARCHAR` and `quality_flags_json VARCHAR` columns; add them if missing.

- [ ] **Step 2: Add model_id and quality_flags_json to bronze DDL if missing**

In `lakehouse/iceberg/010_bronze_tables.sql`, add after `detection_confidence`:

```sql
CREATE TABLE IF NOT EXISTS iceberg.bronze.raw_detection_events (
  event_id VARCHAR,
  vehicle_id VARCHAR,
  device_id VARCHAR,                   -- new
  event_time TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  gps_accuracy_m DOUBLE,
  raw_image_object_key VARCHAR,
  original_mask_json VARCHAR,
  detection_confidence DOUBLE,
  model_id VARCHAR,                    -- new
  quality_flags_json VARCHAR,          -- new (serialized array)
  kafka_topic VARCHAR,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(event_time)']);
```

- [ ] **Step 3: Document v3 in `KAFKA-CONF.md`**

Add a `### pothole.raw.events.v3` section after the v2 section with the full schema, noting that v2 is deprecated (consumers migrated to v3). Note the `severity_score` type change in severity v2 schema section.

- [ ] **Step 4: Commit**

```bash
git add lakehouse/flink/sql/010_kafka_to_bronze.sql \
        lakehouse/iceberg/010_bronze_tables.sql \
        KAFKA-CONF.md
git commit -m "feat(lakehouse): update Flink 010 source DDL and bronze DDL for v3 schema"
```

---

### Task 9: Kafka Topic Creation for v3

**Files:**
- Modify: `docker-compose.yml` (kafka-init entrypoint)

- [ ] **Step 1: Add v3 topic creation to `kafka-init` entrypoint**

In `docker-compose.yml`, add to the `kafka-init` entrypoint script after the existing topic creates:

```yaml
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --create \
    --topic pothole.raw.events.v3 \
    --partitions 12 --replication-factor 3 \
    --if-not-exists \
    --config retention.ms=604800000 --config cleanup.policy=delete

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --create \
    --topic pothole.raw.events.v3.dlq.v1 \
    --partitions 12 --replication-factor 3 \
    --if-not-exists \
    --config retention.ms=1209600000 --config cleanup.policy=delete
```

- [ ] **Step 2: Verify topics created**

```bash
docker compose up kafka-init --no-deps
docker compose exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server kafka-1:9094 | grep pothole.raw.events.v3
# Expected: pothole.raw.events.v3 and pothole.raw.events.v3.dlq.v1
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(kafka): add pothole.raw.events.v3 and DLQ topic to kafka-init"
```
