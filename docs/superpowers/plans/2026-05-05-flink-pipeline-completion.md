# Flink Pipeline Completion — End-to-End Validation + H3 Dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap Iceberg DDL before Flink starts, add checkpointing, validate the full Kafka→Bronze→Silver→Gold→PostGIS streaming path end-to-end, and implement real H3-based defect deduplication in silver so each geographic pothole gets one defect_id.

**Architecture:** A bootstrap init service in docker-compose runs Iceberg DDL via Trino before Flink starts. A shell script submits all 4 Flink SQL jobs via the REST API. A Flink Python UDF computes H3 cells from GPS coordinates and is packaged into a JAR or registered inline. Silver materialization is restructured to use keyed stateful processing for H3 dedup via a Flink DataStream job (replacing the SQL-only LEFT JOIN approach for `silver.observations`). A contract test publishes sample Kafka events and asserts bronze records appear.

**Dependencies:** Plans 1 + 2 complete (v3 schema registered, quality_flags in bronze DDL).

**Tech Stack:** Flink 1.19 SQL + Python UDF, Trino SQL, Docker Compose, pytest + confluent-kafka (for integration test)

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `docker-compose.yml` | Add checkpointing env vars + flink-checkpoints volume + iceberg-bootstrap init service |
| Create | `lakehouse/flink/bootstrap_iceberg.sh` | Run Iceberg DDL via Trino before Flink starts |
| Create | `lakehouse/flink/run_jobs.sh` | Submit all 4 Flink SQL jobs via REST API |
| Modify | `lakehouse/flink/sql/020_silver_materialization.sql` | Add H3 cell computation via UDF |
| Create | `lakehouse/flink/udfs/h3_udf.py` | Flink Python UDF for H3 cell computation |
| Create | `tests/contract/test_flink_pipeline.py` | Kafka→bronze integration test |
| Modify | `lakehouse/flink/sql/README.md` | Add step-by-step run instructions |

---

### Task 1: Docker Compose — Checkpointing and Iceberg Bootstrap Service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add checkpoint env vars and volume to Flink services**

In `docker-compose.yml`, add to both `flink-jobmanager` and `flink-taskmanager` environment sections:

```yaml
  flink-jobmanager:
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - execution.checkpointing.interval=60000
      - execution.checkpointing.mode=EXACTLY_ONCE
      - state.backend=filesystem
      - state.checkpoints.dir=file:///opt/flink/checkpoints
    volumes:
      # ... existing JAR mounts ...
      - flink-checkpoints:/opt/flink/checkpoints

  flink-taskmanager:
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=4
      - state.backend=filesystem
      - state.checkpoints.dir=file:///opt/flink/checkpoints
    volumes:
      # ... existing JAR mounts ...
      - flink-checkpoints:/opt/flink/checkpoints
```

Add `flink-checkpoints:` to the top-level `volumes:` section.

- [ ] **Step 2: Add `iceberg-bootstrap` init service**

```yaml
  iceberg-bootstrap:
    image: trinodb/trino:478
    container_name: iceberg-bootstrap
    depends_on:
      polaris:
        condition: service_healthy
      minio:
        condition: service_healthy
    restart: "no"
    volumes:
      - ./lakehouse/iceberg:/opt/lakehouse/iceberg:ro
      - ./container-conf/trino/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties:ro
    entrypoint: /bin/bash
    command:
      - "-c"
      - |
          set -e
          echo "Waiting for Trino to be ready..."
          until /usr/lib/trino/bin/trino --server localhost:8080 \
                --execute "SELECT 1" > /dev/null 2>&1; do
            sleep 5
          done
          echo "Running Iceberg DDL bootstrap..."
          for f in /opt/lakehouse/iceberg/001_medallion_namespaces.sql \
                    /opt/lakehouse/iceberg/010_bronze_tables.sql \
                    /opt/lakehouse/iceberg/020_silver_tables.sql \
                    /opt/lakehouse/iceberg/030_gold_tables.sql \
                    /opt/lakehouse/iceberg/040_ml_tables.sql; do
            echo "Applying $f..."
            /usr/lib/trino/bin/trino --server localhost:8080 --file "$f"
          done
          echo "Iceberg bootstrap complete."
    networks:
      - kafka-net
```

Update `flink-jobmanager` and `flink-taskmanager` `depends_on` to include:
```yaml
    depends_on:
      iceberg-bootstrap:
        condition: service_completed_successfully
```

- [ ] **Step 3: Verify bootstrap service runs**

```bash
docker compose up iceberg-bootstrap --no-deps
docker compose logs iceberg-bootstrap | tail -5
# Expected: "Iceberg bootstrap complete."
```

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(infra): add Flink checkpointing and Iceberg bootstrap init service"
```

---

### Task 2: Flink Job Runner Script

**Files:**
- Create: `lakehouse/flink/run_jobs.sh`

- [ ] **Step 1: Create `lakehouse/flink/run_jobs.sh`**

```bash
#!/usr/bin/env bash
# Submit all Flink SQL jobs to the running Flink cluster.
# Usage: ./run_jobs.sh [FLINK_REST_URL]
set -e

FLINK_REST="${1:-http://localhost:8084}"
SQL_DIR="$(dirname "$0")/sql"

submit_sql_job() {
  local sql_file="$1"
  local job_name="$(basename "$sql_file" .sql)"
  echo "Submitting $job_name..."
  curl -s -X POST "${FLINK_REST}/v1/sessions" \
    -H "Content-Type: application/json" \
    -d '{"planner": "blink", "executionType": "STREAMING"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['sessionHandle'])" \
    > /tmp/session_handle.txt
  SESSION=$(cat /tmp/session_handle.txt)

  while IFS= read -r statement; do
    statement=$(echo "$statement" | sed '/^--/d' | tr -d '\n' | xargs)
    [ -z "$statement" ] && continue
    curl -s -X POST "${FLINK_REST}/v1/sessions/${SESSION}/statements" \
      -H "Content-Type: application/json" \
      -d "{\"statement\": $(python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))" <<< "$statement")}" \
      > /dev/null
  done < <(python3 -c "
import sys
content = open('$sql_file').read()
# Split on semicolons, skip empty
stmts = [s.strip() for s in content.split(';') if s.strip()]
for s in stmts: print(s + ';')
")
  echo "$job_name submitted (session: $SESSION)"
}

submit_sql_job "${SQL_DIR}/010_kafka_to_bronze.sql"
submit_sql_job "${SQL_DIR}/020_silver_materialization.sql"
submit_sql_job "${SQL_DIR}/030_gold_materialization.sql"
submit_sql_job "${SQL_DIR}/040_gold_to_postgis_projection.sql"

echo "All jobs submitted."
```

Make executable:
```bash
chmod +x lakehouse/flink/run_jobs.sh
```

- [ ] **Step 2: Commit**

```bash
git add lakehouse/flink/run_jobs.sh
git commit -m "feat(lakehouse): add Flink job runner script"
```

---

### Task 3: H3 UDF for Flink

**Files:**
- Create: `lakehouse/flink/udfs/h3_udf.py`

- [ ] **Step 1: Create `lakehouse/flink/udfs/h3_udf.py`**

```python
"""Flink Python UDF: compute H3 cell from lat/lon.
Returns BIGINT (h3.str_to_int of resolution-12 cell).
Register in Flink SQL:
  CREATE FUNCTION h3_cell AS 'h3_udf.H3CellUdf' LANGUAGE PYTHON;
"""
from pyflink.table import ScalarFunction, DataTypes
from pyflink.table.udf import udf
import h3


class H3CellUdf(ScalarFunction):
    def eval(self, lat: float, lon: float, resolution: int = 12) -> int | None:
        if lat is None or lon is None:
            return None
        cell_hex = h3.latlng_to_cell(lat, lon, resolution)
        return h3.str_to_int(cell_hex)


h3_cell = udf(H3CellUdf(), result_type=DataTypes.BIGINT())
```

- [ ] **Step 2: Register UDF in 020_silver_materialization.sql**

Add at the top of `020_silver_materialization.sql`, after `USE CATALOG lakehouse;`:

```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS h3_cell
  AS 'h3_udf.H3CellUdf'
  LANGUAGE PYTHON;
```

- [ ] **Step 3: Update silver.detections INSERT to compute h3_cell**

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
  model_id,
  CAST(NULL AS STRING) AS calibration_id,
  h3_cell(gps_lat, gps_lon, 12) AS h3_cell,          -- REAL H3 CELL NOW
  COALESCE(quality_flags_json, '[]') AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events;
```

- [ ] **Step 4: Verify UDF loads (requires Flink + PyFlink + h3 installed)**

```bash
docker compose exec flink-jobmanager python3 -c \
  "from h3_udf import H3CellUdf; u = H3CellUdf(); print(u.eval(10.8, 106.7))"
# Expected: prints an integer H3 cell ID
```

- [ ] **Step 5: Commit**

```bash
git add lakehouse/flink/udfs/h3_udf.py \
        lakehouse/flink/sql/020_silver_materialization.sql
git commit -m "feat(lakehouse): add H3 cell UDF and compute h3_cell in silver.detections"
```

---

### Task 4: Silver Observations — H3-Based Defect Deduplication

**Files:**
- Modify: `lakehouse/flink/sql/020_silver_materialization.sql`

The H3 dedup for `silver.observations` requires assigning the same `defect_id` to events at the same H3 cell. In pure Flink SQL, this requires either a stateful UDF or a lookup join against an existing `silver.observations` state. For the first milestone, use a deterministic approach: `defect_id = h3_cell_hex(gps_lat, gps_lon, resolution=12)` — same cell always gets same defect_id. This is a valid deduplication strategy (UUID-per-cell semantics), simpler than stateful aggregation.

- [ ] **Step 1: Add h3_cell_hex UDF**

Add to `lakehouse/flink/udfs/h3_udf.py`:

```python
class H3CellHexUdf(ScalarFunction):
    def eval(self, lat: float, lon: float, resolution: int = 12) -> str | None:
        if lat is None or lon is None:
            return None
        return h3.latlng_to_cell(lat, lon, resolution)

h3_cell_hex = udf(H3CellHexUdf(), result_type=DataTypes.STRING())
```

Register in `020_silver_materialization.sql`:

```sql
CREATE TEMPORARY FUNCTION IF NOT EXISTS h3_cell_hex
  AS 'h3_udf.H3CellHexUdf'
  LANGUAGE PYTHON;
```

- [ ] **Step 2: Update `silver.observations` INSERT to use H3-based defect_id**

```sql
INSERT INTO silver.observations
SELECT
  r.event_id AS observation_id,
  COALESCE(h3_cell_hex(r.gps_lat, r.gps_lon, 12), CONCAT('defect-', r.event_id)) AS defect_id,
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
  COALESCE(r.quality_flags_json, '[]') AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events r
LEFT JOIN bronze.surface_area_events a ON r.event_id = a.event_id
LEFT JOIN bronze.depth_estimation_events d ON r.event_id = d.event_id
LEFT JOIN bronze.severity_score_events sev ON r.event_id = sev.event_id;
```

`defect_id` is now the H3 cell hex string for events with valid GPS, falling back to `CONCAT('defect-', event_id)` only when GPS is NULL.

- [ ] **Step 3: Commit**

```bash
git add lakehouse/flink/udfs/h3_udf.py \
        lakehouse/flink/sql/020_silver_materialization.sql
git commit -m "feat(lakehouse): use H3 cell hex as defect_id in silver.observations"
```

---

### Task 5: Contract Test — Kafka to Bronze End-to-End

**Files:**
- Create: `tests/contract/test_flink_pipeline.py`

- [ ] **Step 1: Write contract test**

```python
# tests/contract/test_flink_pipeline.py
"""
Integration test: publish sample events to Kafka topics, verify they appear
in bronze Iceberg tables via Trino within 60s.
Requires: full docker compose up (Kafka + Schema Registry + Flink + Polaris + MinIO + Trino).
"""
import json
import os
import time
import pytest
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import trino

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8082")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))

RAW_V3_SCHEMA = json.loads(
    open("edge/avro_schemas/raw_events_v3.json").read()
)

def _trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user="admin", catalog="iceberg", schema="bronze",
    )

def _produce_raw_event(event_id: str):
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(sr_client, json.dumps(RAW_V3_SCHEMA))
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    record = {
        "event_id": event_id,
        "vehicle_id": "vehicle-test01",
        "device_id": "edge-device-test",
        "timestamp": int(time.time() * 1000),
        "gps_lat": 10.78,
        "gps_lon": 106.69,
        "gps_accuracy": None,
        "raw_image_object_key": f"s3://warehouse/raw_images/{event_id}.jpg",
        "original_mask": [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]],
        "detection_confidence": 0.85,
        "model_id": "yolo11s-v1",
        "quality_flags": [],
    }
    producer.produce(
        topic="pothole.raw.events.v3",
        key=event_id.encode(),
        value=serializer(record, SerializationContext("pothole.raw.events.v3", MessageField.VALUE)),
    )
    producer.flush(timeout=10)

@pytest.mark.integration
@pytest.mark.timeout(120)
def test_raw_event_lands_in_bronze():
    event_id = f"test-e2e-{int(time.time())}"
    _produce_raw_event(event_id)

    conn = _trino_conn()
    cur = conn.cursor()

    # Poll for up to 90 seconds
    deadline = time.time() + 90
    found = False
    while time.time() < deadline:
        cur.execute(
            "SELECT event_id, device_id FROM bronze.raw_detection_events WHERE event_id = ?",
            (event_id,),
        )
        rows = cur.fetchall()
        if rows:
            found = True
            assert rows[0][0] == event_id
            assert rows[0][1] == "edge-device-test"
            break
        time.sleep(5)

    assert found, f"Event {event_id} not found in bronze.raw_detection_events after 90s"
```

- [ ] **Step 2: Run test against full stack**

```bash
docker compose up -d
# Wait for all services healthy, then:
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
SCHEMA_REGISTRY_URL=http://localhost:8082 \
TRINO_HOST=localhost TRINO_PORT=8081 \
pytest tests/contract/test_flink_pipeline.py -v -m integration --timeout=120
# Expected: 1 passed (within 90s)
```

- [ ] **Step 3: Commit**

```bash
git add tests/contract/test_flink_pipeline.py
git commit -m "test(contract): add Kafka-to-bronze end-to-end Flink pipeline test"
```

---

### Task 6: Update lakehouse README

**Files:**
- Modify: `lakehouse/flink/sql/README.md`

- [ ] **Step 1: Update README with run instructions**

Replace the "Remaining work" section with:

```markdown
## Running the Jobs

1. Start infrastructure: `docker compose up -d`
2. Wait for `iceberg-bootstrap` to complete:
   `docker compose logs iceberg-bootstrap | tail -3`
3. Submit all jobs: `./lakehouse/flink/run_jobs.sh http://localhost:8084`
4. Verify jobs running: `http://localhost:8084` → Flink UI → Running Jobs
5. End-to-end test: `pytest tests/contract/test_flink_pipeline.py -v -m integration`

## H3 Deduplication

`silver.observations.defect_id` = H3 cell hex at resolution 12 (≈ 0.3 m² cell).
All events within the same H3 cell are assigned the same `defect_id`.
`gold.current_road_defects` aggregates by `defect_id` → one row per geographic pothole.
```

- [ ] **Step 2: Commit**

```bash
git add lakehouse/flink/sql/README.md
git commit -m "docs(lakehouse): update Flink README with run instructions and H3 dedup notes"
```
