"""Pipeline Observer: end-to-end latency for the v2 path -> Redis + Prometheus.

Consumes the four v2 topics keyed by event_id, watches PostGIS for the moment a
defect becomes queryable (the website final hop), computes per-stage + total
latency, and emits to:
  - Redis, via shared.latency_tracker (the exact schema web/app/lib/redis.server.ts reads)
  - Prometheus /metrics (histogram per stage + last-total gauge)

Stage mapping onto PipelineLatencyEvent (reuses existing stage keys):
  edge_detected_at      = raw event `timestamp`
  kafka_produced_at     = observer receipt of the raw message
  raw_event_ingested_at = surface-area `processed_at`
  severity_calculated_at= severity `calculated_at`
  pothole_stored_at     = time defect row appears in serving.current_road_defects
"""
import os
import sys
import time
import threading
from datetime import datetime
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import start_http_server, Histogram, Gauge

sys.path.append(str(Path(__file__).resolve().parents[1]))
from shared.latency_tracker import LatencyTracker, PipelineLatencyEvent

KAFKA = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9094,kafka-2:9094,kafka-3:9094")
SR = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PG = dict(
    host=os.environ.get("POSTGIS_HOST", "postgis"),
    port=int(os.environ.get("POSTGIS_PORT", "5432")),
    user=os.environ.get("POSTGIS_USER", "serving"),
    password=os.environ.get("POSTGIS_PASSWORD", "servingpassword"),
    dbname=os.environ.get("POSTGIS_DB", "postgis_serving"),
)
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8009"))

TOPICS = [
    "pothole.raw.events.v2",
    "pothole.surface.area.v2",
    "pothole.depth.v1",
    "pothole.severity.score.v1",
]

STAGE_HIST = Histogram(
    "pothole_pipeline_stage_latency_ms", "Per-stage latency (ms)", ["stage"],
    buckets=(50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000),
)
LAST_TOTAL = Gauge("pothole_pipeline_total_latency_ms", "Last total edge->queryable latency (ms)")

_state = {}  # event_id -> {edge_ts, kafka_recv_ts, surface_ts, severity_ts}
_lock = threading.Lock()
_tracker = LatencyTracker(REDIS_HOST, REDIS_PORT)


def _ms(v):
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    return int(v)


def consume():
    sr = SchemaRegistryClient({"url": SR})
    deser = AvroDeserializer(sr, None, lambda o, c: o)  # writer schema fetched from registry by id
    c = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "pipeline-observer-v1",
        # earliest: read all events in the (per-demo-reset) topics so _state is
        # fully populated; latest races consumer-group assignment and skips events.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe(TOPICS)
    print(f"[OBSERVER] consuming {TOPICS}")
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        topic = msg.topic()
        try:
            rec = deser(msg.value(), SerializationContext(topic, MessageField.VALUE))
        except Exception as e:
            print(f"[OBSERVER] deser error on {topic}: {e}")
            continue
        if not rec or "event_id" not in rec:
            continue
        eid = rec["event_id"]
        now_ms = int(time.time() * 1000)
        with _lock:
            s = _state.setdefault(eid, {})
            if topic == "pothole.raw.events.v2":
                s["edge_ts"] = _ms(rec["timestamp"])
                s["kafka_recv_ts"] = now_ms
            elif topic == "pothole.surface.area.v2":
                s["surface_ts"] = _ms(rec["processed_at"])
            elif topic == "pothole.severity.score.v1":
                s["severity_ts"] = _ms(rec["calculated_at"])


def watch_postgis():
    seen = set()
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    print("[OBSERVER] watching serving.current_road_defects")
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT defect_id FROM serving.current_road_defects "
                    "WHERE last_seen_at > now() - interval '10 minutes'"
                )
                rows = [r[0] for r in cur.fetchall()]
        except Exception as e:
            print(f"[OBSERVER] pg poll error: {e}")
            time.sleep(2)
            continue
        now_ms = int(time.time() * 1000)
        for defect_id in rows:
            if defect_id in seen or not defect_id.startswith("defect-"):
                continue
            eid = defect_id[len("defect-"):]
            with _lock:
                s = _state.get(eid)
                if not s or "edge_ts" not in s:
                    continue
                ev = PipelineLatencyEvent(
                    event_id=eid,
                    edge_detected_at=s["edge_ts"],
                    kafka_produced_at=s.get("kafka_recv_ts"),
                    raw_event_ingested_at=s.get("surface_ts"),
                    severity_calculated_at=s.get("severity_ts"),
                    pothole_stored_at=now_ms,
                ).calculate_latencies()
            _tracker.record_event(ev)  # writes latency:* Redis schema
            for stage, val in [
                ("edge_to_kafka", ev.edge_to_kafka_ms),
                ("kafka_to_storage", ev.kafka_to_raw_storage_ms),
                ("depth_estimation", ev.raw_to_severity_ms),
                ("enrichment", ev.severity_to_pothole_ms),
                ("total", ev.total_pipeline_ms),
            ]:
                if val is not None:
                    STAGE_HIST.labels(stage=stage).observe(val)
            if ev.total_pipeline_ms is not None:
                LAST_TOTAL.set(ev.total_pipeline_ms)
            seen.add(defect_id)
            print(f"[OBSERVER] {defect_id} total={ev.total_pipeline_ms}ms")
        time.sleep(2)


def main():
    start_http_server(METRICS_PORT)
    print(f"[OBSERVER] metrics on :{METRICS_PORT}")
    threading.Thread(target=consume, daemon=True).start()
    watch_postgis()


if __name__ == "__main__":
    main()
