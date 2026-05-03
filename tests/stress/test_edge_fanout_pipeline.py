"""
Stress / fan-out pipeline test.

Simulates many concurrent edge devices without running YOLO inference:
  - Extracts a small pool of JPEG frames from edge/assets/test.mp4 once.
  - Spawns EDGE_DEVICE_COUNT simulated producers, CONCURRENCY at a time.
  - Each producer uploads real JPEG images to MinIO and publishes valid Avro
    events to pothole.raw.events.v2.
  - All five cloud services (ETL, BEV, Depth, Severity, Final Enrichment) run
    as subprocesses against the real infrastructure stack.

Assertions:
  - Production throughput >= MIN_THROUGHPUT_EVENTS_PER_SEC
  - Zero DLQ growth across all four DLQ topics
  - >= 90 % of produced events ingested into raw_events (Iceberg)
  - >= 70 % of raw_events have a matching severity_score
  - At least one row appears in the potholes table (H3-deduped)
  - Pipeline p95 latency <= P95_LATENCY_THRESHOLD_MS (when Redis has >= 5 samples)

Configurable via environment variables:
  EDGE_DEVICE_COUNT             default 50
  EVENTS_PER_DEVICE             default 20
  CONCURRENCY                   default 10
  STRESS_DURATION_SECONDS       default 300
  P95_LATENCY_THRESHOLD_MS      default 120000  (2 min)
  MIN_THROUGHPUT_EVENTS_PER_SEC default 5.0
  FRAME_POOL_SIZE               default 20

Run with:
  scripts/run-stress.ps1
  or manually:
    $env:RUN_STRESS = "1"; python -m pytest tests/stress/test_edge_fanout_pipeline.py -m stress -v -s
"""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fixtures.clients import (
    EndpointConfig,
    assert_topics_exist,
    get_redis_latency_percentiles,
    get_topic_high_watermarks,
    redis_ping,
    trino_query,
    wait_until,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EDGE_DEVICE_COUNT = int(os.environ.get("EDGE_DEVICE_COUNT", "50"))
EVENTS_PER_DEVICE = int(os.environ.get("EVENTS_PER_DEVICE", "20"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "10"))
STRESS_DURATION_SECONDS = int(os.environ.get("STRESS_DURATION_SECONDS", "300"))
P95_LATENCY_THRESHOLD_MS = int(os.environ.get("P95_LATENCY_THRESHOLD_MS", "120000"))
MIN_THROUGHPUT_EVENTS_PER_SEC = float(os.environ.get("MIN_THROUGHPUT_EVENTS_PER_SEC", "5.0"))
FRAME_POOL_SIZE = int(os.environ.get("FRAME_POOL_SIZE", "20"))

TOTAL_EVENTS = EDGE_DEVICE_COUNT * EVENTS_PER_DEVICE

RAW_TOPIC = "pothole.raw.events.v2"

PIPELINE_TOPICS = [
    "pothole.raw.events.v2",
    "pothole.surface.area.v2",
    "pothole.depth.v1",
    "pothole.severity.score.v1",
]
DLQ_TOPICS = [
    "pothole.raw.events.dlq.v1",
    "pothole.surface.area.dlq.v1",
    "pothole.depth.dlq.v1",
    "pothole.severity.score.dlq.v1",
]

# Avro schema for the raw event (must match edge/uploader.py exactly)
_RAW_EVENT_SCHEMA_STR = """
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v2",
  "fields": [
    {"name": "event_id",            "type": "string"},
    {"name": "vehicle_id",          "type": "string"},
    {"name": "timestamp",           "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat",             "type": "double"},
    {"name": "gps_lon",             "type": "double"},
    {"name": "gps_accuracy",        "type": ["null", "double"], "default": null},
    {"name": "raw_image_object_key","type": "string"},
    {"name": "original_mask",       "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "detection_confidence","type": ["null", "double"], "default": null}
  ]
}
"""

# ---------------------------------------------------------------------------
# Helpers (mirrors test_real_video_pipeline.py pattern)
# ---------------------------------------------------------------------------

def _require_stress_env(repo_root: Path) -> None:
    if os.environ.get("RUN_STRESS") != "1":
        pytest.skip("Set RUN_STRESS=1 or use scripts/run-stress.ps1 to run the stress test")

    required = [
        repo_root / "edge" / "assets" / "test.mp4",
        repo_root / "edge" / ".conf" / "camera_calibration.json",
        repo_root
        / "cloud"
        / "depth_estimation_model"
        / "Depth-Anything-V2"
        / "depth_anything_v2_vitl.pth",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        pytest.fail("Missing stress test artifact(s):\n" + "\n".join(missing))


def _start_service(
    repo_root: Path, command: list[str], env: dict[str, str], log_name: str
) -> tuple[subprocess.Popen, object]:
    log_dir = repo_root / "tests" / ".artifacts"
    log_dir.mkdir(exist_ok=True)
    log_file = (log_dir / f"{log_name}.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=repo_root,
        env={**os.environ, **env},
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, log_file


def _stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()


def _trino_count(config: EndpointConfig, table: str) -> int:
    try:
        return trino_query(config, f"SELECT count(*) FROM {table}")[0][0]
    except Exception:
        return 0


def _extract_frames(video_path: Path, pool_size: int) -> list[bytes]:
    """Extract up to pool_size evenly-spaced JPEG frames from a video."""
    import cv2

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    step = max(1, total // pool_size)
    frames: list[bytes] = []
    for i in range(0, total, step):
        cap.set(cv2.CAP_PROP_POS_FRAMES, i)
        ret, frame = cap.read()
        if ret:
            _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            frames.append(buf.tobytes())
            if len(frames) >= pool_size:
                break
    cap.release()
    if not frames:
        raise RuntimeError(f"No frames extracted from {video_path}")
    return frames


def _random_mask() -> list[list[float]]:
    """Random rectangular mask in pixel coordinates (typical pothole shape)."""
    x0 = random.uniform(50.0, 300.0)
    y0 = random.uniform(50.0, 200.0)
    w = random.uniform(40.0, 150.0)
    h = random.uniform(30.0, 100.0)
    return [[x0, y0], [x0 + w, y0], [x0 + w, y0 + h], [x0, y0 + h]]


# ---------------------------------------------------------------------------
# Producer worker
# ---------------------------------------------------------------------------

def _produce_device_events(
    device_id: str,
    events_per_device: int,
    frame_pool: list[bytes],
    producer,           # confluent_kafka.Producer — produce() is thread-safe
    serializer,         # AvroSerializer — thread-safe after schema cache warms
    minio_client,       # Minio — thread-safe (urllib3 pool)
    bucket: str,
    produced: list[tuple[str, int]],  # [(event_id, detected_at_ms), ...]
    lock: threading.Lock,
) -> int:
    """Upload images to MinIO and queue Avro events to Kafka. Returns produced count."""
    from confluent_kafka.serialization import MessageField, SerializationContext

    count = 0
    for i in range(events_per_device):
        event_id = str(uuid4())
        frame_bytes = frame_pool[i % len(frame_pool)]
        object_name = f"stress_images/{event_id}.jpg"

        try:
            minio_client.put_object(
                bucket,
                object_name,
                BytesIO(frame_bytes),
                len(frame_bytes),
                content_type="image/jpeg",
            )
        except Exception as exc:
            print(f"[WARN] MinIO upload failed {event_id}: {exc}", flush=True)
            continue

        detected_at = int(time.time() * 1000)
        raw_event = {
            "event_id": event_id,
            "vehicle_id": device_id,
            "timestamp": detected_at,
            "gps_lat": random.uniform(10.70, 10.90),
            "gps_lon": random.uniform(106.60, 106.80),
            "gps_accuracy": random.uniform(5.0, 15.0),
            "raw_image_object_key": f"s3://{bucket}/{object_name}",
            "original_mask": _random_mask(),
            "detection_confidence": random.uniform(0.70, 0.99),
        }

        try:
            serialized = serializer(
                raw_event, SerializationContext(RAW_TOPIC, MessageField.VALUE)
            )
            # produce() is documented as thread-safe in confluent-kafka
            producer.produce(topic=RAW_TOPIC, key=event_id, value=serialized)
            with lock:
                produced.append((event_id, detected_at))
            count += 1
        except Exception as exc:
            print(f"[WARN] Kafka produce failed {event_id}: {exc}", flush=True)

    return count


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.stress
@pytest.mark.requires_docker
@pytest.mark.requires_models
def test_edge_fanout_pipeline(repo_root: Path) -> None:
    """
    Concurrent simulated edge producers → full cloud pipeline.

    Production phase: EDGE_DEVICE_COUNT × EVENTS_PER_DEVICE events are
    produced CONCURRENCY devices at a time.

    Drain phase: waits up to STRESS_DURATION_SECONDS for the pipeline to
    process all events, polling Trino for convergence.

    Assertions cover throughput, DLQ silence, ingestion coverage, severity
    coverage, enrichment activity, and pipeline latency percentiles.
    """
    _require_stress_env(repo_root)

    config = EndpointConfig()
    services: list[subprocess.Popen] = []
    logs: list = []

    assert_topics_exist(config, PIPELINE_TOPICS + DLQ_TOPICS)
    assert redis_ping(config), "Redis not reachable"

    # ---- baseline snapshots ------------------------------------------------
    dlq_before = get_topic_high_watermarks(config, DLQ_TOPICS)
    counts_before = {
        t: _trino_count(config, t)
        for t in ("raw_events", "surface_area_events", "severity_scores", "potholes", "pothole_history")
    }

    # ---- start cloud services ----------------------------------------------
    service_env = {
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUTF8": "1",
        "PYTHONUNBUFFERED": "1",
        "BEV_SURFACE_CONFIG": str(repo_root / "tests" / "config" / "bev_surface.e2e.yaml"),
        "DEPTH_SERVICE_CONFIG": str(repo_root / "tests" / "config" / "depth.e2e.yaml"),
        "SEVERITY_SERVICE_CONFIG": str(repo_root / "tests" / "config" / "severity.e2e.yaml"),
        "ETL_SERVICE_CONFIG": str(repo_root / "tests" / "config" / "etl.e2e.yaml"),
        "FINAL_ENRICHMENT_CONFIG": str(
            repo_root / "tests" / "config" / "final_enrichment.e2e.yaml"
        ),
        "BATCH_SIZE": "25",
        "BATCH_TIMEOUT_SECONDS": "5",
        "AGGREGATION_TIMEOUT_SECONDS": str(STRESS_DURATION_SECONDS),
        "CLEANUP_INTERVAL_SECONDS": "15",
    }

    cloud_commands = [
        ([sys.executable, "cloud/etl_service/etl_microservice.py"],               "stress-etl"),
        ([sys.executable, "cloud/bev_surface_service/bev_surface_service.py"],    "stress-bev"),
        ([sys.executable, "cloud/depth_estimation_model/cloud_pipeline.py"],      "stress-depth"),
        ([sys.executable, "cloud/severity_calculation_service/severity_aggregator.py"], "stress-severity"),
        ([sys.executable, "cloud/final_enrichment_service/final_enrichment_service.py"], "stress-final"),
    ]

    try:
        for command, name in cloud_commands:
            proc, log = _start_service(repo_root, command, service_env, name)
            services.append(proc)
            logs.append(log)

        print(f"\n[STRESS] Warming up services for 20s …", flush=True)
        time.sleep(20)

        # ---- frame pool ----------------------------------------------------
        video_path = repo_root / "edge" / "assets" / "test.mp4"
        frame_pool = _extract_frames(video_path, FRAME_POOL_SIZE)
        print(f"[STRESS] Frame pool: {len(frame_pool)} JPEG frames from {video_path.name}", flush=True)

        # ---- shared Kafka producer / MinIO client --------------------------
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from minio import Minio

        kafka_producer = Producer({"bootstrap.servers": config.kafka_bootstrap_servers})
        schema_registry = SchemaRegistryClient({"url": config.schema_registry_url})
        avro_serializer = AvroSerializer(
            schema_registry, _RAW_EVENT_SCHEMA_STR, lambda obj, ctx: obj
        )
        minio_client = Minio(
            config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=False,
        )

        produced_events: list[tuple[str, int]] = []
        produced_lock = threading.Lock()
        total_produced = 0

        # ---- production phase ----------------------------------------------
        print(
            f"\n[STRESS] Producing {TOTAL_EVENTS} events "
            f"({EDGE_DEVICE_COUNT} devices × {EVENTS_PER_DEVICE} events, "
            f"{CONCURRENCY} concurrent) …",
            flush=True,
        )
        device_ids = [f"stress-device-{i:04d}" for i in range(EDGE_DEVICE_COUNT)]
        production_start = time.time()

        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = {
                pool.submit(
                    _produce_device_events,
                    did,
                    EVENTS_PER_DEVICE,
                    frame_pool,
                    kafka_producer,
                    avro_serializer,
                    minio_client,
                    config.minio_bucket,
                    produced_events,
                    produced_lock,
                ): did
                for did in device_ids
            }
            completed = 0
            for future in as_completed(futures):
                total_produced += future.result()
                completed += 1
                # poll() must only be called from the main thread
                kafka_producer.poll(0)
                if completed % 10 == 0 or completed == EDGE_DEVICE_COUNT:
                    print(
                        f"[STRESS] {completed}/{EDGE_DEVICE_COUNT} devices done, "
                        f"{total_produced} events queued",
                        flush=True,
                    )

        production_elapsed = time.time() - production_start
        throughput = total_produced / production_elapsed if production_elapsed > 0 else 0.0

        remaining = kafka_producer.flush(timeout=60)
        if remaining:
            print(f"[WARN] {remaining} messages not delivered after 60s flush", flush=True)

        print(
            f"\n[STRESS] Production complete: {total_produced}/{TOTAL_EVENTS} events "
            f"in {production_elapsed:.1f}s ({throughput:.1f} events/s)",
            flush=True,
        )

        # ---- drain phase ---------------------------------------------------
        print(
            f"\n[STRESS] Waiting up to {STRESS_DURATION_SECONDS}s for pipeline to drain …",
            flush=True,
        )
        drain_start = time.time()
        prev_sev = -1
        stable_since = time.time()

        while time.time() - drain_start < STRESS_DURATION_SECONDS:
            raw_now = _trino_count(config, "raw_events") - counts_before["raw_events"]
            sev_now = _trino_count(config, "severity_scores") - counts_before["severity_scores"]
            print(
                f"[DRAIN]  raw_events={raw_now}  severity_scores={sev_now}"
                f"  (target ~{total_produced})",
                flush=True,
            )
            if sev_now != prev_sev:
                prev_sev = sev_now
                stable_since = time.time()
            elif time.time() - stable_since >= 30:
                print("[DRAIN] Pipeline stable for 30s — asserting.", flush=True)
                break
            time.sleep(10)

        # ---- collect final metrics -----------------------------------------
        new = {
            t: _trino_count(config, t) - counts_before[t]
            for t in counts_before
        }
        dlq_after = get_topic_high_watermarks(config, DLQ_TOPICS)
        latency = get_redis_latency_percentiles(config, "total")

        # ---- print results table -------------------------------------------
        sep = "=" * 62
        print(f"\n{sep}")
        print(f"  STRESS TEST RESULTS")
        print(sep)
        print(f"  Devices            : {EDGE_DEVICE_COUNT}")
        print(f"  Events / device    : {EVENTS_PER_DEVICE}")
        print(f"  Total produced     : {total_produced} / {TOTAL_EVENTS}")
        print(f"  Production time    : {production_elapsed:.1f}s")
        print(f"  Throughput         : {throughput:.1f} events/s  (min {MIN_THROUGHPUT_EVENTS_PER_SEC})")
        print(f"  --- Iceberg rows (new) ---")
        print(f"  raw_events         : {new['raw_events']}")
        print(f"  surface_area_events: {new['surface_area_events']}")
        print(f"  severity_scores    : {new['severity_scores']}")
        print(f"  potholes           : {new['potholes']}")
        print(f"  pothole_history    : {new['pothole_history']}")
        print(f"  --- DLQ (delta) ---")
        for topic in DLQ_TOPICS:
            delta = dlq_after.get(topic, 0) - dlq_before.get(topic, 0)
            print(f"  {topic:<42}: {'+' if delta > 0 else ''}{delta}")
        print(f"  --- Redis latency (pipeline total) ---")
        if latency:
            print(f"  samples            : {latency['count']}")
            print(f"  avg                : {latency['avg_ms']:.0f} ms")
            print(f"  p50                : {latency['p50_ms']:.0f} ms")
            print(f"  p95                : {latency['p95_ms']:.0f} ms  (threshold {P95_LATENCY_THRESHOLD_MS} ms)")
            print(f"  p99                : {latency['p99_ms']:.0f} ms")
            print(f"  max                : {latency['max_ms']:.0f} ms")
        else:
            print("  (no latency data in Redis — final enrichment may not have completed yet)")
        print(sep + "\n")

        # ---- assertions ----------------------------------------------------

        # 1. Production throughput
        assert throughput >= MIN_THROUGHPUT_EVENTS_PER_SEC, (
            f"Production throughput {throughput:.1f} events/s < minimum {MIN_THROUGHPUT_EVENTS_PER_SEC}"
        )

        # 2. Zero DLQ growth
        dlq_deltas = {t: dlq_after.get(t, 0) - dlq_before.get(t, 0) for t in DLQ_TOPICS}
        assert all(d == 0 for d in dlq_deltas.values()), (
            "DLQ topics received messages during stress run: "
            + ", ".join(f"{t}=+{d}" for t, d in dlq_deltas.items() if d > 0)
        )

        # 3. Raw events ingested: >= 90 % of produced
        assert new["raw_events"] >= int(total_produced * 0.90), (
            f"raw_events new rows {new['raw_events']} < 90% of {total_produced} produced"
        )

        # 4. Severity coverage: >= 70 % of ingested raw events have a severity score
        # (Some events may still be in-flight through BEV → depth at assertion time)
        if new["raw_events"] > 0:
            assert new["severity_scores"] >= int(new["raw_events"] * 0.70), (
                f"severity_scores {new['severity_scores']} < 70% of raw_events {new['raw_events']}"
            )

        # 5. Final enrichment produced at least one pothole
        assert new["potholes"] > 0, (
            "No new rows in potholes table — final enrichment may not have run"
        )

        # 6. p95 latency within threshold (only when Redis has enough samples)
        if latency and latency["count"] >= 5:
            assert latency["p95_ms"] <= P95_LATENCY_THRESHOLD_MS, (
                f"Pipeline p95 latency {latency['p95_ms']:.0f} ms "
                f"exceeds threshold {P95_LATENCY_THRESHOLD_MS} ms"
            )

    finally:
        for proc in reversed(services):
            _stop_process(proc)
        for log in logs:
            log.close()
