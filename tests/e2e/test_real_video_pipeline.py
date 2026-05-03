import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.fixtures.clients import (
    EndpointConfig,
    assert_topics_exist,
    list_minio_prefix,
    redis_ping,
    trino_query,
    wait_until,
)


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


def require_real_e2e(repo_root: Path):
    if os.environ.get("RUN_REAL_E2E") != "1":
        pytest.skip("Set RUN_REAL_E2E=1 or use scripts/run-e2e.ps1 to run the real E2E")

    required = [
        repo_root / "edge" / "assets" / "test.mp4",
        repo_root / "edge" / "models" / "yolo11s.pt",
        repo_root / "edge" / ".conf" / "camera_calibration.json",
        repo_root
        / "cloud"
        / "depth_estimation_model"
        / "Depth-Anything-V2"
        / "depth_anything_v2_vitl.pth",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        pytest.fail("Missing real E2E artifact(s):\n" + "\n".join(missing))


def start_service(repo_root: Path, command: list[str], env: dict[str, str], log_name: str):
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


def stop_process(process: subprocess.Popen):
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


def topic_has_records(topic: str) -> bool:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
            "group.id": f"e2e-assert-{topic}-{time.time_ns()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        deadline = time.time() + 30
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                return True
        return False
    finally:
        consumer.close()


@pytest.mark.e2e
@pytest.mark.requires_docker
@pytest.mark.requires_models
def test_real_video_edge_to_cloud_pipeline(repo_root):
    require_real_e2e(repo_root)
    config = EndpointConfig()
    services = []
    logs = []

    assert_topics_exist(config, PIPELINE_TOPICS + DLQ_TOPICS)
    assert redis_ping(config)

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
        "BATCH_SIZE": "1",
        "BATCH_TIMEOUT_SECONDS": "3",
        "AGGREGATION_TIMEOUT_SECONDS": "300",
        "CLEANUP_INTERVAL_SECONDS": "5",
    }

    try:
        commands = [
            ([sys.executable, "cloud/etl_service/etl_microservice.py"], "etl"),
            ([sys.executable, "cloud/bev_surface_service/bev_surface_service.py"], "bev"),
            ([sys.executable, "cloud/depth_estimation_model/cloud_pipeline.py"], "depth"),
            (
                [sys.executable, "cloud/severity_calculation_service/severity_aggregator.py"],
                "severity",
            ),
            (
                [sys.executable, "cloud/final_enrichment_service/final_enrichment_service.py"],
                "final",
            ),
        ]
        for command, name in commands:
            process, log = start_service(repo_root, command, service_env, name)
            services.append(process)
            logs.append(log)

        time.sleep(20)

        edge = subprocess.Popen(
            [
                sys.executable,
                "main.py",
                "--config",
                "../tests/config/edge.e2e.yaml",
                "--video",
                "./assets/test.mp4",
            ],
            cwd=repo_root / "edge",
            env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1"},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        edge_output, _ = edge.communicate(timeout=600)
        assert edge.returncode == 0, edge_output

        wait_until(lambda: len(list_minio_prefix(config, "raw_images/")) > 0, 120)
        wait_until(lambda: len(list_minio_prefix(config, "bev_images/")) > 0, 180)

        for topic in PIPELINE_TOPICS:
            assert topic_has_records(topic), f"No records observed on {topic}"

        wait_until(
            lambda: trino_query(config, "SELECT count(*) FROM raw_events")[0][0] > 0,
            240,
        )
        wait_until(
            lambda: trino_query(config, "SELECT count(*) FROM surface_area_events")[0][0] > 0,
            240,
        )
        wait_until(
            lambda: trino_query(config, "SELECT count(*) FROM severity_scores")[0][0] > 0,
            240,
        )
        wait_until(lambda: trino_query(config, "SELECT count(*) FROM potholes")[0][0] > 0, 240)
        wait_until(
            lambda: trino_query(config, "SELECT count(*) FROM pothole_history")[0][0] > 0,
            240,
        )

        rows = trino_query(
            config,
            """
            SELECT r.event_id, s.surface_area_cm2, d.surface_area_cm2, p.raw_image_object_key, p.geom_h3
            FROM raw_events r
            JOIN surface_area_events s ON r.event_id = s.event_id
            JOIN severity_scores d ON r.event_id = d.event_id
            JOIN potholes p ON r.event_id = p.first_event_id
            LIMIT 1
            """,
        )
        assert rows, "No joined event_id made it through raw -> surface -> severity -> enrichment"
        event_id, surface_area, severity_area, raw_image_object_key, geom_h3 = rows[0]
        assert event_id
        assert surface_area == severity_area
        assert raw_image_object_key
        assert isinstance(geom_h3, int)

    finally:
        for process in reversed(services):
            stop_process(process)
        for log in logs:
            log.close()
