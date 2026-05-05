"""Small clients used by real integration/E2E tests.

These helpers intentionally wrap production protocols instead of replacing
them. Unit tests should not import this module unless they explicitly opt into
integration/E2E behavior.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class EndpointConfig:
    kafka_bootstrap_servers: str = "localhost:19092,localhost:29092,localhost:39092"
    schema_registry_url: str = "http://localhost:8082"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "warehouse"
    trino_host: str = "localhost"
    trino_port: int = 8081
    redis_host: str = "localhost"
    redis_port: int = 6379


def wait_until(predicate, timeout_seconds: int = 120, interval_seconds: float = 2.0):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception as exc:  # pragma: no cover - diagnostic path
            last_error = exc
        time.sleep(interval_seconds)
    if last_error:
        raise TimeoutError(f"Condition did not become ready: {last_error}")
    raise TimeoutError("Condition did not become ready")


def kafka_topics(config: EndpointConfig) -> set[str]:
    from confluent_kafka.admin import AdminClient

    metadata = AdminClient(
        {"bootstrap.servers": config.kafka_bootstrap_servers}
    ).list_topics(timeout=10)
    return set(metadata.topics)


def assert_topics_exist(config: EndpointConfig, topics: Iterable[str]):
    existing = kafka_topics(config)
    missing = sorted(set(topics) - existing)
    assert not missing, f"Missing Kafka topics: {missing}"


def minio_object_exists(config: EndpointConfig, object_name: str) -> bool:
    from minio import Minio

    client = Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=False,
    )
    try:
        client.stat_object(config.minio_bucket, object_name)
        return True
    except Exception:
        return False


def list_minio_prefix(config: EndpointConfig, prefix: str) -> list[str]:
    from minio import Minio

    client = Minio(
        config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=False,
    )
    return [
        obj.object_name
        for obj in client.list_objects(config.minio_bucket, prefix=prefix, recursive=True)
    ]


def trino_query(config: EndpointConfig, sql: str):
    import trino

    conn = trino.dbapi.connect(
        host=config.trino_host,
        port=config.trino_port,
        user="trino",
        catalog="iceberg",
        schema="city",
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()


def redis_ping(config: EndpointConfig) -> bool:
    import redis

    return bool(redis.Redis(host=config.redis_host, port=config.redis_port).ping())


def get_topic_high_watermarks(config: EndpointConfig, topics: list[str]) -> dict[str, int]:
    """Return total high-watermark offset (sum across partitions) for each topic."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": f"watermark-probe-{int(time.time() * 1000)}",
            "auto.offset.reset": "earliest",
        }
    )
    result: dict[str, int] = {}
    try:
        for topic in topics:
            try:
                meta = consumer.list_topics(topic, timeout=10)
                partitions = list(meta.topics[topic].partitions.keys())
                total = 0
                for pid in partitions:
                    tp = TopicPartition(topic, pid)
                    _lo, hi = consumer.get_watermark_offsets(tp, timeout=5)
                    total += max(0, hi)
                result[topic] = total
            except Exception:
                result[topic] = 0
    finally:
        consumer.close()
    return result


def get_redis_latency_percentiles(
    config: EndpointConfig, stage: str = "total"
) -> "dict | None":
    """Read pipeline latency percentiles for one stage from Redis latency tracker."""
    try:
        import redis as _redis

        r = _redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            decode_responses=True,
            socket_timeout=5,
        )
        values = r.zrange(f"latency:stage:{stage}", 0, -1, withscores=True)
        if not values:
            return None
        latencies = sorted(v[1] for v in values)
        n = len(latencies)
        return {
            "count": n,
            "avg_ms": round(sum(latencies) / n, 1),
            "p50_ms": round(latencies[max(0, int(n * 0.50) - 1)], 1),
            "p95_ms": round(latencies[max(0, int(n * 0.95) - 1)], 1),
            "p99_ms": round(latencies[max(0, int(n * 0.99) - 1)], 1),
            "max_ms": round(latencies[-1], 1),
        }
    except Exception:
        return None
