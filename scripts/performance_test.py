#!/usr/bin/env python3
"""
Performance Testing Suite for Pothole Monitoring Data Platform

Measures and records performance metrics for:
- Kafka (throughput, consumer lag, broker health)
- Trino/Iceberg (query latency, table stats)
- MinIO (storage usage, object counts)
- Redis (cache hit rates, memory usage)
- Docker containers (CPU, memory utilization)
- End-to-end data freshness

Usage:
    python performance_test.py --config config.yaml
    python performance_test.py --duration 60 --output results.json
    python performance_test.py --synthetic-events 100

Requirements:
    pip install pyyaml redis trino minio confluent-kafka
"""

import argparse
import json
import time
import uuid
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import threading

# Check for --help before importing heavy dependencies
if "--help" in sys.argv or "-h" in sys.argv:
    # Skip dependency check for help display
    pass

# Third-party imports with graceful error handling
_MISSING_DEPS = []
try:
    import yaml
except ImportError:
    _MISSING_DEPS.append("pyyaml")
    yaml = None

try:
    import redis as redis_lib
except ImportError:
    _MISSING_DEPS.append("redis")
    redis_lib = None

try:
    import trino.dbapi
except ImportError:
    _MISSING_DEPS.append("trino")
    trino = None

try:
    from minio import Minio
except ImportError:
    _MISSING_DEPS.append("minio")
    Minio = None

try:
    from confluent_kafka import Consumer, Producer, KafkaError
    from confluent_kafka.admin import AdminClient, ConsumerGroupListing
except ImportError:
    _MISSING_DEPS.append("confluent-kafka")
    Consumer = Producer = KafkaError = AdminClient = None

def check_dependencies():
    """Check if all required dependencies are installed."""
    if _MISSING_DEPS:
        print("Missing required packages:")
        for dep in _MISSING_DEPS:
            print(f"  - {dep}")
        print(f"\nInstall with: pip install {' '.join(_MISSING_DEPS)}")
        print("Or run: pip install -r scripts/requirements.txt")
        sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass
class Config:
    """Configuration for performance tests."""
    # Kafka
    kafka_bootstrap_servers: str = "localhost:19092,localhost:29092,localhost:39092"
    kafka_topics: List[str] = field(default_factory=lambda: [
        "pothole.raw.events.v1",
        "pothole.depth.v1",
        "pothole.severity.score.v1",
    ])
    kafka_consumer_groups: List[str] = field(default_factory=lambda: [
        "depth-estimation-service",
        "severity-calculation-service",
        "etl-service",
        "final-enrichment-service-v2",
    ])
    
    # Trino
    trino_host: str = "localhost"
    trino_port: int = 8081
    trino_user: str = "trino"
    trino_catalog: str = "iceberg"
    trino_schema: str = "city"
    
    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "warehouse"
    minio_secure: bool = False
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    
    # Test parameters
    test_duration_seconds: int = 60
    synthetic_event_count: int = 0
    query_iterations: int = 10
    
    # Targets for pass/fail evaluation
    targets: Dict[str, Any] = field(default_factory=lambda: {
        "kafka_throughput_min": 10,  # messages/second
        "consumer_lag_max": 1000,
        "trino_avg_latency_max_ms": 5000,
        "trino_p95_latency_max_ms": 10000,
        "redis_hit_rate_min": 0.5,
        "data_completeness_min": 0.95,
        "uptime_min": 0.99,
    })
    
    @classmethod
    def from_yaml(cls, path: str) -> 'Config':
        """Load configuration from YAML file."""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})
    
    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'Config':
        """Create config from command-line arguments."""
        config = cls()
        if args.kafka_bootstrap:
            config.kafka_bootstrap_servers = args.kafka_bootstrap
        if args.trino_host:
            config.trino_host = args.trino_host
        if args.trino_port:
            config.trino_port = args.trino_port
        if args.minio_endpoint:
            config.minio_endpoint = args.minio_endpoint
        if args.redis_host:
            config.redis_host = args.redis_host
        if args.redis_port:
            config.redis_port = args.redis_port
        if args.duration:
            config.test_duration_seconds = args.duration
        if args.synthetic_events:
            config.synthetic_event_count = args.synthetic_events
        return config


# ============================================================================
# METRICS DATA STRUCTURES
# ============================================================================

@dataclass
class MetricResult:
    """Single metric measurement."""
    component: str
    metric: str
    value: Any
    unit: str = ""
    target: Optional[Any] = None
    status: str = "N/A"  # PASS, FAIL, WARN, N/A
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def evaluate(self, target: Any = None, comparator: str = ">=") -> 'MetricResult':
        """Evaluate metric against target."""
        if target is None:
            target = self.target
        if target is None:
            return self
        
        self.target = target
        try:
            if comparator == ">=":
                self.status = "PASS" if self.value >= target else "FAIL"
            elif comparator == "<=":
                self.status = "PASS" if self.value <= target else "FAIL"
            elif comparator == "==":
                self.status = "PASS" if self.value == target else "FAIL"
            elif comparator == ">":
                self.status = "PASS" if self.value > target else "FAIL"
            elif comparator == "<":
                self.status = "PASS" if self.value < target else "FAIL"
        except (TypeError, ValueError):
            self.status = "N/A"
        return self


@dataclass
class PerformanceReport:
    """Complete performance test report."""
    test_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    start_time: str = ""
    end_time: str = ""
    duration_seconds: float = 0
    config: Dict = field(default_factory=dict)
    metrics: List[MetricResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    summary: Dict = field(default_factory=dict)
    
    def add_metric(self, metric: MetricResult):
        self.metrics.append(metric)
    
    def add_error(self, error: str):
        self.errors.append(f"[{datetime.now().isoformat()}] {error}")
    
    def to_dict(self) -> Dict:
        return {
            "test_id": self.test_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "config": self.config,
            "metrics": [asdict(m) for m in self.metrics],
            "errors": self.errors,
            "summary": self.summary,
        }
    
    def to_json(self, path: str):
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2, default=str)


# ============================================================================
# SERVICE HEALTH CHECKS
# ============================================================================

class HealthChecker:
    """Check health/availability of all services."""
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
    
    def check_kafka(self) -> Tuple[bool, str]:
        """Check Kafka broker connectivity."""
        try:
            admin = AdminClient({
                "bootstrap.servers": self.config.kafka_bootstrap_servers,
                "socket.timeout.ms": 5000,
            })
            cluster_metadata = admin.list_topics(timeout=10)
            broker_count = len(cluster_metadata.brokers)
            return True, f"{broker_count} brokers available"
        except Exception as e:
            return False, str(e)
    
    def check_trino(self) -> Tuple[bool, str]:
        """Check Trino connectivity."""
        try:
            conn = trino.dbapi.connect(
                host=self.config.trino_host,
                port=self.config.trino_port,
                user=self.config.trino_user,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True, "Connected"
        except Exception as e:
            return False, str(e)
    
    def check_minio(self) -> Tuple[bool, str]:
        """Check MinIO connectivity."""
        try:
            client = Minio(
                self.config.minio_endpoint,
                access_key=self.config.minio_access_key,
                secret_key=self.config.minio_secret_key,
                secure=self.config.minio_secure,
            )
            buckets = list(client.list_buckets())
            return True, f"{len(buckets)} buckets found"
        except Exception as e:
            return False, str(e)
    
    def check_redis(self) -> Tuple[bool, str]:
        """Check Redis connectivity."""
        try:
            client = redis_lib.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                socket_timeout=5,
            )
            client.ping()
            info = client.info("server")
            return True, f"Redis {info.get('redis_version', 'unknown')}"
        except Exception as e:
            return False, str(e)
    
    def check_all(self) -> Dict[str, bool]:
        """Check all services and record metrics."""
        services = {
            "Kafka": self.check_kafka,
            "Trino": self.check_trino,
            "MinIO": self.check_minio,
            "Redis": self.check_redis,
        }
        
        results = {}
        for name, check_fn in services.items():
            healthy, details = check_fn()
            results[name] = healthy
            
            self.report.add_metric(MetricResult(
                component=name,
                metric="health_status",
                value="UP" if healthy else "DOWN",
                status="PASS" if healthy else "FAIL",
            ))
            
            if not healthy:
                self.report.add_error(f"{name} health check failed: {details}")
            else:
                print(f"  ✓ {name}: {details}")
        
        return results


# ============================================================================
# KAFKA METRICS COLLECTOR
# ============================================================================

class KafkaMetrics:
    """Collect Kafka performance metrics."""
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
        self.admin = None
        
    def connect(self):
        """Initialize Kafka admin client."""
        self.admin = AdminClient({
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
        })
    
    def get_topic_throughput(self, duration_seconds: int = 10) -> Dict[str, float]:
        """Measure message throughput per topic."""
        consumer_conf = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "group.id": f"perf-test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
        
        throughputs = {}
        
        for topic in self.config.kafka_topics:
            try:
                consumer = Consumer(consumer_conf)
                consumer.subscribe([topic])
                
                # Warm up - get to latest offset
                consumer.poll(timeout=2.0)
                
                start_time = time.time()
                message_count = 0
                
                while time.time() - start_time < duration_seconds:
                    msg = consumer.poll(timeout=1.0)
                    if msg is not None and msg.error() is None:
                        message_count += 1
                
                elapsed = time.time() - start_time
                throughput = message_count / elapsed if elapsed > 0 else 0
                throughputs[topic] = round(throughput, 2)
                
                consumer.close()
                
            except Exception as e:
                self.report.add_error(f"Kafka throughput error for {topic}: {e}")
                throughputs[topic] = 0
        
        return throughputs
    
    def get_consumer_lag(self) -> Dict[str, Dict[str, int]]:
        """Get consumer lag for all consumer groups."""
        lags = {}
        
        for group_id in self.config.kafka_consumer_groups:
            try:
                consumer_conf = {
                    "bootstrap.servers": self.config.kafka_bootstrap_servers,
                    "group.id": group_id,
                }
                consumer = Consumer(consumer_conf)
                
                group_lags = {}
                for topic in self.config.kafka_topics:
                    try:
                        # Get topic partition info
                        metadata = consumer.list_topics(topic, timeout=5)
                        if topic not in metadata.topics:
                            continue
                        
                        partitions = metadata.topics[topic].partitions
                        total_lag = 0
                        
                        for partition_id in partitions.keys():
                            from confluent_kafka import TopicPartition
                            tp = TopicPartition(topic, partition_id)
                            
                            # Get committed offset
                            committed = consumer.committed([tp], timeout=5)
                            committed_offset = committed[0].offset if committed[0].offset >= 0 else 0
                            
                            # Get high watermark (latest offset)
                            low, high = consumer.get_watermark_offsets(tp, timeout=5)
                            
                            if high > committed_offset:
                                total_lag += (high - committed_offset)
                        
                        group_lags[topic] = total_lag
                        
                    except Exception as e:
                        self.report.add_error(f"Lag check error for {group_id}/{topic}: {e}")
                
                consumer.close()
                lags[group_id] = group_lags
                
            except Exception as e:
                self.report.add_error(f"Consumer group error for {group_id}: {e}")
        
        return lags
    
    def get_broker_stats(self) -> Dict:
        """Get broker health and configuration."""
        try:
            metadata = self.admin.list_topics(timeout=10)
            
            return {
                "broker_count": len(metadata.brokers),
                "topic_count": len(metadata.topics),
                "controller_id": metadata.controller_id,
                "brokers": [
                    {"id": b.id, "host": b.host, "port": b.port}
                    for b in metadata.brokers.values()
                ],
            }
        except Exception as e:
            self.report.add_error(f"Broker stats error: {e}")
            return {}
    
    def collect_all(self, throughput_duration: int = 10) -> Dict:
        """Collect all Kafka metrics."""
        print("\n[Kafka Metrics]")
        self.connect()
        
        # Broker stats
        broker_stats = self.get_broker_stats()
        print(f"  Brokers: {broker_stats.get('broker_count', 0)}")
        self.report.add_metric(MetricResult(
            component="Kafka",
            metric="broker_count",
            value=broker_stats.get("broker_count", 0),
            target=3,
        ).evaluate(3, ">="))
        
        # Throughput
        print(f"  Measuring throughput ({throughput_duration}s)...")
        throughputs = self.get_topic_throughput(throughput_duration)
        for topic, rate in throughputs.items():
            short_topic = topic.split(".")[-2]  # e.g., "raw" from "pothole.raw.events.v1"
            print(f"    {short_topic}: {rate} msg/s")
            self.report.add_metric(MetricResult(
                component="Kafka",
                metric=f"throughput_{short_topic}",
                value=rate,
                unit="msg/s",
                target=self.config.targets["kafka_throughput_min"],
            ).evaluate(self.config.targets["kafka_throughput_min"], ">="))
        
        # Consumer lag
        print("  Consumer lag:")
        lags = self.get_consumer_lag()
        max_lag = 0
        for group, topics in lags.items():
            total_lag = sum(topics.values())
            max_lag = max(max_lag, total_lag)
            print(f"    {group}: {total_lag}")
            self.report.add_metric(MetricResult(
                component="Kafka",
                metric=f"consumer_lag_{group.replace('-', '_')}",
                value=total_lag,
                unit="messages",
                target=self.config.targets["consumer_lag_max"],
            ).evaluate(self.config.targets["consumer_lag_max"], "<="))
        
        return {
            "brokers": broker_stats,
            "throughput": throughputs,
            "consumer_lag": lags,
        }


# ============================================================================
# TRINO/ICEBERG METRICS COLLECTOR
# ============================================================================

class TrinoMetrics:
    """Collect Trino query performance metrics."""
    
    # Dashboard queries to benchmark
    BENCHMARK_QUERIES = [
        ("summary_active_count", """
            SELECT COUNT(*) as count
            FROM iceberg.city.potholes
            WHERE status = 'reported'
        """),
        ("summary_severity_dist", """
            SELECT severity_level, COUNT(*) as count
            FROM iceberg.city.potholes
            WHERE status = 'reported'
            GROUP BY severity_level
        """),
        ("summary_last_30_days", """
            SELECT CAST(reported_at AS DATE) as date, COUNT(*) as count
            FROM iceberg.city.potholes
            WHERE reported_at >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
            GROUP BY CAST(reported_at AS DATE)
            ORDER BY date
        """),
        ("map_view_bounds", """
            SELECT pothole_id, gps_lat, gps_lon, severity_score, status
            FROM iceberg.city.potholes
            WHERE gps_lat BETWEEN 10.75 AND 10.85
              AND gps_lon BETWEEN 106.60 AND 106.75
            LIMIT 100
        """),
        ("pothole_detail", """
            SELECT *
            FROM iceberg.city.potholes
            LIMIT 1
        """),
    ]
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
        self.conn = None
    
    def connect(self):
        """Initialize Trino connection."""
        self.conn = trino.dbapi.connect(
            host=self.config.trino_host,
            port=self.config.trino_port,
            user=self.config.trino_user,
            catalog=self.config.trino_catalog,
            schema=self.config.trino_schema,
        )
    
    def run_query(self, query: str, timeout: int = 30) -> Tuple[float, int, Optional[str]]:
        """
        Execute query and return (latency_ms, row_count, error).
        """
        try:
            cursor = self.conn.cursor()
            start = time.perf_counter()
            cursor.execute(query)
            rows = cursor.fetchall()
            latency_ms = (time.perf_counter() - start) * 1000
            cursor.close()
            return latency_ms, len(rows), None
        except Exception as e:
            return 0, 0, str(e)
    
    def benchmark_queries(self, iterations: int = 10) -> Dict[str, Dict]:
        """Run benchmark queries multiple times and collect latencies."""
        results = {}
        
        for query_name, query_sql in self.BENCHMARK_QUERIES:
            latencies = []
            errors = 0
            
            for _ in range(iterations):
                latency_ms, row_count, error = self.run_query(query_sql)
                if error:
                    errors += 1
                    self.report.add_error(f"Query {query_name} error: {error}")
                else:
                    latencies.append(latency_ms)
            
            if latencies:
                results[query_name] = {
                    "avg_ms": round(statistics.mean(latencies), 2),
                    "p50_ms": round(statistics.median(latencies), 2),
                    "p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 2) if len(latencies) >= 5 else round(max(latencies), 2),
                    "min_ms": round(min(latencies), 2),
                    "max_ms": round(max(latencies), 2),
                    "iterations": len(latencies),
                    "errors": errors,
                }
            else:
                results[query_name] = {"error": "All iterations failed", "errors": errors}
        
        return results
    
    def get_table_stats(self) -> Dict:
        """Get Iceberg table statistics."""
        tables = ["potholes", "pothole_history", "raw_events", "severity_scores"]
        stats = {}
        
        for table in tables:
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM iceberg.city.{table}")
                row_count = cursor.fetchone()[0]
                cursor.close()
                stats[table] = {"row_count": row_count}
            except Exception as e:
                stats[table] = {"error": str(e)}
        
        return stats
    
    def collect_all(self, iterations: int = 10) -> Dict:
        """Collect all Trino metrics."""
        print("\n[Trino/Iceberg Metrics]")
        self.connect()
        
        # Table stats
        print("  Table row counts:")
        table_stats = self.get_table_stats()
        for table, stats in table_stats.items():
            if "row_count" in stats:
                print(f"    {table}: {stats['row_count']:,} rows")
                self.report.add_metric(MetricResult(
                    component="Iceberg",
                    metric=f"table_{table}_rows",
                    value=stats["row_count"],
                ))
        
        # Query benchmarks
        print(f"  Running query benchmarks ({iterations} iterations each)...")
        query_results = self.benchmark_queries(iterations)
        
        all_avg_latencies = []
        all_p95_latencies = []
        
        for query_name, stats in query_results.items():
            if "avg_ms" in stats:
                print(f"    {query_name}: avg={stats['avg_ms']}ms, p95={stats['p95_ms']}ms")
                all_avg_latencies.append(stats["avg_ms"])
                all_p95_latencies.append(stats["p95_ms"])
                
                self.report.add_metric(MetricResult(
                    component="Trino",
                    metric=f"query_{query_name}_avg_ms",
                    value=stats["avg_ms"],
                    unit="ms",
                ))
        
        # Overall latency metrics
        if all_avg_latencies:
            overall_avg = round(statistics.mean(all_avg_latencies), 2)
            overall_p95 = round(max(all_p95_latencies), 2)
            
            print(f"  Overall: avg={overall_avg}ms, p95={overall_p95}ms")
            
            self.report.add_metric(MetricResult(
                component="Trino",
                metric="query_overall_avg_ms",
                value=overall_avg,
                unit="ms",
                target=self.config.targets["trino_avg_latency_max_ms"],
            ).evaluate(self.config.targets["trino_avg_latency_max_ms"], "<="))
            
            self.report.add_metric(MetricResult(
                component="Trino",
                metric="query_overall_p95_ms",
                value=overall_p95,
                unit="ms",
                target=self.config.targets["trino_p95_latency_max_ms"],
            ).evaluate(self.config.targets["trino_p95_latency_max_ms"], "<="))
        
        self.conn.close()
        
        return {
            "tables": table_stats,
            "queries": query_results,
        }


# ============================================================================
# MINIO STORAGE METRICS
# ============================================================================

class MinioMetrics:
    """Collect MinIO storage metrics."""
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
        self.client = None
    
    def connect(self):
        """Initialize MinIO client."""
        self.client = Minio(
            self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=self.config.minio_secure,
        )
    
    def get_storage_usage(self) -> Dict:
        """Get storage usage by prefix/object type."""
        # Static prefixes for images
        image_prefixes = {
            "raw_images": "raw_images/",
            "bev_images": "bev_images/",
        }
        
        # Iceberg tables (Polaris appends UUIDs to table names)
        iceberg_tables = ["raw_events", "severity_scores", "potholes", "pothole_history"]
        
        usage = {}
        total_size = 0
        total_objects = 0
        
        # Get image storage
        for name, prefix in image_prefixes.items():
            try:
                objects = list(self.client.list_objects(
                    self.config.minio_bucket,
                    prefix=prefix,
                    recursive=True,
                ))
                
                size = sum(obj.size or 0 for obj in objects)
                count = len(objects)
                
                usage[name] = {
                    "size_bytes": size,
                    "size_mb": round(size / (1024 * 1024), 2),
                    "object_count": count,
                }
                
                total_size += size
                total_objects += count
                
            except Exception as e:
                self.report.add_error(f"MinIO prefix {prefix} error: {e}")
                usage[name] = {"error": str(e)}
        
        # Get all Iceberg objects under city/ and categorize by table
        try:
            all_city_objects = list(self.client.list_objects(
                self.config.minio_bucket,
                prefix="city/",
                recursive=True,
            ))
            
            # Categorize by table name (handles UUID suffixes like potholes-7ff14045...)
            iceberg_usage = {table: {"size_bytes": 0, "object_count": 0, "data_bytes": 0, "metadata_bytes": 0} for table in iceberg_tables}
            
            for obj in all_city_objects:
                obj_name = obj.object_name
                obj_size = obj.size or 0
                
                # Extract table name from path like city/potholes-uuid/data/...
                # Path format: city/<table_name>-<uuid>/data|metadata/...
                parts = obj_name.split("/")
                if len(parts) >= 2:
                    table_dir = parts[1]  # e.g., "potholes-7ff14045e35144a582da45f18ff3bdbb"
                    
                    # Match against known table names
                    for table in iceberg_tables:
                        if table_dir.startswith(f"{table}-"):
                            iceberg_usage[table]["size_bytes"] += obj_size
                            iceberg_usage[table]["object_count"] += 1
                            
                            # Track data vs metadata
                            if "/data/" in obj_name:
                                iceberg_usage[table]["data_bytes"] += obj_size
                            elif "/metadata/" in obj_name:
                                iceberg_usage[table]["metadata_bytes"] += obj_size
                            break
            
            # Add to usage dict
            iceberg_total_size = 0
            iceberg_total_count = 0
            iceberg_data_size = 0
            iceberg_metadata_size = 0
            
            for table, stats in iceberg_usage.items():
                usage[f"iceberg_{table}"] = {
                    "size_bytes": stats["size_bytes"],
                    "size_mb": round(stats["size_bytes"] / (1024 * 1024), 2),
                    "object_count": stats["object_count"],
                    "data_mb": round(stats["data_bytes"] / (1024 * 1024), 2),
                    "metadata_mb": round(stats["metadata_bytes"] / (1024 * 1024), 2),
                }
                
                iceberg_total_size += stats["size_bytes"]
                iceberg_total_count += stats["object_count"]
                iceberg_data_size += stats["data_bytes"]
                iceberg_metadata_size += stats["metadata_bytes"]
                total_size += stats["size_bytes"]
                total_objects += stats["object_count"]
            
            # Iceberg totals
            usage["iceberg_total"] = {
                "size_bytes": iceberg_total_size,
                "size_mb": round(iceberg_total_size / (1024 * 1024), 2),
                "object_count": iceberg_total_count,
                "data_mb": round(iceberg_data_size / (1024 * 1024), 2),
                "metadata_mb": round(iceberg_metadata_size / (1024 * 1024), 2),
            }
            
        except Exception as e:
            self.report.add_error(f"MinIO Iceberg listing error: {e}")
            usage["iceberg_error"] = {"error": str(e)}
        
        usage["total"] = {
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2),
            "object_count": total_objects,
        }
        
        return usage
    
    def estimate_compression_ratio(self) -> Optional[float]:
        """
        Estimate compression efficiency by comparing raw data vs Parquet.
        This is approximate - compares image count to Parquet data file sizes.
        """
        try:
            # Get raw image count (each image = 1 event)
            raw_objects = list(self.client.list_objects(
                self.config.minio_bucket,
                prefix="raw_images/",
                recursive=True,
            ))
            raw_count = len(raw_objects)
            
            # Get Parquet data sizes from Iceberg tables
            city_objects = list(self.client.list_objects(
                self.config.minio_bucket,
                prefix="city/",
                recursive=True,
            ))
            
            # Sum up all parquet files (data files, not metadata)
            parquet_size = sum(
                obj.size or 0 
                for obj in city_objects 
                if obj.object_name.endswith('.parquet') and '/data/' in obj.object_name
            )
            
            if raw_count > 0 and parquet_size > 0:
                # Rough estimate: avg 1KB metadata per event vs actual parquet
                estimated_raw = raw_count * 1024  # 1KB per event metadata
                ratio = estimated_raw / parquet_size if parquet_size > 0 else 0
                return round(ratio, 2)
            
            return None
            
        except Exception as e:
            self.report.add_error(f"Compression ratio error: {e}")
            return None
    
    def collect_all(self) -> Dict:
        """Collect all MinIO metrics."""
        print("\n[MinIO Storage Metrics]")
        self.connect()
        
        # Storage usage
        usage = self.get_storage_usage()
        print("  Storage by type:")
        for name, stats in usage.items():
            if "size_mb" in stats:
                print(f"    {name}: {stats['size_mb']} MB ({stats['object_count']} objects)")
                self.report.add_metric(MetricResult(
                    component="MinIO",
                    metric=f"storage_{name}_mb",
                    value=stats["size_mb"],
                    unit="MB",
                ))
                self.report.add_metric(MetricResult(
                    component="MinIO",
                    metric=f"objects_{name}",
                    value=stats["object_count"],
                ))
        
        # Compression ratio
        ratio = self.estimate_compression_ratio()
        if ratio:
            print(f"  Estimated compression ratio: {ratio}x")
            self.report.add_metric(MetricResult(
                component="MinIO",
                metric="compression_ratio",
                value=ratio,
            ))
        
        return {
            "usage": usage,
            "compression_ratio": ratio,
        }


# ============================================================================
# REDIS METRICS
# ============================================================================

class RedisMetrics:
    """Collect Redis cache metrics."""
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
        self.client = None
    
    def connect(self):
        """Initialize Redis client."""
        self.client = redis_lib.Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            decode_responses=True,
        )
    
    def get_stats(self) -> Dict:
        """Get Redis server statistics."""
        try:
            info = self.client.info()
            memory_info = self.client.info("memory")
            stats = self.client.info("stats")
            
            # Calculate hit rate
            hits = stats.get("keyspace_hits", 0)
            misses = stats.get("keyspace_misses", 0)
            total = hits + misses
            hit_rate = hits / total if total > 0 else 0
            
            return {
                "version": info.get("redis_version"),
                "uptime_seconds": info.get("uptime_in_seconds"),
                "connected_clients": info.get("connected_clients"),
                "used_memory_mb": round(memory_info.get("used_memory", 0) / (1024 * 1024), 2),
                "max_memory_mb": round(memory_info.get("maxmemory", 0) / (1024 * 1024), 2),
                "memory_usage_pct": round(
                    memory_info.get("used_memory", 0) / max(memory_info.get("maxmemory", 1), 1) * 100, 
                    2
                ) if memory_info.get("maxmemory", 0) > 0 else 0,
                "keyspace_hits": hits,
                "keyspace_misses": misses,
                "hit_rate": round(hit_rate, 4),
                "total_keys": sum(
                    self.client.dbsize() if hasattr(self.client, 'dbsize') else 0
                    for _ in [1]
                ),
            }
        except Exception as e:
            self.report.add_error(f"Redis stats error: {e}")
            return {}
    
    def get_key_patterns(self) -> Dict[str, int]:
        """Count keys by pattern (for cache analysis)."""
        patterns = {
            "geocoding": "osm:h3:*",
            "api_cache": "api:*",
            "summary": "api:summary*",
        }
        
        counts = {}
        for name, pattern in patterns.items():
            try:
                # Use SCAN for production safety
                cursor = 0
                count = 0
                while True:
                    cursor, keys = self.client.scan(cursor, match=pattern, count=1000)
                    count += len(keys)
                    if cursor == 0:
                        break
                counts[name] = count
            except Exception as e:
                self.report.add_error(f"Redis pattern {pattern} error: {e}")
                counts[name] = 0
        
        return counts
    
    def collect_all(self) -> Dict:
        """Collect all Redis metrics."""
        print("\n[Redis Metrics]")
        self.connect()
        
        stats = self.get_stats()
        if stats:
            print(f"  Version: {stats.get('version')}")
            print(f"  Memory: {stats.get('used_memory_mb')} MB / {stats.get('max_memory_mb')} MB")
            print(f"  Hit rate: {stats.get('hit_rate', 0) * 100:.1f}%")
            print(f"  Connected clients: {stats.get('connected_clients')}")
            
            self.report.add_metric(MetricResult(
                component="Redis",
                metric="memory_used_mb",
                value=stats.get("used_memory_mb", 0),
                unit="MB",
            ))
            
            self.report.add_metric(MetricResult(
                component="Redis",
                metric="cache_hit_rate",
                value=stats.get("hit_rate", 0),
                target=self.config.targets["redis_hit_rate_min"],
            ).evaluate(self.config.targets["redis_hit_rate_min"], ">="))
        
        # Key patterns
        key_counts = self.get_key_patterns()
        print("  Key counts by pattern:")
        for pattern, count in key_counts.items():
            print(f"    {pattern}: {count}")
            self.report.add_metric(MetricResult(
                component="Redis",
                metric=f"keys_{pattern}",
                value=count,
            ))
        
        return {
            "stats": stats,
            "key_patterns": key_counts,
        }


# ============================================================================
# DOCKER CONTAINER METRICS
# ============================================================================

class DockerMetrics:
    """Collect Docker container resource metrics."""
    
    CONTAINERS = [
        "kafka-kraft-1", "kafka-kraft-2", "kafka-kraft-3",
        "trino", "minio", "redis", "polaris",
        "schema-registry", "postgres",
    ]
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
    
    def get_container_stats(self) -> Dict[str, Dict]:
        """Get CPU and memory usage for containers."""
        stats = {}
        
        try:
            # Use docker stats command
            result = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", 
                 "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) >= 4:
                        name = parts[0]
                        cpu = parts[1].replace("%", "")
                        mem_usage = parts[2]
                        mem_pct = parts[3].replace("%", "")
                        
                        try:
                            stats[name] = {
                                "cpu_percent": float(cpu),
                                "memory_usage": mem_usage,
                                "memory_percent": float(mem_pct),
                            }
                        except ValueError:
                            pass
            else:
                self.report.add_error(f"Docker stats error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            self.report.add_error("Docker stats command timed out")
        except FileNotFoundError:
            self.report.add_error("Docker CLI not found")
        except Exception as e:
            self.report.add_error(f"Docker stats error: {e}")
        
        return stats
    
    def get_container_health(self) -> Dict[str, str]:
        """Get health status of containers."""
        health = {}
        
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) >= 2:
                        name = parts[0]
                        status = parts[1]
                        health[name] = "healthy" if "(healthy)" in status else "running" if "Up" in status else "unhealthy"
                        
        except Exception as e:
            self.report.add_error(f"Docker health check error: {e}")
        
        return health
    
    def collect_all(self) -> Dict:
        """Collect all Docker metrics."""
        print("\n[Docker Container Metrics]")
        
        # Get stats
        stats = self.get_container_stats()
        health = self.get_container_health()
        
        print("  Container resources:")
        for container in self.CONTAINERS:
            if container in stats:
                s = stats[container]
                h = health.get(container, "unknown")
                print(f"    {container}: CPU={s['cpu_percent']}%, MEM={s['memory_usage']} ({h})")
                
                self.report.add_metric(MetricResult(
                    component="Docker",
                    metric=f"cpu_{container.replace('-', '_')}",
                    value=s["cpu_percent"],
                    unit="%",
                ))
                self.report.add_metric(MetricResult(
                    component="Docker",
                    metric=f"mem_{container.replace('-', '_')}",
                    value=s["memory_percent"],
                    unit="%",
                ))
        
        # Uptime calculation
        healthy_count = sum(1 for c in self.CONTAINERS if health.get(c) in ["healthy", "running"])
        uptime_ratio = healthy_count / len(self.CONTAINERS)
        print(f"  Overall uptime: {uptime_ratio * 100:.1f}% ({healthy_count}/{len(self.CONTAINERS)} containers)")
        
        self.report.add_metric(MetricResult(
            component="Docker",
            metric="overall_uptime",
            value=round(uptime_ratio, 4),
            target=self.config.targets["uptime_min"],
        ).evaluate(self.config.targets["uptime_min"], ">="))
        
        return {
            "stats": stats,
            "health": health,
            "uptime_ratio": uptime_ratio,
        }


# ============================================================================
# END-TO-END PERFORMANCE TESTING
# ============================================================================

class EndToEndTest:
    """Measure end-to-end data freshness and completeness."""
    
    def __init__(self, config: Config, report: PerformanceReport):
        self.config = config
        self.report = report
        self.producer = None
        self.trino_conn = None
        self.test_events = []
    
    def setup(self):
        """Initialize connections."""
        self.producer = Producer({
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
        })
        
        self.trino_conn = trino.dbapi.connect(
            host=self.config.trino_host,
            port=self.config.trino_port,
            user=self.config.trino_user,
            catalog=self.config.trino_catalog,
            schema=self.config.trino_schema,
        )
    
    def generate_synthetic_event(self) -> Dict:
        """Generate a synthetic pothole detection event."""
        event_id = f"perf-test-{uuid.uuid4().hex[:12]}"
        return {
            "event_id": event_id,
            "vehicle_id": "PERF-TEST-01",
            "timestamp": int(time.time() * 1000),
            "gps_lat": 10.75 + (hash(event_id) % 1000) / 10000,
            "gps_lon": 106.60 + (hash(event_id) % 1000) / 10000,
            "gps_accuracy": 5.0,
            "raw_image_path": f"s3://warehouse/raw_images/{event_id}.jpg",
            "bev_image_path": f"s3://warehouse/bev_images/{event_id}.jpg",
            "original_mask": [[0, 0], [100, 0], [100, 100], [0, 100]],
            "bev_mask": [[0, 0], [50, 0], [50, 50], [0, 50]],
            "surface_area_cm2": 500.0,
            "detection_confidence": 0.95,
            "_created_at": time.time(),
        }
    
    def send_test_events(self, count: int) -> List[Dict]:
        """Send synthetic events to Kafka."""
        events = []
        topic = "pothole.raw.events.v1"
        
        print(f"\n[E2E Test] Sending {count} synthetic events...")
        
        for i in range(count):
            event = self.generate_synthetic_event()
            events.append(event)
            
            # Note: In production, this would use Avro serialization
            # For testing, we'll just track the events
            self.producer.produce(
                topic,
                key=event["event_id"].encode(),
                value=json.dumps(event).encode(),
            )
            
            if (i + 1) % 10 == 0:
                self.producer.flush()
                print(f"    Sent {i + 1}/{count} events")
        
        self.producer.flush()
        print(f"    All {count} events sent")
        
        return events
    
    def check_data_freshness(self, max_wait_seconds: int = 60) -> Optional[float]:
        """
        Measure time from event creation to queryable in Trino.
        Note: This requires the full pipeline to be running.
        Returns average latency in seconds, or None if no data found.
        """
        # This is a simplified check - in practice, you'd track specific test events
        print(f"  Checking data freshness (max {max_wait_seconds}s wait)...")
        
        try:
            cursor = self.trino_conn.cursor()
            
            # Check for most recent record
            cursor.execute("""
                SELECT 
                    pothole_id,
                    reported_at,
                    last_updated_at
                FROM iceberg.city.potholes
                ORDER BY last_updated_at DESC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                last_updated = row[2]
                now = datetime.now(timezone.utc)
                
                # Calculate staleness
                if hasattr(last_updated, 'tzinfo') and last_updated.tzinfo is None:
                    last_updated = last_updated.replace(tzinfo=timezone.utc)
                
                staleness_seconds = (now - last_updated).total_seconds()
                print(f"    Most recent data: {staleness_seconds:.1f}s ago")
                
                return staleness_seconds
            else:
                print("    No data in potholes table")
                return None
                
        except Exception as e:
            self.report.add_error(f"Data freshness check error: {e}")
            return None
    
    def check_data_completeness(self) -> float:
        """
        Calculate data completeness ratio.
        Compares events in raw_events to potholes (after deduplication).
        """
        try:
            cursor = self.trino_conn.cursor()
            
            # Count raw events (last 24 hours)
            cursor.execute("""
                SELECT COUNT(*) FROM iceberg.city.raw_events
                WHERE ingested_at >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
            """)
            raw_count = cursor.fetchone()[0]
            
            # Count potholes (last 24 hours)
            cursor.execute("""
                SELECT COUNT(*) FROM iceberg.city.potholes
                WHERE reported_at >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
            """)
            pothole_count = cursor.fetchone()[0]
            
            cursor.close()
            
            # Note: completeness < 1.0 is expected due to H3 deduplication
            # A "complete" system should have pothole_count > 0 if raw_count > 0
            if raw_count > 0:
                # For deduped data, we expect roughly 1 pothole per H3 cell
                # So completeness is approximated
                completeness = min(1.0, pothole_count / max(raw_count, 1))
            else:
                completeness = 1.0 if pothole_count == 0 else 0.0
            
            print(f"    Raw events (24h): {raw_count}")
            print(f"    Potholes (24h): {pothole_count}")
            print(f"    Completeness ratio: {completeness:.2%}")
            
            return completeness
            
        except Exception as e:
            self.report.add_error(f"Data completeness check error: {e}")
            return 0.0
    
    def run(self, synthetic_count: int = 0) -> Dict:
        """Run end-to-end tests."""
        print("\n[End-to-End Performance]")
        self.setup()
        
        results = {}
        
        # Send synthetic events if requested
        if synthetic_count > 0:
            events = self.send_test_events(synthetic_count)
            results["synthetic_events_sent"] = len(events)
        
        # Check data freshness
        freshness = self.check_data_freshness()
        if freshness is not None:
            results["data_freshness_seconds"] = freshness
            self.report.add_metric(MetricResult(
                component="E2E",
                metric="data_freshness_seconds",
                value=round(freshness, 2),
                unit="seconds",
            ))
        
        # Check data completeness
        completeness = self.check_data_completeness()
        results["data_completeness"] = completeness
        self.report.add_metric(MetricResult(
            component="E2E",
            metric="data_completeness",
            value=round(completeness, 4),
            target=self.config.targets["data_completeness_min"],
        ).evaluate(self.config.targets["data_completeness_min"], ">="))
        
        self.trino_conn.close()
        
        return results


# ============================================================================
# SUMMARY TABLE GENERATION
# ============================================================================

def generate_summary_table(report: PerformanceReport) -> str:
    """Generate a formatted summary table."""
    lines = [
        "",
        "=" * 80,
        "PERFORMANCE TEST SUMMARY",
        "=" * 80,
        "",
        f"{'Component':<15} {'Metric':<35} {'Value':<12} {'Target':<12} {'Status':<8}",
        "-" * 80,
    ]
    
    # Group metrics by component
    by_component = {}
    for m in report.metrics:
        if m.component not in by_component:
            by_component[m.component] = []
        by_component[m.component].append(m)
    
    # Sort and display
    for component in sorted(by_component.keys()):
        metrics = by_component[component]
        for m in metrics:
            value_str = str(m.value)
            if len(value_str) > 10:
                value_str = value_str[:10] + "..."
            
            target_str = str(m.target) if m.target is not None else "-"
            if len(target_str) > 10:
                target_str = target_str[:10] + "..."
            
            status_icon = {
                "PASS": "✓ PASS",
                "FAIL": "✗ FAIL",
                "WARN": "⚠ WARN",
                "N/A": "- N/A",
            }.get(m.status, m.status)
            
            lines.append(
                f"{m.component:<15} {m.metric:<35} {value_str:<12} {target_str:<12} {status_icon:<8}"
            )
    
    lines.append("-" * 80)
    
    # Summary stats
    total = len(report.metrics)
    passed = sum(1 for m in report.metrics if m.status == "PASS")
    failed = sum(1 for m in report.metrics if m.status == "FAIL")
    
    lines.append(f"Total: {total} metrics | Passed: {passed} | Failed: {failed} | Errors: {len(report.errors)}")
    lines.append("=" * 80)
    
    return "\n".join(lines)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_performance_tests(config: Config) -> PerformanceReport:
    """Run all performance tests and return report."""
    report = PerformanceReport()
    report.start_time = datetime.now(timezone.utc).isoformat()
    report.config = asdict(config)
    
    start_time = time.time()
    
    print("=" * 60)
    print("POTHOLE MONITORING PLATFORM - PERFORMANCE TEST")
    print("=" * 60)
    print(f"Started: {report.start_time}")
    print(f"Duration: {config.test_duration_seconds}s")
    print("=" * 60)
    
    # 1. Health checks
    print("\n[Health Checks]")
    health_checker = HealthChecker(config, report)
    health_results = health_checker.check_all()
    
    # Check if critical services are available
    if not health_results.get("Kafka"):
        report.add_error("Kafka is unavailable - skipping Kafka metrics")
    if not health_results.get("Trino"):
        report.add_error("Trino is unavailable - skipping Trino metrics")
    if not health_results.get("MinIO"):
        report.add_error("MinIO is unavailable - skipping MinIO metrics")
    if not health_results.get("Redis"):
        report.add_error("Redis is unavailable - skipping Redis metrics")
    
    # 2. Kafka metrics
    if health_results.get("Kafka"):
        try:
            kafka_metrics = KafkaMetrics(config, report)
            kafka_results = kafka_metrics.collect_all(throughput_duration=min(10, config.test_duration_seconds // 6))
        except Exception as e:
            report.add_error(f"Kafka metrics collection failed: {e}")
    
    # 3. Trino/Iceberg metrics
    if health_results.get("Trino"):
        try:
            trino_metrics = TrinoMetrics(config, report)
            trino_results = trino_metrics.collect_all(iterations=config.query_iterations)
        except Exception as e:
            report.add_error(f"Trino metrics collection failed: {e}")
    
    # 4. MinIO metrics
    if health_results.get("MinIO"):
        try:
            minio_metrics = MinioMetrics(config, report)
            minio_results = minio_metrics.collect_all()
        except Exception as e:
            report.add_error(f"MinIO metrics collection failed: {e}")
    
    # 5. Redis metrics
    if health_results.get("Redis"):
        try:
            redis_metrics = RedisMetrics(config, report)
            redis_results = redis_metrics.collect_all()
        except Exception as e:
            report.add_error(f"Redis metrics collection failed: {e}")
    
    # 6. Docker metrics
    try:
        docker_metrics = DockerMetrics(config, report)
        docker_results = docker_metrics.collect_all()
    except Exception as e:
        report.add_error(f"Docker metrics collection failed: {e}")
    
    # 7. End-to-end tests
    if health_results.get("Kafka") and health_results.get("Trino"):
        try:
            e2e_test = EndToEndTest(config, report)
            e2e_results = e2e_test.run(synthetic_count=config.synthetic_event_count)
        except Exception as e:
            report.add_error(f"E2E test failed: {e}")
    
    # Finalize report
    report.end_time = datetime.now(timezone.utc).isoformat()
    report.duration_seconds = round(time.time() - start_time, 2)
    
    # Generate summary
    passed = sum(1 for m in report.metrics if m.status == "PASS")
    failed = sum(1 for m in report.metrics if m.status == "FAIL")
    report.summary = {
        "total_metrics": len(report.metrics),
        "passed": passed,
        "failed": failed,
        "error_count": len(report.errors),
        "overall_status": "PASS" if failed == 0 and len(report.errors) == 0 else "FAIL",
    }
    
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Performance testing for Pothole Monitoring Data Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with defaults
  python performance_test.py
  
  # Custom duration and output
  python performance_test.py --duration 120 --output results.json
  
  # With synthetic events
  python performance_test.py --synthetic-events 50
  
  # From config file
  python performance_test.py --config perf_config.yaml
        """
    )
    
    parser.add_argument("--config", help="Path to YAML config file")
    parser.add_argument("--duration", type=int, help="Test duration in seconds")
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--synthetic-events", type=int, default=0, 
                        help="Number of synthetic events to generate")
    
    # Connection overrides
    parser.add_argument("--kafka-bootstrap", help="Kafka bootstrap servers")
    parser.add_argument("--trino-host", help="Trino host")
    parser.add_argument("--trino-port", type=int, help="Trino port")
    parser.add_argument("--minio-endpoint", help="MinIO endpoint")
    parser.add_argument("--redis-host", help="Redis host")
    parser.add_argument("--redis-port", type=int, help="Redis port")
    
    args = parser.parse_args()
    
    # Check dependencies before proceeding
    check_dependencies()
    
    # Load configuration
    if args.config:
        config = Config.from_yaml(args.config)
    else:
        config = Config.from_args(args)
    
    # Override synthetic events
    if args.synthetic_events:
        config.synthetic_event_count = args.synthetic_events
    
    # Run tests
    report = run_performance_tests(config)
    
    # Print summary table
    print(generate_summary_table(report))
    
    # Save to JSON if requested
    if args.output:
        report.to_json(args.output)
        print(f"\nResults saved to: {args.output}")
    
    # Print errors if any
    if report.errors:
        print("\n[Errors encountered]")
        for error in report.errors:
            print(f"  • {error}")
    
    # Exit with appropriate code
    return 0 if report.summary.get("overall_status") == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
