"""
Pipeline Latency Tracker

Tracks end-to-end latency from edge detection through to data availability.

Latency Stages:
1. Edge Detection (timestamp) → Kafka Producer (edge_to_kafka_ms)
2. Kafka → Raw Events Iceberg (kafka_to_storage_ms)  
3. Raw Event → Severity Calculated (depth_estimation_ms)
4. Combined Event → Potholes Table (enrichment_ms)
5. Total: Edge → Queryable in Trino (total_pipeline_ms)

This module provides:
- LatencyTracker class for calculating stage latencies
- Functions to store latency metrics in Redis for real-time monitoring
- Integration with existing services
"""

import time
import json
import redis
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from collections import deque
import threading


@dataclass
class PipelineLatencyEvent:
    """Latency metrics for a single event through the pipeline."""
    event_id: str
    
    # Stage timestamps (epoch milliseconds)
    edge_detected_at: int  # When pothole detected at edge device
    kafka_produced_at: Optional[int] = None  # When sent to Kafka
    raw_event_ingested_at: Optional[int] = None  # When stored in raw_events table
    severity_calculated_at: Optional[int] = None  # When depth/severity done
    pothole_stored_at: Optional[int] = None  # When stored in potholes table
    
    # Calculated latencies (milliseconds)
    edge_to_kafka_ms: Optional[int] = None
    kafka_to_raw_storage_ms: Optional[int] = None
    raw_to_severity_ms: Optional[int] = None
    severity_to_pothole_ms: Optional[int] = None
    total_pipeline_ms: Optional[int] = None
    
    def calculate_latencies(self) -> 'PipelineLatencyEvent':
        """Calculate all stage latencies from timestamps."""
        if self.kafka_produced_at and self.edge_detected_at:
            self.edge_to_kafka_ms = self.kafka_produced_at - self.edge_detected_at
        
        if self.raw_event_ingested_at and self.kafka_produced_at:
            self.kafka_to_raw_storage_ms = self.raw_event_ingested_at - self.kafka_produced_at
        
        if self.severity_calculated_at and self.edge_detected_at:
            self.raw_to_severity_ms = self.severity_calculated_at - self.edge_detected_at
        
        if self.pothole_stored_at and self.severity_calculated_at:
            self.severity_to_pothole_ms = self.pothole_stored_at - self.severity_calculated_at
        
        if self.pothole_stored_at and self.edge_detected_at:
            self.total_pipeline_ms = self.pothole_stored_at - self.edge_detected_at
        
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class LatencyTracker:
    """
    Tracks pipeline latency metrics and stores in Redis for real-time monitoring.
    
    Uses Redis to:
    1. Store recent latency events (last N events)
    2. Maintain running statistics (avg, p50, p95, p99)
    3. Track per-stage latencies
    """
    
    # Redis keys
    RECENT_EVENTS_KEY = "latency:events:recent"
    STATS_KEY = "latency:stats"
    STAGE_STATS_PREFIX = "latency:stage:"
    
    # Configuration
    MAX_RECENT_EVENTS = 100
    STATS_TTL_SECONDS = 300  # 5 minutes
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self._client: Optional[redis.Redis] = None
        self._local_events: deque = deque(maxlen=self.MAX_RECENT_EVENTS)
        self._lock = threading.Lock()
    
    def _get_client(self) -> Optional[redis.Redis]:
        """Get or create Redis client with lazy initialization."""
        if self._client is None:
            try:
                self._client = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    decode_responses=True,
                    socket_timeout=2,
                )
                self._client.ping()
            except Exception as e:
                print(f"[LATENCY] Redis connection failed: {e}")
                self._client = None
        return self._client
    
    def record_event(self, event: PipelineLatencyEvent) -> bool:
        """
        Record a latency event.
        
        Args:
            event: PipelineLatencyEvent with timestamps filled in
            
        Returns:
            True if successfully stored
        """
        # Calculate latencies
        event.calculate_latencies()
        
        # Store locally (always works)
        with self._lock:
            self._local_events.append(event.to_dict())
        
        # Try to store in Redis
        client = self._get_client()
        if client is None:
            return False
        
        try:
            event_json = json.dumps(event.to_dict())
            
            # Add to recent events list (LPUSH + LTRIM for sliding window)
            pipe = client.pipeline()
            pipe.lpush(self.RECENT_EVENTS_KEY, event_json)
            pipe.ltrim(self.RECENT_EVENTS_KEY, 0, self.MAX_RECENT_EVENTS - 1)
            pipe.expire(self.RECENT_EVENTS_KEY, self.STATS_TTL_SECONDS * 2)
            pipe.execute()
            
            # Update statistics
            self._update_stats(client, event)
            
            return True
            
        except Exception as e:
            print(f"[LATENCY] Failed to record event: {e}")
            return False
    
    def _update_stats(self, client: redis.Redis, event: PipelineLatencyEvent):
        """Update running statistics in Redis."""
        now_ms = int(time.time() * 1000)
        
        stats_data = {
            "last_updated": now_ms,
            "last_event_id": event.event_id,
            "last_total_ms": event.total_pipeline_ms or 0,
        }
        
        # Update stage-specific stats
        stages = [
            ("edge_to_kafka", event.edge_to_kafka_ms),
            ("kafka_to_storage", event.kafka_to_raw_storage_ms),
            ("depth_estimation", event.raw_to_severity_ms),
            ("enrichment", event.severity_to_pothole_ms),
            ("total", event.total_pipeline_ms),
        ]
        
        pipe = client.pipeline()
        
        for stage_name, latency_ms in stages:
            if latency_ms is not None:
                # Add to sorted set for percentile calculation
                stage_key = f"{self.STAGE_STATS_PREFIX}{stage_name}"
                pipe.zadd(stage_key, {f"{event.event_id}:{now_ms}": latency_ms})
                # Keep only recent entries (by score/timestamp is tricky, use count)
                pipe.zremrangebyrank(stage_key, 0, -(self.MAX_RECENT_EVENTS + 1))
                pipe.expire(stage_key, self.STATS_TTL_SECONDS * 2)
        
        # Update main stats hash
        pipe.hset(self.STATS_KEY, mapping=stats_data)
        pipe.expire(self.STATS_KEY, self.STATS_TTL_SECONDS)
        
        pipe.execute()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current latency statistics.
        
        Returns dict with:
        - stages: per-stage latency stats (avg, p50, p95, p99)
        - recent_events: last N events
        - summary: overall statistics
        """
        client = self._get_client()
        
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stages": {},
            "recent_events": [],
            "summary": {},
        }
        
        if client is None:
            # Fallback to local data
            with self._lock:
                result["recent_events"] = list(self._local_events)
            result["summary"]["source"] = "local"
            return result
        
        try:
            # Get recent events
            recent_json = client.lrange(self.RECENT_EVENTS_KEY, 0, 20)
            result["recent_events"] = [json.loads(e) for e in recent_json]
            
            # Get stage statistics
            stages = ["edge_to_kafka", "kafka_to_storage", "depth_estimation", "enrichment", "total"]
            
            for stage_name in stages:
                stage_key = f"{self.STAGE_STATS_PREFIX}{stage_name}"
                
                # Get all values from sorted set
                values = client.zrange(stage_key, 0, -1, withscores=True)
                
                if values:
                    latencies = sorted([v[1] for v in values])
                    n = len(latencies)
                    
                    result["stages"][stage_name] = {
                        "count": n,
                        "avg_ms": round(sum(latencies) / n, 2),
                        "min_ms": round(min(latencies), 2),
                        "max_ms": round(max(latencies), 2),
                        "p50_ms": round(latencies[int(n * 0.5)], 2),
                        "p95_ms": round(latencies[int(n * 0.95)] if n >= 20 else latencies[-1], 2),
                        "p99_ms": round(latencies[int(n * 0.99)] if n >= 100 else latencies[-1], 2),
                    }
            
            # Get summary
            main_stats = client.hgetall(self.STATS_KEY)
            if main_stats:
                result["summary"] = {
                    "last_updated": main_stats.get("last_updated"),
                    "last_event_id": main_stats.get("last_event_id"),
                    "last_total_ms": int(main_stats.get("last_total_ms", 0)),
                    "source": "redis",
                }
            
            return result
            
        except Exception as e:
            print(f"[LATENCY] Failed to get stats: {e}")
            result["error"] = str(e)
            return result
    
    def get_recent_latencies(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get the most recent latency events."""
        client = self._get_client()
        
        if client is None:
            with self._lock:
                return list(self._local_events)[-count:]
        
        try:
            recent_json = client.lrange(self.RECENT_EVENTS_KEY, 0, count - 1)
            return [json.loads(e) for e in recent_json]
        except Exception as e:
            print(f"[LATENCY] Failed to get recent: {e}")
            with self._lock:
                return list(self._local_events)[-count:]


# Global tracker instance (lazy initialization)
_tracker: Optional[LatencyTracker] = None


def get_tracker(redis_host: str = "localhost", redis_port: int = 6379) -> LatencyTracker:
    """Get or create global latency tracker."""
    global _tracker
    if _tracker is None:
        _tracker = LatencyTracker(redis_host, redis_port)
    return _tracker


def record_pipeline_latency(
    event_id: str,
    edge_detected_at: int,
    kafka_produced_at: Optional[int] = None,
    raw_event_ingested_at: Optional[int] = None,
    severity_calculated_at: Optional[int] = None,
    pothole_stored_at: Optional[int] = None,
    redis_host: str = "localhost",
    redis_port: int = 6379,
) -> PipelineLatencyEvent:
    """
    Convenience function to record pipeline latency.
    
    Args:
        event_id: The event identifier
        edge_detected_at: Timestamp (ms) when detected at edge
        kafka_produced_at: Timestamp (ms) when sent to Kafka
        raw_event_ingested_at: Timestamp (ms) when stored in raw_events
        severity_calculated_at: Timestamp (ms) when severity calculated
        pothole_stored_at: Timestamp (ms) when stored in potholes table
        
    Returns:
        PipelineLatencyEvent with calculated latencies
    """
    event = PipelineLatencyEvent(
        event_id=event_id,
        edge_detected_at=edge_detected_at,
        kafka_produced_at=kafka_produced_at,
        raw_event_ingested_at=raw_event_ingested_at,
        severity_calculated_at=severity_calculated_at,
        pothole_stored_at=pothole_stored_at,
    )
    
    tracker = get_tracker(redis_host, redis_port)
    tracker.record_event(event)
    
    return event


# For testing/demonstration
if __name__ == "__main__":
    import random
    
    print("Testing LatencyTracker...")
    tracker = LatencyTracker()
    
    # Simulate 10 events
    for i in range(10):
        now = int(time.time() * 1000)
        
        event = PipelineLatencyEvent(
            event_id=f"test-event-{i}",
            edge_detected_at=now - random.randint(5000, 10000),
            kafka_produced_at=now - random.randint(4000, 5000),
            raw_event_ingested_at=now - random.randint(3000, 4000),
            severity_calculated_at=now - random.randint(1000, 3000),
            pothole_stored_at=now,
        )
        
        tracker.record_event(event)
        print(f"  Event {i}: total={event.total_pipeline_ms}ms")
    
    # Get stats
    print("\nLatency Statistics:")
    stats = tracker.get_stats()
    
    for stage, data in stats.get("stages", {}).items():
        print(f"  {stage}: avg={data['avg_ms']}ms, p95={data['p95_ms']}ms")
    
    print(f"\nRecent events: {len(stats.get('recent_events', []))}")
