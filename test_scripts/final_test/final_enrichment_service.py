"""
Final Enrichment Service (v2 - Kafka Aggregation)

Consumes BOTH raw events and severity scores from Kafka,
aggregates by event_id, deduplicates potholes using H3,
enriches with OSM address data (cached in Redis),
and writes to Iceberg potholes + pothole_history tables.

Uses windowing logic to wait for both raw event and severity score
before processing, avoiding race conditions with ETL batching.
"""

import json
import time
import threading
import redis
import requests
import h3
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import trino.dbapi


# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"

# Input topics
RAW_EVENTS_TOPIC = "pothole.raw.events.v1"
SEVERITY_SCORE_TOPIC = "pothole.severity.score.v1"

KAFKA_GROUP_ID = "final-enrichment-service-v2"

TRINO_HOST = "localhost"
TRINO_PORT = 8081
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "city"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

# H3 Resolutions
H3_DEDUP_RESOLUTION = 12   # ~0.5m hexagons for pothole deduplication
H3_CACHE_RESOLUTION = 10   # ~150m hexagons for address caching

# Cache settings
CACHE_KEY_PREFIX = "osm:h3:"
CACHE_TTL_SECONDS = 2592000  # 30 days

# OSM API rate limiting
OSM_MIN_REQUEST_INTERVAL = 1.1  # seconds (1 req/sec max)

# Aggregation settings
AGGREGATION_TIMEOUT_SECONDS = 120  # Max time to wait for both events
CLEANUP_INTERVAL_SECONDS = 30  # How often to check for stale entries


# ============================================================================
# AVRO SCHEMAS FOR DESERIALIZATION
# ============================================================================
RAW_EVENT_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "raw_image_path", "type": "string"},
    {"name": "bev_image_path", "type": ["null", "string"], "default": null},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "bev_mask", "type": ["null", {"type": "array", "items": {"type": "array", "items": "double"}}], "default": null},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null}
  ]
}
"""

SEVERITY_SCORE_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "SeverityScore",
  "namespace": "pothole.severity.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "depth_cm", "type": "double"},
    {"name": "surface_area_cm2", "type": "double"},
    {"name": "severity_score", "type": "double"},
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["MINOR", "MODERATE", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


# ============================================================================
# AGGREGATION STORE (like severity_aggregator)
# ============================================================================
class EventAggregationStore:
    """
    Thread-safe store for aggregating raw events and severity scores.
    Waits for both to arrive before triggering processing.
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        self._store: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "raw_event": None,
            "severity": None,
            "created_at": time.time(),
        })
    
    def add_raw_event(self, event_id: str, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add raw event for an event_id."""
        with self._lock:
            self._store[event_id]["raw_event"] = raw_event
            return self._check_complete(event_id)
    
    def add_severity(self, event_id: str, severity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add severity score for an event_id."""
        with self._lock:
            self._store[event_id]["severity"] = severity
            return self._check_complete(event_id)
    
    def _check_complete(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Check if both raw event and severity are available.
        Returns combined data if complete, None otherwise.
        """
        entry = self._store[event_id]
        
        if entry["raw_event"] is not None and entry["severity"] is not None:
            # Both available - return combined data and remove from store
            result = {
                "event_id": event_id,
                "raw_event": entry["raw_event"],
                "severity": entry["severity"],
            }
            del self._store[event_id]
            return result
        
        return None
    
    def cleanup_stale(self, max_age_seconds: float) -> int:
        """Remove entries older than max_age_seconds. Returns count removed."""
        current_time = time.time()
        removed = 0
        
        with self._lock:
            stale_keys = [
                k for k, v in self._store.items()
                if current_time - v["created_at"] > max_age_seconds
            ]
            
            for key in stale_keys:
                entry = self._store[key]
                # Log what we're dropping
                has_raw = entry["raw_event"] is not None
                has_sev = entry["severity"] is not None
                print(f"[CLEANUP] Dropping {key}: raw={has_raw}, severity={has_sev}")
                del self._store[key]
                removed += 1
        
        return removed
    
    def size(self) -> int:
        """Return number of pending aggregations."""
        with self._lock:
            return len(self._store)
    
    def stats(self) -> Dict[str, int]:
        """Return stats about pending entries."""
        with self._lock:
            raw_only = sum(1 for v in self._store.values() if v["raw_event"] and not v["severity"])
            sev_only = sum(1 for v in self._store.values() if v["severity"] and not v["raw_event"])
            return {"raw_only": raw_only, "severity_only": sev_only, "total": len(self._store)}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in meters between two GPS points using Haversine formula"""
    R = 6371000  # Earth radius in meters
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    return R * c


def format_timestamp(ts_value) -> str:
    """Convert timestamp value to Trino-compatible string format."""
    if isinstance(ts_value, datetime):
        return ts_value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    elif isinstance(ts_value, (int, float)):
        dt = datetime.fromtimestamp(ts_value / 1000, tz=timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    else:
        return str(ts_value)


def escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL strings."""
    if value is None:
        return None
    return value.replace("'", "''")


def mask_to_polygon_string(mask: list) -> str:
    """
    Convert original_mask (list of [x, y] points) to a JSON string for storage.
    The mask is stored as-is since it represents pixel coordinates.
    In production, this could be converted to GeoJSON if needed.
    """
    if mask is None:
        return "[]"
    return json.dumps(mask)


def find_existing_pothole(
    trino_conn,
    lat: float,
    lon: float,
    h3_resolution: int = H3_DEDUP_RESOLUTION
) -> Optional[str]:
    """
    Check if pothole already exists within same H3 cell.
    
    Uses H3 resolution 12 (~0.5m diameter hexagons).
    If same H3 index + within 2 meters = probably same pothole.
    """
    h3_index_hex = h3.latlng_to_cell(lat, lon, h3_resolution)
    h3_index_int = h3.str_to_int(h3_index_hex)
    
    query = f"""
        SELECT pothole_id, gps_lat, gps_lon
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.potholes
        WHERE geom_h3 = {h3_index_int}
          AND status IN ('reported', 'in_progress')
        ORDER BY reported_at DESC
        LIMIT 1
    """
    
    try:
        cursor = trino_conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            pothole_id, existing_lat, existing_lon = result
            distance = haversine_distance(lat, lon, existing_lat, existing_lon)
            
            if distance < 2.0:  # within 2 meters
                print(f"[DEDUP] Found existing pothole {pothole_id} at {distance:.2f}m distance")
                return pothole_id
    except Exception as e:
        # Table might not exist yet, that's OK
        print(f"[WARN] Could not check for existing pothole: {e}")
    
    return None


# ============================================================================
# OSM GEOCODER WITH REDIS CACHE
# ============================================================================

class OSMGeocoder:
    """OpenStreetMap reverse geocoding with H3-based Redis caching"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.cache_ttl = CACHE_TTL_SECONDS
        self.last_request_time = 0
        self.min_request_interval = OSM_MIN_REQUEST_INTERVAL
    
    def reverse_geocode(
        self, 
        lat: float, 
        lon: float,
        h3_res: int = H3_CACHE_RESOLUTION
    ) -> Optional[Dict[str, str]]:
        """
        Get address from lat/lon with H3-based caching.
        
        Uses H3 resolution 10 (~150m hexagons) for address cache
        since addresses don't change within a city block.
        """
        # Get H3 index for cache key
        h3_index_hex = h3.latlng_to_cell(lat, lon, h3_res)
        cache_key = f"{CACHE_KEY_PREFIX}{h3_index_hex}"
        
        # Try cache first
        try:
            cached = self.redis.get(cache_key)
            if cached:
                print(f"[CACHE HIT] H3: {h3_index_hex}")
                return json.loads(cached)
        except Exception as e:
            print(f"[WARN] Redis cache read failed: {e}")
        
        print(f"[CACHE MISS] H3: {h3_index_hex}, calling OSM API...")
        
        # Rate limiting
        self._rate_limit()
        
        # Call OSM Nominatim API
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1,
        }
        # OSM requires a valid User-Agent with real contact info
        headers = {
            "User-Agent": "PotholeMonitoringSystem/1.0 (Academic Research Project; https://github.com/pothole-monitoring)",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            # Extract address components
            address = data.get("address", {})
            result = {
                "city": address.get("city") or address.get("town") or "Ho Chi Minh City",
                "ward": address.get("suburb") or address.get("neighbourhood"),
                "district": address.get("city_district") or address.get("county"),
                "street_name": address.get("road"),
                "road_id": str(data.get("osm_id")) if data.get("osm_id") else None,
                "cached_at": time.time(),
            }
            
            # Cache it
            try:
                self.redis.setex(
                    cache_key,
                    self.cache_ttl,
                    json.dumps(result)
                )
            except Exception as e:
                print(f"[WARN] Redis cache write failed: {e}")
            
            print(f"[OSM API] Geocoded: {result.get('street_name')}, {result.get('ward')}, {result.get('district')}")
            
            return result
            
        except requests.RequestException as e:
            print(f"[ERROR] OSM API failed: {e}")
            return None
    
    def _rate_limit(self):
        """Enforce 1 request per second to OSM Nominatim"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            print(f"[RATE LIMIT] Sleeping {sleep_time:.2f}s...")
            time.sleep(sleep_time)
        self.last_request_time = time.time()


# ============================================================================
# FINAL ENRICHMENT SERVICE
# ============================================================================

class FinalEnrichmentService:
    """
    Main service that:
    1. Consumes from BOTH pothole.raw.events.v1 AND pothole.severity.score.v1
    2. Aggregates by event_id (waits for both to arrive)
    3. Deduplicates using H3
    4. Enriches with OSM + Redis cache
    5. UPSERTs to potholes table
    6. INSERTs to pothole_history table
    """
    
    def __init__(self):
        print("[INIT] Starting Final Enrichment Service (v2 - Kafka aggregation)...")
        
        # Kafka consumer and deserializers
        self.consumer = self._create_consumer()
        self.raw_deserializer, self.severity_deserializer = self._create_deserializers()
        
        # Aggregation store
        self.aggregation_store = EventAggregationStore()
        
        # Trino connection
        self.trino_conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )
        print(f"[INIT] Connected to Trino at {TRINO_HOST}:{TRINO_PORT}")
        
        # Redis for caching
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
        )
        print(f"[INIT] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        
        # OSM geocoder
        self.geocoder = OSMGeocoder(self.redis_client)
        
        # Ensure tables exist
        self._ensure_tables_exist()
    
    def _create_consumer(self) -> Consumer:
        """Create Kafka consumer subscribing to both topics"""
        consumer_conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([RAW_EVENTS_TOPIC, SEVERITY_SCORE_TOPIC])
        
        print(f"[INIT] Kafka consumer subscribed to {RAW_EVENTS_TOPIC} and {SEVERITY_SCORE_TOPIC}")
        
        return consumer
    
    def _create_deserializers(self):
        """Create Avro deserializers for both topics"""
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        raw_deserializer = AvroDeserializer(
            schema_registry_client,
            RAW_EVENT_AVRO_SCHEMA,
            lambda obj, ctx: obj,
        )
        
        severity_deserializer = AvroDeserializer(
            schema_registry_client,
            SEVERITY_SCORE_AVRO_SCHEMA,
            lambda obj, ctx: obj,
        )
        
        return raw_deserializer, severity_deserializer
    
    def _ensure_tables_exist(self):
        """Create tables if they don't exist"""
        cursor = self.trino_conn.cursor()
        
        # Create namespace first
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.city")
            cursor.fetchall()
            print("[INIT] Ensured schema iceberg.city exists")
        except Exception as e:
            print(f"[WARN] Schema creation: {e}")
        
        # Create potholes table
        potholes_ddl = """
        CREATE TABLE IF NOT EXISTS iceberg.city.potholes (
            pothole_id VARCHAR NOT NULL COMMENT 'UUID for this pothole',
            first_event_id VARCHAR NOT NULL COMMENT 'Event that first detected this pothole',
            reported_at TIMESTAMP(3) NOT NULL COMMENT 'First detection timestamp',
            gps_lat DOUBLE NOT NULL COMMENT 'Latitude',
            gps_lon DOUBLE NOT NULL COMMENT 'Longitude',
            geom_h3 BIGINT NOT NULL COMMENT 'H3 index at resolution 12',
            city VARCHAR COMMENT 'City name',
            ward VARCHAR COMMENT 'Ward',
            district VARCHAR COMMENT 'District',
            street_name VARCHAR COMMENT 'Street name',
            road_id VARCHAR COMMENT 'OSM way_id or road identifier',
            depth_cm DOUBLE NOT NULL COMMENT 'Latest depth in centimeters',
            surface_area_cm2 DOUBLE NOT NULL COMMENT 'Latest surface area',
            severity_score DOUBLE NOT NULL COMMENT 'Latest severity',
            severity_level VARCHAR NOT NULL COMMENT 'MINOR/MODERATE/HIGH/CRITICAL',
            pothole_polygon VARCHAR NOT NULL COMMENT 'Latest GeoJSON polygon',
            status VARCHAR NOT NULL COMMENT 'reported | in_progress | fixed',
            in_progress_at TIMESTAMP(3) COMMENT 'When repair started',
            fixed_at TIMESTAMP(3) COMMENT 'When repair completed',
            last_updated_at TIMESTAMP(3) NOT NULL COMMENT 'Last UPSERT timestamp',
            observation_count INTEGER NOT NULL COMMENT 'How many times seen'
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['month(reported_at)']
        )
        """
        
        # Create pothole_history table
        history_ddl = """
        CREATE TABLE IF NOT EXISTS iceberg.city.pothole_history (
            observation_id VARCHAR NOT NULL COMMENT 'UUID for this observation',
            pothole_id VARCHAR NOT NULL COMMENT 'Links to potholes table',
            event_id VARCHAR NOT NULL COMMENT 'Links to raw_events',
            recorded_at TIMESTAMP(3) NOT NULL COMMENT 'Observation timestamp',
            depth_cm DOUBLE NOT NULL COMMENT 'Depth at observation',
            surface_area_cm2 DOUBLE NOT NULL COMMENT 'Surface area at observation',
            severity_score DOUBLE NOT NULL COMMENT 'Severity at observation',
            severity_level VARCHAR NOT NULL COMMENT 'Severity level at observation',
            gps_lat DOUBLE NOT NULL COMMENT 'Lat at observation',
            gps_lon DOUBLE NOT NULL COMMENT 'Lon at observation',
            pothole_polygon VARCHAR NOT NULL COMMENT 'Polygon at observation',
            status VARCHAR NOT NULL COMMENT 'Status at this observation'
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['day(recorded_at)']
        )
        """
        
        try:
            cursor.execute(potholes_ddl)
            cursor.fetchall()
            print("[INIT] Ensured potholes table exists")
        except Exception as e:
            print(f"[WARN] Error creating potholes table: {e}")
        
        try:
            cursor.execute(history_ddl)
            cursor.fetchall()
            print("[INIT] Ensured pothole_history table exists")
        except Exception as e:
            print(f"[WARN] Error creating pothole_history table: {e}")
        
        cursor.close()
    
    def run(self):
        """Main consumer loop"""
        print("=" * 70)
        print("FINAL ENRICHMENT SERVICE (v2 - Kafka Aggregation)")
        print("=" * 70)
        print(f"Consuming from: {RAW_EVENTS_TOPIC}, {SEVERITY_SCORE_TOPIC}")
        print(f"Consumer group: {KAFKA_GROUP_ID}")
        print(f"Aggregation timeout: {AGGREGATION_TIMEOUT_SECONDS}s")
        print("Press Ctrl+C to stop")
        print("=" * 70)
        
        raw_count = 0
        severity_count = 0
        processed_count = 0
        last_cleanup = time.time()
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                # Periodic cleanup of stale entries
                if time.time() - last_cleanup > CLEANUP_INTERVAL_SECONDS:
                    removed = self.aggregation_store.cleanup_stale(AGGREGATION_TIMEOUT_SECONDS)
                    if removed > 0:
                        print(f"[CLEANUP] Removed {removed} stale entries")
                    stats = self.aggregation_store.stats()
                    if stats["total"] > 0:
                        print(f"[PENDING] raw_only={stats['raw_only']}, severity_only={stats['severity_only']}")
                    last_cleanup = time.time()
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"[ERROR] Kafka error: {msg.error()}")
                        continue
                
                topic = msg.topic()
                combined_data = None
                
                try:
                    if topic == RAW_EVENTS_TOPIC:
                        # Deserialize raw event
                        record = self.raw_deserializer(
                            msg.value(),
                            SerializationContext(RAW_EVENTS_TOPIC, MessageField.VALUE)
                        )
                        
                        if record:
                            event_id = record["event_id"]
                            raw_count += 1
                            print(f"[RAW #{raw_count}] event_id={event_id}, vehicle={record['vehicle_id']}")
                            
                            # Add to store and check if complete
                            combined_data = self.aggregation_store.add_raw_event(event_id, record)
                    
                    elif topic == SEVERITY_SCORE_TOPIC:
                        # Deserialize severity score
                        record = self.severity_deserializer(
                            msg.value(),
                            SerializationContext(SEVERITY_SCORE_TOPIC, MessageField.VALUE)
                        )
                        
                        if record:
                            event_id = record["event_id"]
                            severity_count += 1
                            print(f"[SEVERITY #{severity_count}] event_id={event_id}, score={record['severity_score']:.2f}")
                            
                            # Add to store and check if complete
                            combined_data = self.aggregation_store.add_severity(event_id, record)
                    
                    # If we have both raw event and severity, process
                    if combined_data:
                        try:
                            self.process_combined_event(combined_data)
                            processed_count += 1
                        except Exception as e:
                            print(f"[ERROR] Failed to process event: {e}")
                            import traceback
                            traceback.print_exc()
                
                except Exception as e:
                    print(f"[ERROR] Deserialization failed: {e}")
                    continue
                        
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
        finally:
            print(f"\n[STATS] Raw events received: {raw_count}")
            print(f"[STATS] Severity scores received: {severity_count}")
            print(f"[STATS] Potholes processed: {processed_count}")
            print(f"[STATS] Pending aggregations: {self.aggregation_store.size()}")
            self.consumer.close()
            self.trino_conn.close()
            print("[INFO] Shutdown complete")
    
    def process_combined_event(self, combined_data: Dict[str, Any]):
        """
        Process a complete event (raw + severity).
        
        1. Check if pothole exists (H3 deduplication)
        2. Geo-enrich with OSM + Redis cache
        3. UPSERT to potholes table
        4. INSERT to pothole_history
        """
        event_id = combined_data["event_id"]
        raw_event = combined_data["raw_event"]
        severity_event = combined_data["severity"]
        
        print(f"\n{'='*70}")
        print(f"[PROCESSING] Event ID: {event_id}")
        print(f"[RAW] Vehicle: {raw_event['vehicle_id']}, GPS: ({raw_event['gps_lat']:.6f}, {raw_event['gps_lon']:.6f})")
        print(f"[SEVERITY] Score: {severity_event['severity_score']:.2f}, Level: {severity_event['severity_level']}")
        
        # Step 1: Check for existing pothole (H3 deduplication)
        existing_pothole_id = find_existing_pothole(
            self.trino_conn,
            raw_event['gps_lat'],
            raw_event['gps_lon'],
            h3_resolution=H3_DEDUP_RESOLUTION
        )
        
        if existing_pothole_id:
            pothole_id = existing_pothole_id
            is_new = False
        else:
            pothole_id = str(uuid4())
            print(f"[NEW POTHOLE] Created ID: {pothole_id}")
            is_new = True
        
        # Step 2: Geo-enrich with OSM API + Redis cache
        address = self.geocoder.reverse_geocode(
            raw_event['gps_lat'],
            raw_event['gps_lon'],
            h3_res=H3_CACHE_RESOLUTION
        )
        
        if not address:
            print(f"[WARN] Geocoding failed, using nulls for address")
            address = {
                "city": None,
                "ward": None,
                "district": None,
                "street_name": None,
                "road_id": None,
            }
        
        # Step 3: Calculate H3 for storage (convert hex string to int for BIGINT column)
        h3_index_hex = h3.latlng_to_cell(raw_event['gps_lat'], raw_event['gps_lon'], H3_DEDUP_RESOLUTION)
        h3_index = h3.str_to_int(h3_index_hex)
        
        # Step 4: UPSERT to potholes table
        self._upsert_pothole(
            pothole_id=pothole_id,
            is_new=is_new,
            raw_event=raw_event,
            severity_event=severity_event,
            address=address,
            h3_index=h3_index,
        )
        
        # Step 5: INSERT to pothole_history
        self._insert_history(
            pothole_id=pothole_id,
            event_id=event_id,
            raw_event=raw_event,
            severity_event=severity_event,
        )
        
        print(f"[SUCCESS] Pothole {pothole_id} processed")
    
    def _upsert_pothole(
        self,
        pothole_id: str,
        is_new: bool,
        raw_event: dict,
        severity_event: dict,
        address: dict,
        h3_index: int,
    ):
        """UPSERT to potholes table"""
        cursor = self.trino_conn.cursor()
        
        # Format timestamps
        reported_at = format_timestamp(raw_event['timestamp'])
        calculated_at = format_timestamp(severity_event['calculated_at'])
        
        # Convert mask to polygon string for storage
        pothole_polygon = escape_sql_string(mask_to_polygon_string(raw_event['original_mask']))
        city = escape_sql_string(address.get('city')) if address.get('city') else None
        ward = escape_sql_string(address.get('ward')) if address.get('ward') else None
        district = escape_sql_string(address.get('district')) if address.get('district') else None
        street_name = escape_sql_string(address.get('street_name')) if address.get('street_name') else None
        road_id = escape_sql_string(address.get('road_id')) if address.get('road_id') else None
        
        if is_new:
            # INSERT new pothole
            query = f"""
                INSERT INTO iceberg.city.potholes (
                    pothole_id,
                    first_event_id,
                    reported_at,
                    gps_lat,
                    gps_lon,
                    geom_h3,
                    city,
                    ward,
                    district,
                    street_name,
                    road_id,
                    depth_cm,
                    surface_area_cm2,
                    severity_score,
                    severity_level,
                    pothole_polygon,
                    status,
                    last_updated_at,
                    observation_count
                ) VALUES (
                    '{pothole_id}',
                    '{raw_event['event_id']}',
                    TIMESTAMP '{reported_at}',
                    {raw_event['gps_lat']},
                    {raw_event['gps_lon']},
                    {h3_index},
                    {f"'{city}'" if city else 'NULL'},
                    {f"'{ward}'" if ward else 'NULL'},
                    {f"'{district}'" if district else 'NULL'},
                    {f"'{street_name}'" if street_name else 'NULL'},
                    {f"'{road_id}'" if road_id else 'NULL'},
                    {severity_event['depth_cm']},
                    {severity_event['surface_area_cm2']},
                    {severity_event['severity_score']},
                    '{severity_event['severity_level']}',
                    '{pothole_polygon}',
                    'reported',
                    TIMESTAMP '{calculated_at}',
                    1
                )
            """
            
            try:
                cursor.execute(query)
                cursor.fetchall()
                print(f"[INSERT] Created new pothole {pothole_id}")
            except Exception as e:
                print(f"[ERROR] Failed to insert pothole: {e}")
        
        else:
            # UPDATE existing pothole
            query = f"""
                UPDATE iceberg.city.potholes
                SET 
                    depth_cm = {severity_event['depth_cm']},
                    surface_area_cm2 = {severity_event['surface_area_cm2']},
                    severity_score = {severity_event['severity_score']},
                    severity_level = '{severity_event['severity_level']}',
                    pothole_polygon = '{pothole_polygon}',
                    last_updated_at = TIMESTAMP '{calculated_at}',
                    observation_count = observation_count + 1
                WHERE pothole_id = '{pothole_id}'
            """
            
            try:
                cursor.execute(query)
                cursor.fetchall()
                print(f"[UPDATE] Updated existing pothole {pothole_id}")
            except Exception as e:
                print(f"[ERROR] Failed to update pothole: {e}")
        
        cursor.close()
    
    def _insert_history(
        self,
        pothole_id: str,
        event_id: str,
        raw_event: dict,
        severity_event: dict,
    ):
        """INSERT to pothole_history table"""
        cursor = self.trino_conn.cursor()
        
        observation_id = str(uuid4())
        recorded_at = format_timestamp(severity_event['calculated_at'])
        pothole_polygon = escape_sql_string(mask_to_polygon_string(raw_event['original_mask']))
        
        query = f"""
            INSERT INTO iceberg.city.pothole_history (
                observation_id,
                pothole_id,
                event_id,
                recorded_at,
                depth_cm,
                surface_area_cm2,
                severity_score,
                severity_level,
                gps_lat,
                gps_lon,
                pothole_polygon,
                status
            ) VALUES (
                '{observation_id}',
                '{pothole_id}',
                '{event_id}',
                TIMESTAMP '{recorded_at}',
                {severity_event['depth_cm']},
                {severity_event['surface_area_cm2']},
                {severity_event['severity_score']},
                '{severity_event['severity_level']}',
                {raw_event['gps_lat']},
                {raw_event['gps_lon']},
                '{pothole_polygon}',
                'reported'
            )
        """
        
        try:
            cursor.execute(query)
            cursor.fetchall()
            print(f"[HISTORY] Recorded observation {observation_id}")
        except Exception as e:
            print(f"[ERROR] Failed to insert history: {e}")
        
        cursor.close()


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    service = FinalEnrichmentService()
    service.run()
