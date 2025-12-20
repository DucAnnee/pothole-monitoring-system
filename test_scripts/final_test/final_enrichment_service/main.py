"""
Final Enrichment Service

Consumes severity scores from Kafka, joins with raw events from Iceberg,
deduplicates potholes using H3, enriches with OSM address data (cached in Redis),
and writes to Iceberg potholes + pothole_history tables.
"""

import json
import time
import redis
import requests
import h3
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from math import radians, cos, sin, asin, sqrt

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
KAFKA_TOPIC = "pothole.severity.score.v1"
KAFKA_GROUP_ID = "final-enrichment-service-5"

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


# ============================================================================
# AVRO SCHEMA FOR DESERIALIZATION
# ============================================================================
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
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


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
        # Using a more descriptive agent and adding Referer header
        headers = {
            "User-Agent": "PotholeMonitoringSystem/1.0 (Academic Research Project; https://github.com/DucAnnee/pothole-monitoring-system.git)",
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
    1. Consumes from pothole.severity.score.v1
    2. Joins with raw_events from Iceberg
    3. Deduplicates using H3
    4. Enriches with OSM + Redis cache
    5. UPSERTs to potholes table
    6. INSERTs to pothole_history table
    """
    
    def __init__(self):
        print("[INIT] Starting Final Enrichment Service...")
        
        # Kafka consumer
        self.consumer = self._create_consumer()
        self.deserializer = self._create_deserializer()
        
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
        """Create Kafka consumer"""
        consumer_conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC])
        
        print(f"[INIT] Kafka consumer subscribed to {KAFKA_TOPIC}")
        
        return consumer
    
    def _create_deserializer(self) -> AvroDeserializer:
        """Create Avro deserializer with Schema Registry"""
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        return AvroDeserializer(
            schema_registry_client,
            SEVERITY_SCORE_AVRO_SCHEMA,
            lambda obj, ctx: obj,
        )
    
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
            severity_level VARCHAR NOT NULL COMMENT 'LOW/MEDIUM/HIGH/CRITICAL',
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
        print("FINAL ENRICHMENT SERVICE RUNNING")
        print(f"Consuming from: {KAFKA_TOPIC}")
        print(f"Consumer group: {KAFKA_GROUP_ID}")
        print("Press Ctrl+C to stop")
        print("=" * 70)
        
        processed_count = 0
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"[ERROR] Kafka error: {msg.error()}")
                        continue
                
                # Deserialize Avro message
                try:
                    severity_event = self.deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                except Exception as e:
                    print(f"[ERROR] Deserialization failed: {e}")
                    continue
                
                if severity_event:
                    try:
                        self.process_event(severity_event)
                        processed_count += 1
                    except Exception as e:
                        print(f"[ERROR] Failed to process event: {e}")
                        import traceback
                        traceback.print_exc()
                        
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
        finally:
            print(f"\n[STATS] Total events processed: {processed_count}")
            self.consumer.close()
            self.trino_conn.close()
            print("[INFO] Shutdown complete")
    
    def process_event(self, severity_event: dict):
        """
        Main processing logic:
        1. Join with raw_events from Iceberg
        2. Check if pothole exists (H3 deduplication)
        3. Geo-enrich with OSM + Redis cache
        4. UPSERT to potholes table
        5. INSERT to pothole_history
        """
        event_id = severity_event['event_id']
        
        print(f"\n{'='*70}")
        print(f"[PROCESSING] Event ID: {event_id}")
        print(f"[SEVERITY] Score: {severity_event['severity_score']:.2f}, Level: {severity_event['severity_level']}")
        
        # Step 1: Get raw event data from Iceberg
        raw_event = self._get_raw_event(event_id)
        if not raw_event:
            print(f"[ERROR] Raw event {event_id} not found in Iceberg")
            return
        
        print(f"[RAW EVENT] Vehicle: {raw_event['vehicle_id']}, GPS: ({raw_event['gps_lat']:.6f}, {raw_event['gps_lon']:.6f})")
        
        # Step 2: Check for existing pothole (H3 deduplication)
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
        
        # Step 3: Geo-enrich with OSM API + Redis cache
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
        
        # Step 4: Calculate H3 for storage (convert hex string to int for BIGINT column)
        h3_index_hex = h3.latlng_to_cell(raw_event['gps_lat'], raw_event['gps_lon'], H3_DEDUP_RESOLUTION)
        h3_index = h3.str_to_int(h3_index_hex)
        
        # Step 5: UPSERT to potholes table
        self._upsert_pothole(
            pothole_id=pothole_id,
            is_new=is_new,
            raw_event=raw_event,
            severity_event=severity_event,
            address=address,
            h3_index=h3_index,
        )
        
        # Step 6: INSERT to pothole_history
        self._insert_history(
            pothole_id=pothole_id,
            event_id=event_id,
            raw_event=raw_event,
            severity_event=severity_event,
        )
        
        print(f"[SUCCESS] Pothole {pothole_id} processed")
    
    def _get_raw_event(self, event_id: str) -> Optional[dict]:
        """Query Iceberg for raw event data"""
        query = f"""
            SELECT 
                event_id,
                vehicle_id,
                created_at,
                gps_lat,
                gps_lon,
                gps_accuracy,
                raw_image_path,
                pothole_polygon,
                detection_confidence
            FROM iceberg.city.raw_events
            WHERE event_id = '{escape_sql_string(event_id)}'
            LIMIT 1
        """
        
        try:
            cursor = self.trino_conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                columns = [desc[0] for desc in cursor.description]
                cursor.close()
                return dict(zip(columns, result))
            cursor.close()
        except Exception as e:
            print(f"[ERROR] Failed to query raw_events: {e}")
        
        return None
    
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
        reported_at = format_timestamp(raw_event['created_at'])
        calculated_at = format_timestamp(severity_event['calculated_at'])
        
        # Escape strings
        pothole_polygon = escape_sql_string(str(raw_event['pothole_polygon']))
        print(f"[DEBUG] Address: {address}")
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
            # UPDATE existing pothole (Iceberg doesn't support UPDATE, use MERGE)
            # For simplicity, we'll delete and re-insert or use Trino MERGE
            # Actually, let's just insert a note that we'd update
            # Iceberg supports row-level updates via Trino
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
        pothole_polygon = escape_sql_string(str(raw_event['pothole_polygon']))
        
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
