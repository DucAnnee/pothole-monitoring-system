"""
Generalized Kafka → Iceberg ETL Microservice

Reads from configurable Kafka topics and writes to corresponding Iceberg tables.
Currently supports:
- pothole.raw.events.v1 → iceberg.city.raw_events
- pothole.severity.score.v1 → iceberg.city.severity_scores
"""

import json
import time
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass

import pyarrow as pa
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
import requests
import trino

# ============================================================================
# CONFIGURATION
# ============================================================================
BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
CONSUMER_GROUP_ID = "pothole-iceberg-etl"

# Polaris/Iceberg config
POLARIS_URL = "http://localhost:8181/api/catalog/"
POLARIS_CREDENTIALS = "root:s3cr3t"
POLARIS_WAREHOUSE = "warehouse"
ICEBERG_CATALOG_NAME = "iceberg"
ICEBERG_NAMESPACE = "city"

# Trino config
TRINO_HOST = "localhost"
TRINO_PORT = 8081
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"

# Batching config
BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 30

# DLQ topic suffix
DLQ_SUFFIX = ".dlq"

# ============================================================================
# AVRO SCHEMAS
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
    {"name": "image_path", "type": "string"},
    {"name": "pothole_polygon", "type": "string"},
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
    {"name": "severity_level", "type": {"type": "enum", "name": "SeverityLevel", "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]}},
    {"name": "calculated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

# ============================================================================
# PYARROW SCHEMAS (for Iceberg tables)
# Note: nullable=False for required fields to match Iceberg table schema
# Note: PyIceberg requires microsecond precision ("us") for timestamp transforms
# ============================================================================
RAW_EVENTS_ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("vehicle_id", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us"), nullable=False),
    pa.field("gps_lat", pa.float64(), nullable=False),
    pa.field("gps_lon", pa.float64(), nullable=False),
    pa.field("gps_accuracy", pa.float64(), nullable=True),  # Optional
    pa.field("raw_image_path", pa.string(), nullable=False),
    pa.field("pothole_polygon", pa.string(), nullable=False),
    pa.field("detection_confidence", pa.float64(), nullable=True),  # Optional
    pa.field("ingested_at", pa.timestamp("us"), nullable=False),
])

SEVERITY_SCORES_ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("depth_cm", pa.float64(), nullable=False),
    pa.field("surface_area_cm2", pa.float64(), nullable=False),
    pa.field("severity_score", pa.float64(), nullable=False),
    pa.field("severity_level", pa.string(), nullable=False),
    pa.field("calculated_at", pa.timestamp("us"), nullable=False),
])

# ============================================================================
# TRINO DDL STATEMENTS
# ============================================================================
CREATE_NAMESPACE_SQL = "CREATE SCHEMA IF NOT EXISTS iceberg.city"

CREATE_RAW_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.city.raw_events (
    event_id VARCHAR NOT NULL COMMENT 'Unique event identifier',
    vehicle_id VARCHAR NOT NULL COMMENT 'Vehicle identifier',
    created_at TIMESTAMP(3) NOT NULL COMMENT 'Event timestamp from device',
    
    gps_lat DOUBLE NOT NULL COMMENT 'Latitude',
    gps_lon DOUBLE NOT NULL COMMENT 'Longitude',
    gps_accuracy DOUBLE COMMENT 'GPS accuracy in meters',
    
    raw_image_path VARCHAR NOT NULL COMMENT 'S3 URI (s3://warehouse/raw_images/{event_id}.jpg)',
    pothole_polygon VARCHAR NOT NULL COMMENT 'GeoJSON polygon from edge detection',
    detection_confidence DOUBLE COMMENT 'Edge model confidence score',
    
    ingested_at TIMESTAMP(3) NOT NULL COMMENT 'When ingested into Iceberg'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
)
"""

CREATE_SEVERITY_SCORES_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.city.severity_scores (
    event_id VARCHAR NOT NULL COMMENT 'Links to raw_events',
    
    depth_cm DOUBLE NOT NULL COMMENT 'Estimated depth in centimeters',
    surface_area_cm2 DOUBLE NOT NULL COMMENT 'Estimated surface area',
    severity_score DOUBLE NOT NULL COMMENT 'Calculated severity (0-1 scale)',
    severity_level VARCHAR NOT NULL COMMENT 'LOW/MEDIUM/HIGH/CRITICAL',
    
    calculated_at TIMESTAMP(3) NOT NULL COMMENT 'When severity was calculated'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(calculated_at)']
)
"""

# ============================================================================
# TRANSFORMATION FUNCTIONS
# ============================================================================
def _convert_timestamp(ts_value) -> datetime:
    """
    Convert timestamp value to timezone-naive datetime.
    Handles both:
    - datetime objects (auto-converted by Avro deserializer from timestamp-millis)
    - int/long milliseconds since epoch
    """
    if isinstance(ts_value, datetime):
        # Already a datetime (Avro deserializer converted it)
        return ts_value.replace(tzinfo=None)
    else:
        # It's milliseconds since epoch
        return datetime.fromtimestamp(ts_value / 1000, tz=timezone.utc).replace(tzinfo=None)


def transform_raw_event(avro_record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw event from Kafka to Iceberg schema."""
    now = datetime.now(timezone.utc)
    
    # Convert timestamp (handles both datetime and long formats)
    created_at = _convert_timestamp(avro_record['timestamp'])
    
    return {
        "event_id": avro_record['event_id'],
        "vehicle_id": avro_record['vehicle_id'],
        "created_at": created_at,
        "gps_lat": avro_record['gps_lat'],
        "gps_lon": avro_record['gps_lon'],
        "gps_accuracy": avro_record.get('gps_accuracy'),
        "raw_image_path": avro_record['image_path'],
        "pothole_polygon": avro_record['pothole_polygon'],
        "detection_confidence": avro_record.get('detection_confidence'),
        "ingested_at": now.replace(tzinfo=None),
    }


def transform_severity_score(avro_record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform severity score from Kafka to Iceberg schema."""
    # Convert timestamp (handles both datetime and long formats)
    calculated_at = _convert_timestamp(avro_record['calculated_at'])
    
    return {
        "event_id": avro_record['event_id'],
        "depth_cm": avro_record['depth_cm'],
        "surface_area_cm2": avro_record['surface_area_cm2'],
        "severity_score": avro_record['severity_score'],
        "severity_level": avro_record['severity_level'],
        "calculated_at": calculated_at,
    }


# ============================================================================
# TOPIC-TABLE MAPPING CONFIGURATION
# ============================================================================
@dataclass
class TopicTableMapping:
    """Configuration for a Kafka topic to Iceberg table mapping."""
    kafka_topic: str
    iceberg_table: str
    avro_schema: str
    arrow_schema: pa.Schema
    create_table_sql: str
    transform_fn: Callable[[Dict[str, Any]], Dict[str, Any]]
    
    @property
    def dlq_topic(self) -> str:
        return self.kafka_topic + DLQ_SUFFIX


# Define all topic-table mappings
TOPIC_TABLE_MAPPINGS: List[TopicTableMapping] = [
    TopicTableMapping(
        kafka_topic="pothole.raw.events.v1",
        iceberg_table="raw_events",
        avro_schema=RAW_EVENT_AVRO_SCHEMA,
        arrow_schema=RAW_EVENTS_ARROW_SCHEMA,
        create_table_sql=CREATE_RAW_EVENTS_SQL,
        transform_fn=transform_raw_event,
    ),
    TopicTableMapping(
        kafka_topic="pothole.severity.score.v1",
        iceberg_table="severity_scores",
        avro_schema=SEVERITY_SCORE_AVRO_SCHEMA,
        arrow_schema=SEVERITY_SCORES_ARROW_SCHEMA,
        create_table_sql=CREATE_SEVERITY_SCORES_SQL,
        transform_fn=transform_severity_score,
    ),
]

# Create lookup dict for quick access
TOPIC_TO_MAPPING: Dict[str, TopicTableMapping] = {
    m.kafka_topic: m for m in TOPIC_TABLE_MAPPINGS
}


# ============================================================================
# TRINO CONNECTION
# ============================================================================
class TrinoManager:
    """Manages Trino connections and DDL operations."""
    
    def __init__(self, host: str, port: int, user: str, catalog: str):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.connection = None
    
    def connect(self, max_retries=5, delay=2):
        """Connect to Trino with retry logic."""
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[INFO] Connecting to Trino (attempt {attempt})...")
                
                self.connection = trino.dbapi.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                )
                
                # Test connection
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.close()
                
                print(f"[SUCCESS] Connected to Trino at {self.host}:{self.port}")
                return True
                
            except Exception as e:
                print(f"[ERROR] Trino connection failed: {e}")
                if attempt < max_retries:
                    print(f"[INFO] Retrying in {delay} seconds...")
                    time.sleep(delay)
        
        print("[FATAL] Could not connect to Trino after multiple attempts")
        return False
    
    def execute_ddl(self, sql: str) -> bool:
        """Execute DDL statement."""
        try:
            cursor = self.connection.cursor()
            print(f"[INFO] Executing SQL: {sql[:80]}...")
            cursor.execute(sql)
            cursor.fetchall()
            cursor.close()
            print("[SUCCESS] SQL executed successfully")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to execute SQL: {e}")
            return False
    
    def namespace_exists(self, namespace: str) -> bool:
        """Check if a namespace (schema) exists."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW SCHEMAS IN {self.catalog}")
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return namespace in schemas
        except Exception as e:
            print(f"[ERROR] Failed to check namespace: {e}")
            return False
    
    def close(self):
        """Close Trino connection."""
        if self.connection:
            self.connection.close()
            print("[INFO] Trino connection closed")


# ============================================================================
# POLARIS TOKEN MANAGEMENT
# ============================================================================
class PolarisTokenManager:
    """Manages OAuth2 token for Polaris catalog."""
    
    def __init__(self, polaris_url: str, credentials: str):
        self.polaris_url = polaris_url.rstrip('/api/catalog/') + '/api/catalog/v1/oauth/tokens'
        self.client_id, self.client_secret = credentials.split(':')
        self.token = None
        self.token_expires_at = 0
    
    def get_token(self) -> str:
        """Get valid token, refreshing if necessary."""
        if self.token and time.time() < self.token_expires_at:
            return self.token
        
        print("[INFO] Obtaining Polaris access token...")
        
        response = requests.post(
            self.polaris_url,
            auth=(self.client_id, self.client_secret),
            headers={"Polaris-Realm": "POLARIS"},
            data={
                "grant_type": "client_credentials",
                "scope": "PRINCIPAL_ROLE:ALL"
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to obtain Polaris token: {response.text}")
        
        token_data = response.json()
        self.token = token_data['access_token']
        self.token_expires_at = time.time() + 3000
        
        print("[SUCCESS] Obtained Polaris access token")
        return self.token


# ============================================================================
# ICEBERG CATALOG MANAGEMENT
# ============================================================================
def create_iceberg_catalog(token_manager: PolarisTokenManager) -> RestCatalog:
    """Create and configure Iceberg REST catalog."""
    print("[INFO] Initializing Iceberg catalog...")
    
    catalog = RestCatalog(
        name=ICEBERG_CATALOG_NAME,
        **{
            "uri": POLARIS_URL,
            "warehouse": POLARIS_WAREHOUSE,
            "credential": POLARIS_CREDENTIALS,
            "scope": "PRINCIPAL_ROLE:ALL",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-west-2",
            "s3.path-style-access": "true",
        }
    )
    
    print("[SUCCESS] Iceberg catalog initialized")
    return catalog


def ensure_tables_exist(catalog: RestCatalog, trino_mgr: TrinoManager) -> bool:
    """Ensure all required Iceberg tables exist."""
    
    # First ensure namespace exists
    if not trino_mgr.namespace_exists(ICEBERG_NAMESPACE):
        print(f"[INFO] Creating namespace {ICEBERG_NAMESPACE}...")
        if not trino_mgr.execute_ddl(CREATE_NAMESPACE_SQL):
            print("[ERROR] Failed to create namespace")
            return False
        print(f"[SUCCESS] Created namespace {ICEBERG_NAMESPACE}")
    else:
        print(f"[INFO] Namespace {ICEBERG_NAMESPACE} already exists")
    
    # Check/create each table
    for mapping in TOPIC_TABLE_MAPPINGS:
        table_identifier = f"{ICEBERG_NAMESPACE}.{mapping.iceberg_table}"
        
        try:
            catalog.load_table(table_identifier)
            print(f"[SUCCESS] Table {table_identifier} exists")
        except NoSuchTableError:
            print(f"[WARN] Table {table_identifier} does not exist, creating...")
            
            if not trino_mgr.execute_ddl(mapping.create_table_sql):
                print(f"[ERROR] Failed to create table {table_identifier}")
                return False
            
            # Verify creation
            time.sleep(2)
            try:
                catalog.load_table(table_identifier)
                print(f"[SUCCESS] Created and verified table {table_identifier}")
            except NoSuchTableError:
                print(f"[ERROR] Table {table_identifier} created but not accessible")
                return False
    
    return True


# ============================================================================
# KAFKA SETUP
# ============================================================================
def create_consumer(topics: List[str]) -> Consumer:
    """Create Kafka consumer subscribed to multiple topics."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)
    print(f"[SUCCESS] Subscribed to topics: {', '.join(topics)}")
    
    return consumer


def create_dlq_producer() -> Producer:
    """Create Kafka producer for dead letter queues."""
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def create_deserializers(mappings: List[TopicTableMapping]) -> Dict[str, AvroDeserializer]:
    """Create Avro deserializers for all topics."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    deserializers = {}
    for mapping in mappings:
        deserializers[mapping.kafka_topic] = AvroDeserializer(
            schema_registry_client,
            mapping.avro_schema,
            lambda obj, ctx: obj,
        )
    
    return deserializers


# ============================================================================
# BATCH PROCESSOR
# ============================================================================
class MultiBatchProcessor:
    """Handles batching and writing to multiple Iceberg tables."""
    
    def __init__(
        self,
        catalog: RestCatalog,
        consumer: Consumer,
        dlq_producer: Producer,
        mappings: List[TopicTableMapping],
    ):
        self.catalog = catalog
        self.consumer = consumer
        self.dlq_producer = dlq_producer
        
        # Initialize batches and tables for each mapping
        self.batches: Dict[str, List[Dict[str, Any]]] = {}
        self.batch_messages: Dict[str, List[Any]] = {}
        self.tables: Dict[str, Any] = {}
        self.mappings: Dict[str, TopicTableMapping] = {}
        
        for mapping in mappings:
            topic = mapping.kafka_topic
            self.batches[topic] = []
            self.batch_messages[topic] = []
            self.mappings[topic] = mapping
            
            # Load Iceberg table
            table_id = f"{ICEBERG_NAMESPACE}.{mapping.iceberg_table}"
            self.tables[topic] = catalog.load_table(table_id)
        
        self.batch_start_time = time.time()
    
    def add_record(self, topic: str, record: Dict[str, Any], kafka_msg: Any):
        """Add record to the appropriate batch."""
        self.batches[topic].append(record)
        self.batch_messages[topic].append(kafka_msg)
    
    def should_flush(self) -> bool:
        """Check if any batch should be flushed."""
        # Check total batch size across all topics
        total_records = sum(len(batch) for batch in self.batches.values())
        if total_records >= BATCH_SIZE:
            return True
        
        # Check timeout
        if time.time() - self.batch_start_time >= BATCH_TIMEOUT_SECONDS:
            return True
        
        return False
    
    def flush_all(self):
        """Flush all batches to their respective Iceberg tables."""
        for topic in self.batches:
            self._flush_topic(topic)
        
        # Reset timer
        self.batch_start_time = time.time()
    
    def _flush_topic(self, topic: str):
        """Flush a specific topic's batch to Iceberg."""
        batch = self.batches[topic]
        if not batch:
            return
        
        mapping = self.mappings[topic]
        table = self.tables[topic]
        
        try:
            print(f"\n[INFO] Writing {len(batch)} records to {mapping.iceberg_table}...")
            
            # Convert to PyArrow Table
            arrow_table = pa.Table.from_pylist(batch, schema=mapping.arrow_schema)
            
            # Append to Iceberg
            table.append(arrow_table)
            
            print(f"[SUCCESS] Wrote {len(batch)} records to {mapping.iceberg_table}")
            
            # Commit Kafka offsets
            if self.batch_messages[topic]:
                last_msg = self.batch_messages[topic][-1]
                self.consumer.commit(message=last_msg)
                print(f"[SUCCESS] Committed offset for {topic}: partition={last_msg.partition()}, offset={last_msg.offset()}")
            
            # Clear batch
            self.batches[topic].clear()
            self.batch_messages[topic].clear()
            
        except Exception as e:
            print(f"[ERROR] Failed to write to {mapping.iceberg_table}: {e}")
            raise
    
    def send_to_dlq(self, topic: str, kafka_msg: Any, error: str):
        """Send failed message to dead letter queue."""
        mapping = self.mappings.get(topic)
        if not mapping:
            print(f"[ERROR] Unknown topic {topic}, cannot send to DLQ")
            return
        
        try:
            dlq_record = {
                "original_topic": kafka_msg.topic(),
                "original_partition": kafka_msg.partition(),
                "original_offset": kafka_msg.offset(),
                "error_message": str(error),
                "error_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            self.dlq_producer.produce(
                topic=mapping.dlq_topic,
                value=json.dumps(dlq_record).encode('utf-8'),
            )
            self.dlq_producer.poll(0)
            
            print(f"[DLQ] Sent message to {mapping.dlq_topic}: {error}")
            
        except Exception as dlq_error:
            print(f"[ERROR] Failed to send to DLQ: {dlq_error}")
    
    def get_pending_count(self) -> int:
        """Get total number of pending records across all batches."""
        return sum(len(batch) for batch in self.batches.values())


# ============================================================================
# MAIN ETL LOOP
# ============================================================================
def main():
    print("=" * 70)
    print("GENERALIZED KAFKA → ICEBERG ETL PIPELINE")
    print("=" * 70)
    
    # Print configured mappings
    print("\n[CONFIG] Topic-Table Mappings:")
    for mapping in TOPIC_TABLE_MAPPINGS:
        print(f"  - {mapping.kafka_topic} → iceberg.{ICEBERG_NAMESPACE}.{mapping.iceberg_table}")
    print()
    
    # Initialize Trino connection
    trino_mgr = TrinoManager(TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG)
    if not trino_mgr.connect():
        print("[FATAL] Could not connect to Trino. Exiting.")
        sys.exit(1)
    
    # Initialize Polaris/Iceberg
    token_manager = PolarisTokenManager(POLARIS_URL, POLARIS_CREDENTIALS)
    catalog = create_iceberg_catalog(token_manager)
    
    # Ensure all tables exist
    if not ensure_tables_exist(catalog, trino_mgr):
        print("[FATAL] Table setup failed. Exiting.")
        trino_mgr.close()
        sys.exit(1)
    
    # Initialize Kafka
    topics = [m.kafka_topic for m in TOPIC_TABLE_MAPPINGS]
    consumer = create_consumer(topics)
    dlq_producer = create_dlq_producer()
    deserializers = create_deserializers(TOPIC_TABLE_MAPPINGS)
    
    # Initialize batch processor
    batch_processor = MultiBatchProcessor(
        catalog, consumer, dlq_producer, TOPIC_TABLE_MAPPINGS
    )
    
    print(f"\n[INFO] Starting ETL pipeline...")
    print(f"[INFO] Batch size: {BATCH_SIZE} messages")
    print(f"[INFO] Batch timeout: {BATCH_TIMEOUT_SECONDS} seconds")
    print(f"[INFO] Press Ctrl+C to stop.\n")
    
    message_counts: Dict[str, int] = {m.kafka_topic: 0 for m in TOPIC_TABLE_MAPPINGS}
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            # Check if we should flush based on timeout
            if batch_processor.should_flush():
                batch_processor.flush_all()
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue
            
            topic = msg.topic()
            
            # Get the appropriate deserializer and mapping
            if topic not in deserializers:
                print(f"[WARN] Unknown topic {topic}, skipping")
                continue
            
            deserializer = deserializers[topic]
            mapping = TOPIC_TO_MAPPING[topic]
            
            try:
                # Deserialize message
                avro_record = deserializer(
                    msg.value(),
                    SerializationContext(topic, MessageField.VALUE),
                )
                
                if avro_record is None:
                    continue
                
                # Transform to Iceberg schema
                iceberg_record = mapping.transform_fn(avro_record)
                
                # Add to batch
                batch_processor.add_record(topic, iceberg_record, msg)
                message_counts[topic] += 1
                
                # Get a readable ID for logging
                event_id = avro_record.get('event_id', 'unknown')
                print(f"[RECEIVED] {topic} #{message_counts[topic]}: event_id={event_id}")
                
                # Check if batch is full
                if batch_processor.should_flush():
                    batch_processor.flush_all()
                
            except Exception as e:
                print(f"[ERROR] Failed to process message from {topic}: {e}")
                batch_processor.send_to_dlq(topic, msg, str(e))
                continue
    
    except KeyboardInterrupt:
        print("\n[INFO] Shutdown signal received")
    
    finally:
        # Flush any remaining records
        pending = batch_processor.get_pending_count()
        if pending > 0:
            print(f"[INFO] Flushing {pending} remaining records...")
            try:
                batch_processor.flush_all()
            except Exception as e:
                print(f"[ERROR] Failed to flush remaining records: {e}")
        
        # Print final stats
        print("\n[STATS] Messages processed:")
        for topic, count in message_counts.items():
            print(f"  - {topic}: {count}")
        
        print("[INFO] Closing consumer...")
        consumer.close()
        
        print("[INFO] Flushing DLQ producer...")
        dlq_producer.flush()
        
        print("[INFO] Closing Trino connection...")
        trino_mgr.close()
        
        print("[INFO] ETL pipeline stopped")


if __name__ == "__main__":
    main()
