import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any
import sys

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
KAFKA_SOURCE_TOPIC = "pothole.raw.events.v1"
KAFKA_DLQ_TOPIC = "pothole.raw.events.v1.dlq"

BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
CONSUMER_GROUP_ID = "pothole-iceberg-etl"

# Polaris/Iceberg config
POLARIS_URL = "http://localhost:8181/api/catalog/"
POLARIS_CREDENTIALS = "root:s3cr3t"
POLARIS_WAREHOUSE = "warehouse"
ICEBERG_CATALOG_NAME = "iceberg"
ICEBERG_NAMESPACE = "city"
ICEBERG_TABLE_NAME = "raw_events"

# Trino config
TRINO_HOST = "localhost"
TRINO_PORT = 8081
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"

# Batching config
BATCH_SIZE = 100  # Write after this many messages
BATCH_TIMEOUT_SECONDS = 30  # Or after this many seconds

RAW_EVENT_SCHEMA_STR = """
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "image_path", "type": "string"},
    {"name": "detection_confidence", "type": ["null","double"], "default": null}
  ]
}
"""

# SQL for table creation
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS iceberg.city.raw_events (
    event_id VARCHAR COMMENT 'Unique event identifier',
    vehicle_id VARCHAR COMMENT 'Identifier of the vehicle generating the event',
    created_at TIMESTAMP COMMENT 'Original event timestamp from Kafka',
    gps_lat DOUBLE COMMENT 'Latitude reported by the device',
    gps_lon DOUBLE COMMENT 'Longitude reported by the device',
    gps_accuracy DOUBLE COMMENT 'Reported GPS accuracy in meters',
    raw_image_path VARCHAR COMMENT 'S3 URI of the event image',
    detection_confidence DOUBLE COMMENT 'Model confidence score (optional)',
    ingested_at TIMESTAMP COMMENT 'Timestamp when the event was ingested into this table of the data lake'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
)
"""

CREATE_NAMESPACE_SQL = "CREATE SCHEMA IF NOT EXISTS iceberg.city"


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
            print(f"[INFO] Executing SQL: {sql[:100]}...")
            cursor.execute(sql)
            cursor.fetchall()  # Consume results
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
    
    def table_exists(self, namespace: str, table: str) -> bool:
        """Check if a table exists."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW TABLES IN {self.catalog}.{namespace}")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return table in tables
        except Exception as e:
            print(f"[ERROR] Failed to check table: {e}")
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
        # Refresh token before it expires (default is ~1 hour, refresh at 50 minutes)
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


def ensure_table_exists(catalog: RestCatalog, trino_mgr: TrinoManager) -> bool:
    """Check if table exists, create if missing using Trino."""
    table_identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
    
    # First check via PyIceberg
    try:
        table = catalog.load_table(table_identifier)
        print(f"[SUCCESS] Table {table_identifier} exists")
        return True
    except NoSuchTableError:
        print(f"[WARN] Table {table_identifier} does not exist")
    
    # Table doesn't exist, create via Trino
    print("[INFO] Creating table via Trino...")
    
    # First ensure namespace exists
    if not trino_mgr.namespace_exists(ICEBERG_NAMESPACE):
        print(f"[INFO] Creating namespace {ICEBERG_NAMESPACE}...")
        if not trino_mgr.execute_ddl(CREATE_NAMESPACE_SQL):
            print("[ERROR] Failed to create namespace")
            return False
        print(f"[SUCCESS] Created namespace {ICEBERG_NAMESPACE}")
    else:
        print(f"[INFO] Namespace {ICEBERG_NAMESPACE} already exists")
    
    # Create the table
    if not trino_mgr.execute_ddl(CREATE_TABLE_SQL):
        print("[ERROR] Failed to create table")
        return False
    
    print(f"[SUCCESS] Created table {table_identifier}")
    
    # Verify it now exists
    time.sleep(2)  # Give Polaris a moment to register the table
    try:
        catalog.load_table(table_identifier)
        print(f"[SUCCESS] Verified table {table_identifier} is accessible")
        return True
    except NoSuchTableError:
        print(f"[ERROR] Table created but not accessible via PyIceberg")
        return False


# ============================================================================
# KAFKA SETUP
# ============================================================================
def create_consumer() -> Consumer:
    """Create Kafka consumer with Avro deserialization."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Manual commit after Iceberg write
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_SOURCE_TOPIC])
    print(f"[SUCCESS] Subscribed to {KAFKA_SOURCE_TOPIC}")
    
    return consumer


def create_dlq_producer() -> Producer:
    """Create Kafka producer for dead letter queue."""
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    
    return Producer(producer_conf)


def create_avro_deserializer() -> AvroDeserializer:
    """Create Avro deserializer with Schema Registry."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    return AvroDeserializer(
        schema_registry_client,
        RAW_EVENT_SCHEMA_STR,
        lambda obj, ctx: obj,  # Return dict as-is
    )


def create_dlq_serializer() -> AvroSerializer:
    """Create Avro serializer for DLQ messages."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # DLQ schema includes error info
    dlq_schema = """
    {
      "type": "record",
      "name": "DeadLetterRecord",
      "namespace": "pothole.dlq.v1",
      "fields": [
        {"name": "original_topic", "type": "string"},
        {"name": "original_partition", "type": "int"},
        {"name": "original_offset", "type": "long"},
        {"name": "error_message", "type": "string"},
        {"name": "error_timestamp", "type": "string"},
        {"name": "raw_value", "type": "bytes"}
      ]
    }
    """
    
    return AvroSerializer(
        schema_registry_client,
        dlq_schema,
        lambda obj, ctx: obj,
    )


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================
def transform_to_iceberg_record(avro_record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform Avro record to Iceberg table schema."""
    now = datetime.now(timezone.utc)
    
    # Parse timestamp string to datetime
    created_at = datetime.fromisoformat(avro_record['timestamp'].replace('Z', '+00:00'))
    
    return {
        "event_id": avro_record['event_id'],
        "vehicle_id": avro_record['vehicle_id'],
        "created_at": created_at,
        "gps_lat": avro_record['gps_lat'],
        "gps_lon": avro_record['gps_lon'],
        "gps_accuracy": avro_record.get('gps_accuracy'),
        "raw_image_path": avro_record['image_path'],
        "detection_confidence": avro_record.get('detection_confidence'),
        "ingested_at": now,
    }


# ============================================================================
# BATCH PROCESSOR
# ============================================================================
class BatchProcessor:
    """Handles batching and writing to Iceberg."""
    
    def __init__(self, catalog: RestCatalog, consumer: Consumer, 
                 dlq_producer: Producer, dlq_serializer: AvroSerializer):
        self.catalog = catalog
        self.consumer = consumer
        self.dlq_producer = dlq_producer
        self.dlq_serializer = dlq_serializer
        
        self.batch: List[Dict[str, Any]] = []
        self.batch_messages: List[Any] = []  # Store Kafka messages for commit
        self.batch_start_time = time.time()
        
        table_identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
        self.table = catalog.load_table(table_identifier)
    
    def add_record(self, record: Dict[str, Any], kafka_msg: Any):
        """Add record to batch."""
        self.batch.append(record)
        self.batch_messages.append(kafka_msg)
    
    def should_flush(self) -> bool:
        """Check if batch should be flushed."""
        if len(self.batch) >= BATCH_SIZE:
            return True
        
        if time.time() - self.batch_start_time >= BATCH_TIMEOUT_SECONDS:
            return True
        
        return False
    
    def flush(self):
        """Write batch to Iceberg and commit offsets."""
        if not self.batch:
            return
        
        try:
            print(f"\n[INFO] Writing batch of {len(self.batch)} records to Iceberg...")
            
            # Write to Iceberg using PyIceberg
            with self.table.update_spec() as update:
                pass  # Schema is already set
            
            # Append data
            self.table.append(self.batch)
            
            print(f"[SUCCESS] Wrote {len(self.batch)} records to Iceberg")
            
            # Commit Kafka offsets after successful write
            if self.batch_messages:
                last_msg = self.batch_messages[-1]
                self.consumer.commit(message=last_msg)
                print(f"[SUCCESS] Committed Kafka offset: partition={last_msg.partition()}, offset={last_msg.offset()}")
            
            # Reset batch
            self.batch.clear()
            self.batch_messages.clear()
            self.batch_start_time = time.time()
            
        except Exception as e:
            print(f"[ERROR] Failed to write batch to Iceberg: {e}")
            # Don't clear batch - let's retry or handle differently
            raise
    
    def send_to_dlq(self, kafka_msg: Any, error: str):
        """Send failed message to dead letter queue."""
        try:
            dlq_record = {
                "original_topic": kafka_msg.topic(),
                "original_partition": kafka_msg.partition(),
                "original_offset": kafka_msg.offset(),
                "error_message": str(error),
                "error_timestamp": datetime.now(timezone.utc).isoformat(),
                "raw_value": kafka_msg.value(),
            }
            
            serialized_value = self.dlq_serializer(
                dlq_record,
                SerializationContext(KAFKA_DLQ_TOPIC, MessageField.VALUE),
            )
            
            self.dlq_producer.produce(
                topic=KAFKA_DLQ_TOPIC,
                value=serialized_value,
            )
            self.dlq_producer.poll(0)
            
            print(f"[DLQ] Sent message to dead letter queue: {error}")
            
        except Exception as dlq_error:
            print(f"[ERROR] Failed to send to DLQ: {dlq_error}")


# ============================================================================
# MAIN ETL LOOP
# ============================================================================
def main():
    print("=" * 70)
    print("KAFKA → ICEBERG ETL PIPELINE")
    print("=" * 70)
    
    # Initialize Trino connection
    trino_mgr = TrinoManager(TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG)
    if not trino_mgr.connect():
        print("[FATAL] Could not connect to Trino. Exiting.")
        sys.exit(1)
    
    # Initialize Polaris/Iceberg
    token_manager = PolarisTokenManager(POLARIS_URL, POLARIS_CREDENTIALS)
    catalog = create_iceberg_catalog(token_manager)
    
    # Ensure table exists (create if needed)
    if not ensure_table_exists(catalog, trino_mgr):
        print("[FATAL] Table setup failed. Exiting.")
        trino_mgr.close()
        sys.exit(1)
    
    # Initialize Kafka
    consumer = create_consumer()
    dlq_producer = create_dlq_producer()
    avro_deserializer = create_avro_deserializer()
    dlq_serializer = create_dlq_serializer()
    
    batch_processor = BatchProcessor(catalog, consumer, dlq_producer, dlq_serializer)
    
    print(f"\n[INFO] Starting ETL pipeline...")
    print(f"[INFO] Batch size: {BATCH_SIZE} messages")
    print(f"[INFO] Batch timeout: {BATCH_TIMEOUT_SECONDS} seconds")
    print(f"[INFO] Press Ctrl+C to stop.\n")
    
    try:
        message_count = 0
        
        while True:
            msg = consumer.poll(1.0)
            
            # Check if we should flush based on timeout
            if batch_processor.should_flush():
                batch_processor.flush()
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue
            
            # Deserialize message
            try:
                raw_event = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
                
                # Transform to Iceberg schema
                iceberg_record = transform_to_iceberg_record(raw_event)
                
                # Add to batch
                batch_processor.add_record(iceberg_record, msg)
                message_count += 1
                
                print(f"[RECEIVED] Message {message_count}: event_id={raw_event['event_id']}")
                
                # Check if batch is full
                if batch_processor.should_flush():
                    batch_processor.flush()
                
            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                batch_processor.send_to_dlq(msg, str(e))
                # Commit the failed message offset so we don't reprocess it
                consumer.commit(message=msg)
    
    except KeyboardInterrupt:
        print("\n[INFO] Shutdown signal received")
    
    finally:
        # Flush any remaining records
        if batch_processor.batch:
            print("[INFO] Flushing remaining records...")
            try:
                batch_processor.flush()
            except Exception as e:
                print(f"[ERROR] Failed to flush final batch: {e}")
        
        print("[INFO] Closing consumer...")
        consumer.close()
        
        print("[INFO] Flushing DLQ producer...")
        dlq_producer.flush()
        
        print("[INFO] Closing Trino connection...")
        trino_mgr.close()
        
        print("[INFO] ETL pipeline stopped")


if __name__ == "__main__":
    main()