"""
Geo Enrichment Service.

Consumes pothole.raw.events.v2, reverse-geocodes gps_lat/gps_lon via OSM
Nominatim (Redis H3-cached), publishes pothole.geo.enriched.v1 so the Flink
projection can fill district/ward/road on serving.current_road_defects.

Env-driven (no config.yaml): KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL,
REDIS_HOST, REDIS_PORT, plus optional topic/group overrides.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import h3
import redis
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

try:
    from cloud.shared.dlq import send_to_dlq
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from shared.dlq import send_to_dlq

SERVICE_NAME = "geo-enrichment"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9094,kafka-2:9094,kafka-3:9094")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "pothole.raw.events.v2")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "pothole.geo.enriched.v1")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "pothole.geo.enriched.dlq.v1")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "geo-enrichment-v1")

OSM_MIN_REQUEST_INTERVAL = float(os.environ.get("OSM_MIN_REQUEST_INTERVAL", "1.0"))
CACHE_TTL_SECONDS = int(os.environ.get("GEO_CACHE_TTL_SECONDS", str(30 * 24 * 3600)))
H3_CACHE_RESOLUTION = int(os.environ.get("H3_CACHE_RESOLUTION", "10"))
CACHE_KEY_PREFIX = "geocode:"


RAW_EVENT_SCHEMA_STR = """
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "pothole.raw.v2",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "vehicle_id", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "gps_lat", "type": "double"},
    {"name": "gps_lon", "type": "double"},
    {"name": "gps_accuracy", "type": ["null", "double"], "default": null},
    {"name": "raw_image_object_key", "type": "string"},
    {"name": "original_mask", "type": {"type": "array", "items": {"type": "array", "items": "double"}}},
    {"name": "detection_confidence", "type": ["null", "double"], "default": null}
  ]
}
"""

GEO_ENRICHED_SCHEMA_STR = """
{
  "type": "record",
  "name": "GeoEnriched",
  "namespace": "pothole.geo.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "city", "type": ["null", "string"], "default": null},
    {"name": "district", "type": ["null", "string"], "default": null},
    {"name": "ward", "type": ["null", "string"], "default": null},
    {"name": "road_segment_id", "type": ["null", "string"], "default": null},
    {"name": "geocoded_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""


class OSMGeocoder:
    """OpenStreetMap reverse geocoding with H3-based Redis caching."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.cache_ttl = CACHE_TTL_SECONDS
        self.last_request_time = 0.0
        self.min_request_interval = OSM_MIN_REQUEST_INTERVAL

    def reverse_geocode(self, lat: float, lon: float, h3_res: int = H3_CACHE_RESOLUTION) -> Optional[Dict[str, str]]:
        h3_index_hex = h3.latlng_to_cell(lat, lon, h3_res)
        cache_key = f"{CACHE_KEY_PREFIX}{h3_index_hex}"

        try:
            cached = self.redis.get(cache_key)
            if cached:
                print(f"[CACHE HIT] H3: {h3_index_hex}")
                return json.loads(cached)
        except Exception as e:
            print(f"[WARN] Redis cache read failed: {e}")

        print(f"[CACHE MISS] H3: {h3_index_hex}, calling OSM API...")
        self._rate_limit()

        url = "https://nominatim.openstreetmap.org/reverse"
        params = {"lat": lat, "lon": lon, "format": "json", "addressdetails": 1}
        headers = {
            "User-Agent": "PotholeMonitoringSystem/1.0 (Academic Research Project; https://github.com/pothole-monitoring)",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            address = data.get("address", {})
            result = {
                "city": address.get("city") or address.get("town") or "Ho Chi Minh City",
                "ward": address.get("suburb") or address.get("neighbourhood"),
                "district": address.get("city_district") or address.get("county"),
                "street_name": address.get("road"),
                "cached_at": time.time(),
            }
            try:
                self.redis.setex(cache_key, self.cache_ttl, json.dumps(result))
            except Exception as e:
                print(f"[WARN] Redis cache write failed: {e}")
            print(f"[OSM API] Geocoded: {result.get('street_name')}, {result.get('ward')}, {result.get('district')}")
            return result
        except requests.RequestException as e:
            print(f"[ERROR] OSM API failed: {e}")
            return None

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[DELIVERED] {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_and_flush(producer, topic, key, value, timeout=30):
    delivery_error = {"error": None}

    def callback(err, msg):
        delivery_report(err, msg)
        if err is not None:
            delivery_error["error"] = err

    producer.produce(topic=topic, key=key, value=value, on_delivery=callback)
    remaining = producer.flush(timeout)
    if remaining > 0:
        raise TimeoutError(f"Timed out delivering message to {topic}")
    if delivery_error["error"] is not None:
        raise RuntimeError(f"Failed delivering message to {topic}: {delivery_error['error']}")


def main():
    print("=" * 70)
    print("GEO ENRICHMENT SERVICE")
    print("=" * 70)

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=2)
    geocoder = OSMGeocoder(redis_client)

    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(schema_registry, RAW_EVENT_SCHEMA_STR, lambda obj, ctx: obj)
    serializer = AvroSerializer(schema_registry, GEO_ENRICHED_SCHEMA_STR, lambda obj, ctx: obj)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"[INFO] Consuming from: {SOURCE_TOPIC}")
    print(f"[INFO] Producing to: {OUTPUT_TOPIC}")

    count = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            raw_event = None
            try:
                raw_event = deserializer(msg.value(), SerializationContext(SOURCE_TOPIC, MessageField.VALUE))
                if raw_event is None:
                    consumer.commit(message=msg)
                    continue

                event_id = raw_event["event_id"]
                lat = raw_event["gps_lat"]
                lon = raw_event["gps_lon"]
                count += 1
                print(f"\n[RECEIVED #{count}] event_id={event_id} lat={lat} lon={lon}")

                geo = geocoder.reverse_geocode(lat, lon) or {}
                output = {
                    "event_id": event_id,
                    "city": geo.get("city"),
                    "district": geo.get("district"),
                    "ward": geo.get("ward"),
                    "road_segment_id": geo.get("street_name"),
                    "geocoded_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                }

                serialized = serializer(output, SerializationContext(OUTPUT_TOPIC, MessageField.VALUE))
                produce_and_flush(producer, OUTPUT_TOPIC, event_id, serialized)
                consumer.commit(message=msg)
                print(f"[PUBLISHED] event_id={event_id}, district={output['district']}, ward={output['ward']}")

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                payload = raw_event if raw_event is not None else msg.value()
                key = payload.get("event_id") if isinstance(payload, dict) else None
                send_to_dlq(dlq_producer, DLQ_TOPIC, SOURCE_TOPIC, SERVICE_NAME, e, payload, key=key)
                consumer.commit(message=msg)
                import traceback

                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
    finally:
        producer.flush()
        dlq_producer.flush()
        consumer.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
