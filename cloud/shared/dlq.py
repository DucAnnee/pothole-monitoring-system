import json
import time


def build_dlq_record(source_topic, service_name, error, payload, key=None):
    return {
        "source_topic": source_topic,
        "service_name": service_name,
        "key": key,
        "error": str(error),
        "payload": payload,
        "failed_at": int(time.time() * 1000),
    }


def send_to_dlq(producer, dlq_topic, source_topic, service_name, error, payload, key=None):
    record = build_dlq_record(source_topic, service_name, error, payload, key=key)
    value = json.dumps(record, default=str).encode("utf-8")
    producer.produce(topic=dlq_topic, key=key, value=value)
    producer.poll(0)
