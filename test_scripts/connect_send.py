import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError

TOPIC = "road-condition"


# Kafka brokers exposed to localhost
BOOTSTRAP_SERVERS = [
    "localhost:19092",
    "localhost:29092",
    "localhost:39092",
]


def create_producer(max_retries=5, delay=2):
    """Attempt to create a Kafka producer with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Kafka (attempt {attempt})...")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
                max_request_size=52428800,
            )
            # Force metadata refresh to verify connection
            producer.bootstrap_connected()
            print("[SUCCESS] Connected to Kafka brokers.")
            return producer

        except NoBrokersAvailable:
            print("[ERROR] Kafka brokers not reachable at the given addresses.")
        except KafkaTimeoutError:
            print("[ERROR] Connection to Kafka timed out.")
        except Exception as e:
            print(f"[ERROR] Unexpected error during connection: {e}")

        if attempt < max_retries:
            print(f"[INFO] Retrying in {delay} seconds...")
            time.sleep(delay)

    print("[FATAL] Could not connect to Kafka after multiple attempts.")
    return None


def send_payload(producer, payload):
    """Send JSON payload to Kafka with error handling."""
    try:
        future = producer.send(TOPIC, payload)
        result = future.get(timeout=10)

        print("[SUCCESS] Message delivered:")
        print(f"  Topic: {result.topic}")
        print(f"  Partition: {result.partition}")
        print(f"  Offset: {result.offset}")

    except KafkaTimeoutError:
        print("[ERROR] Timeout waiting for Kafka to acknowledge the message.")
    except KafkaError as e:
        print(f"[ERROR] Kafka error occurred: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error sending message: {e}")


def main():
    # Load payload.json
    try:
        with open("payload.json", "r") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"[FATAL] Failed to load payload.json: {e}")
        return

    # Connect to Kafka
    producer = create_producer()
    if producer is None:
        print("[FATAL] Exiting due to failed Kafka connection.")
        return

    # Send the payload
    send_payload(producer, payload)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
