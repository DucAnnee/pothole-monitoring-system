import json
import base64
import time
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError
from pathlib import Path

TOPIC = "road-condition"
PARTITION = 0

BOOTSTRAP_SERVERS = [
    "localhost:19092",
    "localhost:29092",
    "localhost:39092",
]

OUTPUT_DIR = Path("decoded_output")
OUTPUT_DIR.mkdir(exist_ok=True)


def create_consumer(max_retries=5, delay=2):
    """Attempt to connect to Kafka with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Kafka (attempt {attempt})...")
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=5000,
                fetch_max_bytes=52428800,
                max_partition_fetch_bytes=52428800,
            )

            # Assign specific partition
            tp = TopicPartition(TOPIC, PARTITION)
            consumer.assign([tp])

            print("[SUCCESS] Connected to Kafka.")
            return consumer

        except NoBrokersAvailable:
            print("[ERROR] Kafka brokers unreachable.")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")

        if attempt < max_retries:
            print(f"[INFO] Retrying in {delay} seconds...")
            time.sleep(delay)

    print("[FATAL] Could not connect to Kafka.")
    return None


def process_message(data):
    """Extract JSON metadata + Base64 image and save as files."""
    metadata = data.get("metadata")
    img_b64 = data.get("image_base64")

    if metadata is None or img_b64 is None:
        print("[WARN] Message missing required fields. Skipping.")
        return

    # Build file names
    road_id = metadata.get("road_id", "unknown")
    frame_id = metadata.get("frame_id", "noframe")

    img_filename = OUTPUT_DIR / f"{road_id}_{frame_id}.jpg"
    meta_filename = OUTPUT_DIR / f"{road_id}_{frame_id}.json"

    # Decode image
    try:
        img_bytes = base64.b64decode(img_b64)
        with open(img_filename, "wb") as f:
            f.write(img_bytes)

        with open(meta_filename, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"[SAVED] Image → {img_filename}")
        print(f"[SAVED] Metadata → {meta_filename}")

    except Exception as e:
        print(f"[ERROR] Failed to decode or save image: {e}")


def main():
    consumer = create_consumer()
    if consumer is None:
        return

    print(f"[INFO] Listening on topic '{TOPIC}', partition {PARTITION}...")

    try:
        for msg in consumer:
            try:
                payload = json.loads(msg.value.decode("utf-8"))
                print(f"[RECV] Offset {msg.offset}")
                process_message(payload)

            except json.JSONDecodeError:
                print("[ERROR] Received invalid JSON.")
            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")

    except KeyboardInterrupt:
        print("\n[INFO] Consumer stopped manually.")

    finally:
        consumer.close()
        print("[INFO] Consumer connection closed.")


if __name__ == "__main__":
    main()
