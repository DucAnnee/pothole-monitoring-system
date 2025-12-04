import json
import base64
import time
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError
from minio import Minio
from minio.error import S3Error
from io import BytesIO

TOPIC = "road-condition"
PARTITION = 0

BOOTSTRAP_SERVERS = [
    "localhost:19092",
    "localhost:29092",
    "localhost:39092",
]

# MinIO config
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "road-data"


def connect_kafka(max_retries=5, delay=2):
    """Attempt to connect to Kafka with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Connecting to Kafka (attempt {attempt})...")
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=5000,
            )

            tp = TopicPartition(TOPIC, PARTITION)
            consumer.assign([tp])

            print("[SUCCESS] Connected to Kafka.")
            return consumer

        except Exception as e:
            print(f"[ERROR] Kafka connection failed: {e}")
            if attempt < max_retries:
                print(f"[INFO] Retrying in {delay} seconds...")
                time.sleep(delay)

    print("[FATAL] Could not establish Kafka connection.")
    return None


def connect_minio():
    """Initialize MinIO client and ensure bucket exists."""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # Create bucket if not exists
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)
            print(f"[INFO] Created MinIO bucket: {BUCKET}")
        else:
            print(f"[INFO] Using MinIO bucket: {BUCKET}")

        return client

    except S3Error as e:
        print(f"[FATAL] MinIO S3 error: {e}")
        return None
    except Exception as e:
        print(f"[FATAL] Failed connecting to MinIO: {e}")
        return None


def upload_to_minio(client, img_bytes, metadata):
    """Upload decoded image and metadata JSON into MinIO."""
    road_id = metadata.get("road_id", "unknown")
    frame_id = metadata.get("frame_id", "noframe")

    img_name = f"{road_id}_{frame_id}.jpg"
    meta_name = f"{road_id}_{frame_id}.json"

    try:
        # Upload image
        client.put_object(
            bucket_name=BUCKET,
            object_name=img_name,
            data=BytesIO(img_bytes),
            length=len(img_bytes),
            content_type="image/jpeg",
        )

        # Upload metadata JSON
        meta_json = json.dumps(metadata, indent=2).encode()
        client.put_object(
            bucket_name=BUCKET,
            object_name=meta_name,
            data=BytesIO(meta_json),
            length=len(meta_json),
            content_type="application/json",
        )

        print(f"[UPLOADED] {img_name}, {meta_name}")
    except Exception as e:
        print(f"[ERROR] Failed uploading to MinIO: {e}")


def main():
    consumer = connect_kafka()
    if consumer is None:
        return

    minio_client = connect_minio()
    if minio_client is None:
        return

    print(f"[INFO] Listening on topic '{TOPIC}'...")

    try:
        for msg in consumer:
            try:
                payload = json.loads(msg.value.decode("utf-8"))
                metadata = payload.get("metadata")
                image_b64 = payload.get("image_base64")

                if metadata is None or image_b64 is None:
                    print("[WARN] Missing fields in message. Skipping.")
                    continue

                img_bytes = base64.b64decode(image_b64)

                upload_to_minio(minio_client, img_bytes, metadata)

            except json.JSONDecodeError:
                print("[ERROR] Bad JSON in Kafka message.")
            except Exception as e:
                print(f"[ERROR] Failed processing message: {e}")

    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")

    finally:
        consumer.close()
        print("[INFO] Kafka consumer closed.")


if __name__ == "__main__":
    main()
