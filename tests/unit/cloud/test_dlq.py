from datetime import datetime, timezone
import json


def test_build_dlq_record_includes_expected_payload(repo_root, load_module):
    module = load_module(
        "shared_dlq_unit",
        repo_root / "cloud" / "shared" / "dlq.py",
        repo_root,
    )

    record = module.build_dlq_record(
        "source.topic",
        "service-name",
        ValueError("bad payload"),
        {"event_id": "evt-1"},
        key="evt-1",
    )

    assert record["source_topic"] == "source.topic"
    assert record["service_name"] == "service-name"
    assert record["key"] == "evt-1"
    assert record["error"] == "bad payload"
    assert record["payload"] == {"event_id": "evt-1"}
    assert isinstance(record["failed_at"], int)


def test_send_to_dlq_serializes_record_and_polls(repo_root, load_module):
    module = load_module(
        "shared_dlq_send_unit",
        repo_root / "cloud" / "shared" / "dlq.py",
        repo_root,
    )

    class Producer:
        def __init__(self):
            self.produced = None
            self.polls = []

        def produce(self, **kwargs):
            self.produced = kwargs

        def poll(self, timeout):
            self.polls.append(timeout)

    producer = Producer()
    payload = {"event_id": "evt-2", "seen_at": datetime(2026, 5, 6, tzinfo=timezone.utc)}

    module.send_to_dlq(
        producer,
        "dlq.topic",
        "source.topic",
        "service-name",
        RuntimeError("boom"),
        payload,
        key="evt-2",
    )

    assert producer.produced["topic"] == "dlq.topic"
    assert producer.produced["key"] == "evt-2"
    assert isinstance(producer.produced["value"], bytes)
    decoded = json.loads(producer.produced["value"].decode("utf-8"))
    assert decoded["source_topic"] == "source.topic"
    assert decoded["service_name"] == "service-name"
    assert decoded["error"] == "boom"
    assert decoded["payload"]["seen_at"] == "2026-05-06 00:00:00+00:00"
    assert producer.polls == [0]
