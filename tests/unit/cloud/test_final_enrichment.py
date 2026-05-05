import pytest

from tests.fixtures.sample_records import RAW_EVENT_V2, SEVERITY_SCORE_V1


@pytest.mark.unit
def test_aggregation_store_returns_combined_records_with_messages(repo_root, load_module):
    pytest.importorskip("h3")
    pytest.importorskip("redis")
    pytest.importorskip("confluent_kafka")
    pytest.importorskip("trino")
    module = load_module(
        "final_enrichment_unit",
        repo_root / "cloud" / "final_enrichment_service" / "final_enrichment_service.py",
        repo_root / "cloud" / "final_enrichment_service",
    )
    store = module.EventAggregationStore()
    raw_msg = object()
    severity_msg = object()

    assert store.add_raw_event("evt-test-001", RAW_EVENT_V2, raw_msg) is None
    combined = store.add_severity("evt-test-001", SEVERITY_SCORE_V1, severity_msg)

    assert combined["raw_event"] == RAW_EVENT_V2
    assert combined["severity"] == SEVERITY_SCORE_V1
    assert combined["raw_msg"] is raw_msg
    assert combined["severity_msg"] is severity_msg
    assert store.size() == 0


@pytest.mark.unit
def test_final_enrichment_helpers_escape_and_preserve_h3_bigint(repo_root, load_module):
    pytest.importorskip("h3")
    pytest.importorskip("redis")
    pytest.importorskip("confluent_kafka")
    pytest.importorskip("trino")
    module = load_module(
        "final_enrichment_helpers_unit",
        repo_root / "cloud" / "final_enrichment_service" / "final_enrichment_service.py",
        repo_root / "cloud" / "final_enrichment_service",
    )

    assert module.escape_sql_string("Nguyen's Road") == "Nguyen''s Road"
    h3_index = module.h3.str_to_int(module.h3.latlng_to_cell(10.775, 106.701, 12))
    assert isinstance(h3_index, int)
    assert h3_index > 0


@pytest.mark.unit
def test_final_enrichment_consumer_is_manual_commit(repo_root):
    source = (
        repo_root / "cloud" / "final_enrichment_service" / "final_enrichment_service.py"
    ).read_text(encoding="utf-8")

    assert '"enable.auto.commit": False' in source
