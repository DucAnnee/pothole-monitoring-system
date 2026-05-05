import pytest


@pytest.mark.unit
def test_severity_discretization_boundaries(repo_root, load_module):
    pytest.importorskip("confluent_kafka")
    module = load_module(
        "severity_aggregator_unit",
        repo_root / "cloud" / "severity_calculation_service" / "severity_aggregator.py",
        repo_root / "cloud" / "severity_calculation_service",
    )

    assert module.map_area_to_discrete(299.99) == 1
    assert module.map_area_to_discrete(300) == 2
    assert module.map_area_to_discrete(45000) == 10
    assert module.map_depth_to_discrete(0.99) == 1
    assert module.map_depth_to_discrete(1.0) == 2
    assert module.map_depth_to_discrete(25) == 10


@pytest.mark.unit
def test_severity_levels(repo_root, load_module):
    pytest.importorskip("confluent_kafka")
    module = load_module(
        "severity_aggregator_levels_unit",
        repo_root / "cloud" / "severity_calculation_service" / "severity_aggregator.py",
        repo_root / "cloud" / "severity_calculation_service",
    )

    assert module.get_severity_level(1) == "MINOR"
    assert module.get_severity_level(4) == "MODERATE"
    assert module.get_severity_level(6) == "HIGH"
    assert module.get_severity_level(8) == "CRITICAL"


@pytest.mark.unit
def test_severity_produce_flush_raises_before_commit_on_delivery_error(repo_root, load_module):
    pytest.importorskip("confluent_kafka")
    module = load_module(
        "severity_aggregator_delivery_unit",
        repo_root / "cloud" / "severity_calculation_service" / "severity_aggregator.py",
        repo_root / "cloud" / "severity_calculation_service",
    )

    class Producer:
        def produce(self, **kwargs):
            kwargs["on_delivery"](RuntimeError("boom"), None)

        def flush(self, timeout):
            return 0

    with pytest.raises(RuntimeError):
        module.produce_and_flush(Producer(), "topic", "key", b"value")
