import pytest


@pytest.mark.unit
def test_simulated_gps_returns_reading_within_bounds(repo_root, load_module):
    module = load_module(
        "edge_gps_provider_unit",
        repo_root / "edge" / "gps_provider.py",
        repo_root / "edge",
    )
    p = module.SimulatedGpsProvider(lat_min=10.7, lat_max=10.9, lon_min=106.6, lon_max=106.8)
    reading = p.read()
    assert 10.7 <= reading.lat <= 10.9
    assert 106.6 <= reading.lon <= 106.8
    assert reading.accuracy_m is not None
    assert 5.0 <= reading.accuracy_m <= 15.0


@pytest.mark.unit
def test_gps_reading_converts_to_raw_event_fields(repo_root, load_module):
    module = load_module(
        "edge_gps_provider_unit_fields",
        repo_root / "edge" / "gps_provider.py",
        repo_root / "edge",
    )
    reading = module.GpsReading(lat=10.75, lon=106.65, accuracy_m=8.5)

    assert reading.to_raw_event_fields() == {
        "gps_lat": 10.75,
        "gps_lon": 106.65,
        "gps_accuracy": 8.5,
    }


@pytest.mark.unit
def test_build_gps_provider_uses_config_bounds_and_accuracy(repo_root, load_module):
    module = load_module(
        "edge_gps_provider_unit_builder",
        repo_root / "edge" / "gps_provider.py",
        repo_root / "edge",
    )
    provider = module.build_gps_provider(
        {
            "gps": {
                "lat_min": 10.7,
                "lat_max": 10.71,
                "lon_min": 106.6,
                "lon_max": 106.61,
                "accuracy_min_m": 2.0,
                "accuracy_max_m": 3.0,
            }
        }
    )

    reading = provider.read()

    assert isinstance(provider, module.SimulatedGpsProvider)
    assert 10.7 <= reading.lat <= 10.71
    assert 106.6 <= reading.lon <= 106.61
    assert 2.0 <= reading.accuracy_m <= 3.0


@pytest.mark.unit
def test_gps_provider_is_abstract(repo_root, load_module):
    import inspect
    module = load_module(
        "edge_gps_provider_unit",
        repo_root / "edge" / "gps_provider.py",
        repo_root / "edge",
    )
    assert inspect.isabstract(module.GpsProvider)
