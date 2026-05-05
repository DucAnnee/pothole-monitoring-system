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
def test_gps_provider_is_abstract(repo_root, load_module):
    import inspect
    module = load_module(
        "edge_gps_provider_unit",
        repo_root / "edge" / "gps_provider.py",
        repo_root / "edge",
    )
    assert inspect.isabstract(module.GpsProvider)
