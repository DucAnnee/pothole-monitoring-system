import json
import pytest


@pytest.mark.unit
def test_load_creates_state_file_on_first_run(tmp_path, repo_root, load_module):
    state_path = tmp_path / ".device_state.json"
    module = load_module(
        "edge_device_state_unit",
        repo_root / "edge" / "device_state.py",
        repo_root / "edge",
    )
    state = module.load_device_state(state_path)
    assert state_path.exists()
    assert state["vehicle_id"].startswith("vehicle-")
    assert state["device_id"].startswith("device-")
    assert len(state["vehicle_id"]) > 8
    assert len(state["device_id"]) > 7


@pytest.mark.unit
def test_load_returns_same_vehicle_id_on_second_run(tmp_path, repo_root, load_module):
    state_path = tmp_path / ".device_state.json"
    module = load_module(
        "edge_device_state_unit_2",
        repo_root / "edge" / "device_state.py",
        repo_root / "edge",
    )
    first = module.load_device_state(state_path)
    second = module.load_device_state(state_path)
    assert first["vehicle_id"] == second["vehicle_id"]
    assert first["device_id"] == second["device_id"]


@pytest.mark.unit
def test_load_respects_existing_state(tmp_path, repo_root, load_module):
    state_path = tmp_path / ".device_state.json"
    state_path.write_text(json.dumps({"vehicle_id": "vehicle-abc123"}))
    module = load_module(
        "edge_device_state_unit_3",
        repo_root / "edge" / "device_state.py",
        repo_root / "edge",
    )
    state = module.load_device_state(state_path)
    assert state["vehicle_id"] == "vehicle-abc123"
    assert state["device_id"].startswith("device-")


@pytest.mark.unit
def test_load_migrates_vehicle_only_state_with_device_id(tmp_path, repo_root, load_module):
    state_path = tmp_path / ".device_state.json"
    state_path.write_text(json.dumps({"vehicle_id": "vehicle-legacy"}))
    module = load_module(
        "edge_device_state_unit_4",
        repo_root / "edge" / "device_state.py",
        repo_root / "edge",
    )

    state = module.load_device_state(state_path)
    persisted = json.loads(state_path.read_text())

    assert state["vehicle_id"] == "vehicle-legacy"
    assert state["device_id"].startswith("device-")
    assert persisted == state
