import json
import pytest
from pathlib import Path
from unittest.mock import patch


def test_load_creates_state_file_on_first_run(tmp_path):
    state_path = tmp_path / ".device_state.json"
    from edge.device_state import load_device_state
    state = load_device_state(state_path)
    assert state_path.exists()
    assert state["vehicle_id"].startswith("vehicle-")
    assert len(state["vehicle_id"]) > 8


def test_load_returns_same_vehicle_id_on_second_run(tmp_path):
    state_path = tmp_path / ".device_state.json"
    from edge.device_state import load_device_state
    first = load_device_state(state_path)
    second = load_device_state(state_path)
    assert first["vehicle_id"] == second["vehicle_id"]


def test_load_respects_existing_state(tmp_path):
    state_path = tmp_path / ".device_state.json"
    state_path.write_text(json.dumps({"vehicle_id": "vehicle-abc123"}))
    from edge.device_state import load_device_state
    state = load_device_state(state_path)
    assert state["vehicle_id"] == "vehicle-abc123"
