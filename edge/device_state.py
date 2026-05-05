import json
from pathlib import Path
from uuid import uuid4

_DEFAULT_STATE_PATH = Path(__file__).parent / "local_storage" / ".device_state.json"


def _new_vehicle_id() -> str:
    return f"vehicle-{uuid4().hex[:8]}"


def _new_device_id() -> str:
    return f"device-{uuid4().hex[:8]}"


def load_device_state(path: Path = _DEFAULT_STATE_PATH) -> dict:
    """Load or create persistent device state."""
    if path.exists():
        with open(path, encoding="utf-8") as f:
            state = json.load(f)

        changed = False
        if "vehicle_id" not in state:
            state["vehicle_id"] = _new_vehicle_id()
            changed = True
        if "device_id" not in state:
            state["device_id"] = _new_device_id()
            changed = True

        if changed:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)

        return state

    path.parent.mkdir(parents=True, exist_ok=True)
    state = {"vehicle_id": _new_vehicle_id(), "device_id": _new_device_id()}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    return state
