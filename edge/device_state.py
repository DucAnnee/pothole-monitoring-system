import json
from pathlib import Path
from uuid import uuid4

_DEFAULT_STATE_PATH = Path(__file__).parent / "local_storage" / ".device_state.json"


def load_device_state(path: Path = _DEFAULT_STATE_PATH) -> dict:
    """Load or create persistent device state. Returns dict with vehicle_id."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {"vehicle_id": f"vehicle-{uuid4().hex[:8]}"}
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    return state
