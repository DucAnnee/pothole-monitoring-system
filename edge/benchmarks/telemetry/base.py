from __future__ import annotations

import csv
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Protocol


TELEMETRY_FIELDS = [
    "timestamp_utc",
    "elapsed_sec",
    "cpu_percent",
    "cpu_count",
    "ram_used_mb",
    "ram_total_mb",
    "ram_percent",
    "process_rss_mb",
    "process_cpu_percent",
    "gpu_index",
    "gpu_name",
    "gpu_driver_version",
    "gpu_util_percent",
    "gpu_memory_util_percent",
    "gpu_memory_used_mb",
    "gpu_memory_total_mb",
    "gpu_temperature_c",
    "gpu_power_draw_w",
    "gpu_power_limit_w",
    "gpu_graphics_clock_mhz",
    "gpu_memory_clock_mhz",
    "telemetry_error",
]


class TelemetryProvider(Protocol):
    """Read one hardware telemetry sample."""

    def sample(self, started_at: float) -> Dict[str, object]:
        """Return one telemetry sample as a flat dictionary."""


class TelemetryLogger:
    """Write telemetry samples to CSV from a background thread."""

    def __init__(
        self,
        provider: TelemetryProvider,
        output_path: Path,
        interval_sec: float = 1.0,
    ):
        if interval_sec <= 0:
            raise ValueError("interval_sec must be greater than 0")
        self.provider = provider
        self.output_path = output_path
        self.interval_sec = interval_sec
        self._started_at = time.perf_counter()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start telemetry collection."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._started_at = time.perf_counter()
        self._thread.start()

    def stop(self) -> None:
        """Stop telemetry collection and wait for the writer thread."""
        self._stop_event.set()
        self._thread.join(timeout=max(5.0, self.interval_sec * 2))

    def _run(self) -> None:
        with self.output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TELEMETRY_FIELDS)
            writer.writeheader()

            while not self._stop_event.is_set():
                row = self._read_row()
                writer.writerow(row)
                f.flush()
                self._stop_event.wait(self.interval_sec)

    def _read_row(self) -> Dict[str, object]:
        try:
            sample = self.provider.sample(self._started_at)
        except Exception as exc:  # pragma: no cover - defensive telemetry path
            sample = {"telemetry_error": repr(exc)}

        row = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "elapsed_sec": round(time.perf_counter() - self._started_at, 6),
        }
        row.update(sample)
        return {field: row.get(field, "") for field in TELEMETRY_FIELDS}
