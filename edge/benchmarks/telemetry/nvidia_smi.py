from __future__ import annotations

import csv
import os
import subprocess
from io import StringIO
from typing import Dict, Optional


try:
    import psutil
except ImportError:  # pragma: no cover - handled at runtime for minimal installs
    psutil = None  # type: ignore


GPU_QUERY_FIELDS = [
    "index",
    "name",
    "driver_version",
    "utilization.gpu",
    "utilization.memory",
    "memory.used",
    "memory.total",
    "temperature.gpu",
    "power.draw",
    "power.limit",
    "clocks.gr",
    "clocks.mem",
]


class NvidiaSmiTelemetry:
    """Sample Linux system metrics plus one NVIDIA GPU through nvidia-smi."""

    def __init__(self, gpu_index: Optional[int] = 0):
        self.gpu_index = gpu_index
        self._process = psutil.Process(os.getpid()) if psutil else None

        if psutil:
            psutil.cpu_percent(interval=None)
            if self._process:
                self._process.cpu_percent(interval=None)

    def sample(self, started_at: float) -> Dict[str, object]:
        """Return one system/GPU telemetry sample."""
        _ = started_at
        system = self._sample_system()
        gpu = self._sample_gpu()
        errors = [
            str(part.get("telemetry_error"))
            for part in [system, gpu]
            if part.get("telemetry_error")
        ]

        row: Dict[str, object] = {}
        row.update(system)
        row.update(gpu)
        row["telemetry_error"] = "; ".join(errors)
        return row

    def _sample_system(self) -> Dict[str, object]:
        if not psutil:
            return {"telemetry_error": "psutil is not installed"}

        vm = psutil.virtual_memory()
        process_rss = self._process.memory_info().rss if self._process else 0
        process_cpu = self._process.cpu_percent(interval=None) if self._process else 0.0

        return {
            "cpu_percent": round(psutil.cpu_percent(interval=None), 3),
            "cpu_count": psutil.cpu_count(logical=True),
            "ram_used_mb": _bytes_to_mb(vm.used),
            "ram_total_mb": _bytes_to_mb(vm.total),
            "ram_percent": round(vm.percent, 3),
            "process_rss_mb": _bytes_to_mb(process_rss),
            "process_cpu_percent": round(process_cpu, 3),
        }

    def _sample_gpu(self) -> Dict[str, object]:
        cmd = [
            "nvidia-smi",
            f"--query-gpu={','.join(GPU_QUERY_FIELDS)}",
            "--format=csv,noheader,nounits",
        ]
        if self.gpu_index is not None:
            cmd[1:1] = ["-i", str(self.gpu_index)]

        try:
            completed = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=2.0,
            )
        except Exception as exc:
            return {"telemetry_error": f"nvidia-smi unavailable: {exc!r}"}

        rows = list(csv.reader(StringIO(completed.stdout.strip())))
        if not rows:
            return {"telemetry_error": "nvidia-smi returned no rows"}

        values = [value.strip() for value in rows[0]]
        gpu = dict(zip(GPU_QUERY_FIELDS, values))

        return {
            "gpu_index": _parse_int(gpu.get("index")),
            "gpu_name": gpu.get("name", ""),
            "gpu_driver_version": gpu.get("driver_version", ""),
            "gpu_util_percent": _parse_float(gpu.get("utilization.gpu")),
            "gpu_memory_util_percent": _parse_float(gpu.get("utilization.memory")),
            "gpu_memory_used_mb": _parse_float(gpu.get("memory.used")),
            "gpu_memory_total_mb": _parse_float(gpu.get("memory.total")),
            "gpu_temperature_c": _parse_float(gpu.get("temperature.gpu")),
            "gpu_power_draw_w": _parse_float(gpu.get("power.draw")),
            "gpu_power_limit_w": _parse_float(gpu.get("power.limit")),
            "gpu_graphics_clock_mhz": _parse_float(gpu.get("clocks.gr")),
            "gpu_memory_clock_mhz": _parse_float(gpu.get("clocks.mem")),
            "telemetry_error": "",
        }


def _bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 3)


def _parse_float(value: object) -> object:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.upper() in {"N/A", "[NOT SUPPORTED]"}:
        return ""
    try:
        return round(float(text), 3)
    except ValueError:
        return ""


def _parse_int(value: object) -> object:
    parsed = _parse_float(value)
    if parsed == "":
        return ""
    return int(float(parsed))
