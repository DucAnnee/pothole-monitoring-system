"""Telemetry backends used by benchmark runs."""

from benchmarks.telemetry.base import TelemetryLogger
from benchmarks.telemetry.nvidia_smi import NvidiaSmiTelemetry

__all__ = ["TelemetryLogger", "NvidiaSmiTelemetry"]

