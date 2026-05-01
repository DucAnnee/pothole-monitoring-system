from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

import numpy as np

TIMING_FIELDS = [
    "preprocess_ms",
    "inference_ms",
    "postprocess_ms",
    "total_ms",
]


def summarize_frame_metrics(
    frame_metrics: List[Dict[str, Any]],
    timed_wall_sec: float,
) -> Dict[str, Any]:
    """Build aggregate runtime statistics from per-frame benchmark rows."""
    summary: Dict[str, Any] = {
        "benchmarked_frames": len(frame_metrics),
        "timed_wall_sec": round(timed_wall_sec, 6),
        "effective_fps": (
            round(len(frame_metrics) / timed_wall_sec, 6) if timed_wall_sec > 0 else 0
        ),
        "detections_total": int(
            sum(int(row.get("detections_after_roi", 0)) for row in frame_metrics)
        ),
        "detections_raw_total": int(
            sum(int(row.get("detections_raw", 0)) for row in frame_metrics)
        ),
        "timing": {},
    }

    for field in TIMING_FIELDS:
        values = [float(row[field]) for row in frame_metrics if row.get(field) != ""]
        summary["timing"][field] = _summarize_numbers(values)

    return summary


def write_summary_json(output_path: Path, payload: Dict[str, Any]) -> None:
    """Write a pretty JSON summary."""
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )


def write_summary_markdown(output_path: Path, payload: Dict[str, Any]) -> None:
    """Write a concise Markdown summary for humans and audit packets."""
    lines = [
        "# Segmentation Benchmark Summary",
        "",
        "## Run",
        "",
    ]

    run = payload.get("run", {})
    model = payload.get("model", {})
    input_video = payload.get("input", {})
    summary = payload.get("summary", {})

    source_type = input_video.get("source_type", "video")
    if source_type == "camera":
        input_lines = [
            f"- Source: `camera`",
            f"- Camera index: `{input_video.get('camera_index', '')}`",
            f"- Backend: `{input_video.get('camera_backend', '')}`",
            f"- Requested: `{input_video.get('requested_width', '')}x{input_video.get('requested_height', '')} @ {input_video.get('requested_fps', '')} FPS`",
            f"- Observed: `{input_video.get('observed_width', '')}x{input_video.get('observed_height', '')} @ {input_video.get('observed_fps', '')} FPS`",
        ]
    else:
        input_lines = [
            f"- Source: `video`",
            f"- Video: `{input_video.get('video_path', '')}`",
            f"- Resolution: `{input_video.get('width', '')}x{input_video.get('height', '')}`",
            f"- FPS: `{input_video.get('fps', '')}`",
            f"- Frames: `{input_video.get('frame_count', '')}`",
            f"- SHA256: `{input_video.get('sha256', '')}`",
        ]

    lines.extend(
        [
            f"- Run ID: `{run.get('run_id', '')}`",
            f"- Created: `{run.get('created_at_utc', '')}`",
            f"- Command: `{run.get('command', '')}`",
            f"- Git commit: `{run.get('git_commit', '')}`",
            f"- Git dirty: `{run.get('git_dirty', '')}`",
            "",
            "## Model",
            "",
            f"- Type: `{model.get('model_type', '')}`",
            f"- Path: `{model.get('model_path', '')}`",
            f"- SHA256: `{model.get('model_sha256', '')}`",
            f"- Confidence threshold: `{model.get('confidence_threshold', '')}`",
            f"- Load time: `{model.get('load_time_sec', '')} sec`",
            "",
            "## Input",
            "",
            *input_lines,
            "",
            "## Runtime",
            "",
            f"- Benchmarked frames: `{summary.get('benchmarked_frames', '')}`",
            f"- Timed wall time: `{summary.get('timed_wall_sec', '')} sec`",
            f"- Effective FPS: `{summary.get('effective_fps', '')}`",
            f"- Raw detections: `{summary.get('detections_raw_total', '')}`",
            f"- Detections after ROI: `{summary.get('detections_total', '')}`",
            "",
            "| Metric | Avg ms | P50 ms | P90 ms | P95 ms | P99 ms | Min ms | Max ms |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    timing = summary.get("timing", {})
    for field in TIMING_FIELDS:
        stats = timing.get(field, {})
        lines.append(
            "| {name} | {avg} | {p50} | {p90} | {p95} | {p99} | {min_} | {max_} |".format(
                name=field,
                avg=stats.get("avg", ""),
                p50=stats.get("p50", ""),
                p90=stats.get("p90", ""),
                p95=stats.get("p95", ""),
                p99=stats.get("p99", ""),
                min_=stats.get("min", ""),
                max_=stats.get("max", ""),
            )
        )

    lines.extend(
        [
            "",
            "## Files",
            "",
            "- `frame_metrics.jsonl`: per-frame segmentation timing",
            "- `hardware_metrics.csv`: sampled CPU/RAM/GPU telemetry when enabled",
            "- `device_info.json`: host, Python, package, and GPU metadata",
            "- `run_config.yaml`: effective benchmark configuration",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _summarize_numbers(values: Iterable[float]) -> Dict[str, Any]:
    values = list(values)
    if not values:
        return {
            "count": 0,
            "avg": "",
            "min": "",
            "max": "",
            "p50": "",
            "p90": "",
            "p95": "",
            "p99": "",
        }

    arr = np.array(values, dtype=float)
    return {
        "count": int(arr.size),
        "avg": round(float(np.mean(arr)), 6),
        "min": round(float(np.min(arr)), 6),
        "max": round(float(np.max(arr)), 6),
        "p50": round(float(np.percentile(arr, 50)), 6),
        "p90": round(float(np.percentile(arr, 90)), 6),
        "p95": round(float(np.percentile(arr, 95)), 6),
        "p99": round(float(np.percentile(arr, 99)), 6),
    }
