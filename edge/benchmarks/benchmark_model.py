from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import cv2
import numpy as np
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.report import (  # noqa: E402
    summarize_frame_metrics,
    write_summary_json,
    write_summary_markdown,
)
from benchmarks.telemetry import NvidiaSmiTelemetry, TelemetryLogger  # noqa: E402
from segmentation.pothole_segmenter import (  # noqa: E402
    PotholeSegmenter,
    RFDETRSegmenter,
    YOLOSegmenter,
)

# List of important packages to record versions for in the device info
IMPORTANT_PACKAGES = [
    "numpy",
    "opencv-python",
    "PyYAML",
    "ultralytics",
    "torch",
    "torchvision",
    "rfdetr",
    "psutil",
]


class TimedSegmenterRunner:
    """Run one segmentation frame with split timings for benchmark output."""

    def __init__(
        self,
        segmenter: PotholeSegmenter,
        confidence_threshold: float,
        imgsz: Optional[int],
        device: Optional[str],
    ):
        self.segmenter = segmenter
        self.confidence_threshold = confidence_threshold
        self.imgsz = imgsz
        self.device = device

    def run(self, frame_rgb: np.ndarray) -> Dict[str, Any]:
        """Run segmentation and return timing/detection counts."""
        total_start = time.perf_counter()

        preprocess_start = time.perf_counter()
        masked_image = self.segmenter.create_masked_image(frame_rgb)
        preprocess_ms = _elapsed_ms(preprocess_start)

        _synchronize_cuda()
        inference_start = time.perf_counter()
        raw_results = self._predict(masked_image)
        _synchronize_cuda()
        inference_ms = _elapsed_ms(inference_start)

        postprocess_start = time.perf_counter()
        detections_raw, detections_after_roi = self._postprocess(
            raw_results,
            frame_rgb.shape,
        )
        postprocess_ms = _elapsed_ms(postprocess_start)

        return {
            "preprocess_ms": preprocess_ms,
            "inference_ms": inference_ms,
            "postprocess_ms": postprocess_ms,
            "total_ms": _elapsed_ms(total_start),
            "detections_raw": detections_raw,
            "detections_after_roi": detections_after_roi,
        }

    def _predict(self, masked_image: np.ndarray) -> Any:
        if isinstance(self.segmenter, YOLOSegmenter):
            predict_kwargs: Dict[str, Any] = {
                "conf": self.confidence_threshold,
                "verbose": False,
            }
            if self.imgsz is not None:
                predict_kwargs["imgsz"] = self.imgsz
            if self.device is not None:
                predict_kwargs["device"] = self.device
            return self.segmenter.model.predict(masked_image, **predict_kwargs)

        if isinstance(self.segmenter, RFDETRSegmenter):
            return self.segmenter.model.predict(
                masked_image,
                threshold=self.confidence_threshold,
            )

        return self.segmenter.segment(masked_image)

    def _postprocess(
        self,
        raw_results: Any,
        frame_shape: Tuple[int, ...],
    ) -> Tuple[int, int]:
        if isinstance(self.segmenter, YOLOSegmenter):
            return self._postprocess_yolo(raw_results, frame_shape)

        if isinstance(self.segmenter, RFDETRSegmenter):
            return self._postprocess_rfdetr(raw_results, frame_shape)

        raw_count = len(raw_results) if raw_results is not None else 0
        return raw_count, raw_count

    def _postprocess_yolo(
        self,
        results: Any,
        frame_shape: Tuple[int, ...],
    ) -> Tuple[int, int]:
        if not results or results[0].masks is None:
            return 0, 0

        masks_data = results[0].masks.xy if hasattr(results[0].masks, "xy") else []
        confidences = results[0].boxes.conf if results[0].boxes is not None else []

        raw_count = 0
        roi_count = 0
        for i, contour in enumerate(masks_data):
            if i >= len(confidences):
                continue
            if not isinstance(contour, np.ndarray):
                contour = np.array(contour)
            if contour.shape[0] <= 2:
                continue

            raw_count += 1
            contour = contour.astype(np.float32)
            if self.segmenter.pothole_in_trapezoid(contour, frame_shape):
                roi_count += 1

        return raw_count, roi_count

    def _postprocess_rfdetr(
        self,
        results: Any,
        frame_shape: Tuple[int, ...],
    ) -> Tuple[int, int]:
        if results is None or not hasattr(results, "mask") or results.mask is None:
            return 0, 0

        raw_count = 0
        roi_count = 0
        for binary_mask in results.mask:
            contour = self.segmenter._mask_to_contour(binary_mask)
            if contour is None or len(contour) <= 2:
                continue

            raw_count += 1
            if self.segmenter.pothole_in_trapezoid(contour, frame_shape):
                roi_count += 1

        return raw_count, roi_count


def main() -> None:
    args = parse_args()
    config = load_yaml(args.config)
    model_settings = resolve_model_settings(args, config)
    input_source = resolve_input_source(args)

    run_dir = create_run_dir(args, model_settings)
    telemetry_logger = build_telemetry_logger(args, run_dir)
    frame_metrics: List[Dict[str, Any]] = []
    segmenter: Optional[PotholeSegmenter] = None

    started_at = datetime.now(timezone.utc)
    print(f"[BENCH] Writing results to {run_dir}")

    try:
        if telemetry_logger:
            telemetry_logger.start()

        write_json(run_dir / "device_info.json", collect_device_info(args.gpu_index))
        write_run_config(
            run_dir / "run_config.yaml", args, model_settings, input_source
        )

        load_started = time.perf_counter()
        segmenter = PotholeSegmenter.create(
            model_type=model_settings["model_type"],
            model_path=model_settings["model_path"],
            trapezoid_coords=model_settings["trapezoid_coords"],
            confidence_threshold=model_settings["confidence_threshold"],
            frame_interval=args.frame_interval,
        )
        load_time_sec = round(time.perf_counter() - load_started, 6)

        runner = TimedSegmenterRunner(
            segmenter=segmenter,
            confidence_threshold=model_settings["confidence_threshold"],
            imgsz=args.imgsz,
            device=args.device,
        )

        warmup_count, frame_metrics, timed_wall_sec, input_manifest = (
            run_capture_benchmark(
                input_source=input_source,
                runner=runner,
                frame_interval=args.frame_interval,
                warmup_frames=args.warmup_frames,
                max_frames=args.max_frames,
                resize=parse_resize(args.resize),
                output_path=run_dir / "frame_metrics.jsonl",
            )
        )
        write_json(run_dir / "input_manifest.json", input_manifest)

        model_info = collect_model_info(model_settings, load_time_sec)
        write_json(run_dir / "model_info.json", model_info)

        summary = summarize_frame_metrics(frame_metrics, timed_wall_sec)
        payload = {
            "run": collect_run_info(args, run_dir, started_at),
            "model": model_info,
            "input": input_manifest,
            "summary": summary,
            "warmup_frames_completed": warmup_count,
        }
        write_summary_json(run_dir / "summary.json", payload)
        write_summary_markdown(run_dir / "summary.md", payload)

        print(f"[BENCH] Frames benchmarked: {summary['benchmarked_frames']}")
        print(f"[BENCH] Effective FPS: {summary['effective_fps']}")
        print(f"[BENCH] Summary: {run_dir / 'summary.md'}")

    finally:
        if segmenter is not None:
            segmenter.cleanup()
        if telemetry_logger:
            telemetry_logger.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark segmentation model runtime and Linux/NVIDIA telemetry.",
    )
    parser.add_argument("--config", default="config.yaml", help="YAML config path")
    parser.add_argument(
        "--video",
        default="assets/test.mp4",
        help="Input video used for benchmark frames unless --camera-index is set",
    )
    parser.add_argument(
        "--camera-index",
        type=int,
        help="Open a connected camera by index instead of reading --video",
    )
    parser.add_argument(
        "--camera-backend",
        choices=["auto", "v4l2"],
        default="auto",
        help="OpenCV backend used for --camera-index on Linux",
    )
    parser.add_argument(
        "--camera-width",
        type=int,
        help="Requested camera capture width before benchmarking",
    )
    parser.add_argument(
        "--camera-height",
        type=int,
        help="Requested camera capture height before benchmarking",
    )
    parser.add_argument(
        "--camera-fps",
        type=float,
        help="Requested camera capture FPS before benchmarking",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmarks/results",
        help="Directory where benchmark run folders are written",
    )
    parser.add_argument("--model-type", choices=["yolo", "rfdetr"])
    parser.add_argument("--model-path", help="Override model weights path")
    parser.add_argument(
        "--confidence", type=float, help="Override confidence threshold"
    )
    parser.add_argument("--frame-interval", type=int, default=1)
    parser.add_argument("--warmup-frames", type=int, default=30)
    parser.add_argument("--max-frames", type=int, default=300)
    parser.add_argument("--imgsz", type=int, help="Ultralytics YOLO inference size")
    parser.add_argument(
        "--device",
        help="Model runtime device passed to Ultralytics, for example '0' or 'cpu'",
    )
    parser.add_argument(
        "--resize",
        help="Optional input resize before inference, formatted WIDTHxHEIGHT",
    )
    parser.add_argument(
        "--telemetry",
        choices=["nvidia-smi", "none"],
        default="nvidia-smi",
        help="Hardware telemetry backend",
    )
    parser.add_argument("--telemetry-interval-sec", type=float, default=1.0)
    parser.add_argument("--gpu-index", type=int, default=0)
    parser.add_argument(
        "--hardware-label",
        default="linux-nvidia",
        help="Label used in the benchmark run folder name",
    )
    return parser.parse_args()


def resolve_model_settings(
    args: argparse.Namespace,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    model_type = args.model_type or config.get("model_type", "yolo")
    models = config.get("models", {})
    model_config = models.get(model_type, {})

    model_path = args.model_path or model_config.get("weights_path")
    if not model_path:
        raise ValueError(f"No model path configured for model_type={model_type}")

    confidence = args.confidence
    if confidence is None:
        confidence = float(model_config.get("confidence_threshold", 0.25))

    trapezoid = config.get("detection_region", {}).get("trapezoid_coords") or [
        [0, 0],
        [1, 0],
        [1, 1],
        [0, 1],
    ]

    return {
        "model_type": model_type,
        "model_path": str(Path(model_path)),
        "confidence_threshold": float(confidence),
        "trapezoid_coords": np.array(trapezoid, dtype=np.float32),
    }


def resolve_input_source(args: argparse.Namespace) -> Dict[str, Any]:
    """Resolve the benchmark frame source from CLI arguments."""
    if args.camera_index is not None:
        return {
            "source_type": "camera",
            "camera_index": args.camera_index,
            "camera_backend": args.camera_backend,
            "requested_width": args.camera_width,
            "requested_height": args.camera_height,
            "requested_fps": args.camera_fps,
        }

    return {
        "source_type": "video",
        "video_path": args.video,
    }


def run_capture_benchmark(
    input_source: Dict[str, Any],
    runner: TimedSegmenterRunner,
    frame_interval: int,
    warmup_frames: int,
    max_frames: int,
    resize: Optional[Tuple[int, int]],
    output_path: Path,
) -> Tuple[int, List[Dict[str, Any]], float, Dict[str, Any]]:
    cap = open_capture(input_source)
    input_manifest = collect_input_manifest(cap, input_source)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame_metrics: List[Dict[str, Any]] = []
    warmup_count = 0
    timed_started_at: Optional[float] = None
    timed_finished_at: Optional[float] = None

    try:
        with output_path.open("w", encoding="utf-8") as f:
            for source_frame_index, frame_bgr in iter_sampled_frames(
                cap,
                frame_interval,
                resize,
            ):
                if warmup_count >= warmup_frames and len(frame_metrics) >= max_frames:
                    break

                frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)

                if warmup_count < warmup_frames:
                    runner.run(frame_rgb)
                    warmup_count += 1
                    continue

                if timed_started_at is None:
                    timed_started_at = time.perf_counter()

                metric = runner.run(frame_rgb)
                metric.update(
                    {
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "benchmark_frame_index": len(frame_metrics) + 1,
                        "source_frame_index": source_frame_index,
                        "width": int(frame_rgb.shape[1]),
                        "height": int(frame_rgb.shape[0]),
                    }
                )
                frame_metrics.append(metric)
                f.write(json.dumps(metric, sort_keys=True) + "\n")
                if len(frame_metrics) >= max_frames:
                    break

            timed_finished_at = time.perf_counter()
    finally:
        cap.release()

    if not frame_metrics:
        raise RuntimeError(
            "No frames were benchmarked. Check video length and frame limits."
        )

    timed_wall_sec = (timed_finished_at or time.perf_counter()) - (
        timed_started_at or timed_finished_at or time.perf_counter()
    )
    input_manifest["benchmarked_frames"] = len(frame_metrics)
    input_manifest["warmup_frames"] = warmup_count
    return warmup_count, frame_metrics, timed_wall_sec, input_manifest


def open_capture(input_source: Dict[str, Any]) -> cv2.VideoCapture:
    """Open either a file or camera capture source."""
    if input_source["source_type"] == "camera":
        camera_index = int(input_source["camera_index"])
        backend = input_source.get("camera_backend", "auto")
        if backend == "v4l2":
            cap = cv2.VideoCapture(camera_index, cv2.CAP_V4L2)
        else:
            cap = cv2.VideoCapture(camera_index)

        if input_source.get("requested_width"):
            cap.set(cv2.CAP_PROP_FRAME_WIDTH, int(input_source["requested_width"]))
        if input_source.get("requested_height"):
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, int(input_source["requested_height"]))
        if input_source.get("requested_fps"):
            cap.set(cv2.CAP_PROP_FPS, float(input_source["requested_fps"]))

        if not cap.isOpened():
            raise RuntimeError(f"Could not open camera index: {camera_index}")
        return cap

    video_path = str(input_source["video_path"])
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open benchmark video: {video_path}")
    return cap


def iter_sampled_frames(
    cap: cv2.VideoCapture,
    frame_interval: int,
    resize: Optional[Tuple[int, int]],
) -> Iterable[Tuple[int, np.ndarray]]:
    source_frame_index = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        source_frame_index += 1
        if frame_interval > 1 and source_frame_index % frame_interval != 0:
            continue

        if resize is not None:
            frame = cv2.resize(frame, resize, interpolation=cv2.INTER_AREA)

        yield source_frame_index, frame


def build_telemetry_logger(
    args: argparse.Namespace,
    run_dir: Path,
) -> Optional[TelemetryLogger]:
    if args.telemetry == "none":
        return None

    return TelemetryLogger(
        provider=NvidiaSmiTelemetry(gpu_index=args.gpu_index),
        output_path=run_dir / "hardware_metrics.csv",
        interval_sec=args.telemetry_interval_sec,
    )


def create_run_dir(args: argparse.Namespace, model_settings: Dict[str, Any]) -> Path:
    created = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    model_name = Path(model_settings["model_path"]).stem
    run_name = "_".join(
        [
            created,
            _slug(args.hardware_label),
            _slug(model_settings["model_type"]),
            _slug(model_name),
        ]
    )
    run_dir = Path(args.output_dir) / run_name
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def collect_model_info(
    model_settings: Dict[str, Any],
    load_time_sec: float,
) -> Dict[str, Any]:
    model_path = Path(model_settings["model_path"])
    return {
        "model_type": model_settings["model_type"],
        "model_path": str(model_path),
        "model_sha256": sha256_file(model_path),
        "confidence_threshold": model_settings["confidence_threshold"],
        "load_time_sec": load_time_sec,
    }


def collect_input_manifest(
    cap: cv2.VideoCapture,
    input_source: Dict[str, Any],
) -> Dict[str, Any]:
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 0)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)

    if input_source["source_type"] == "camera":
        return {
            "source_type": "camera",
            "camera_index": input_source["camera_index"],
            "camera_backend": input_source.get("camera_backend", "auto"),
            "requested_width": input_source.get("requested_width") or "",
            "requested_height": input_source.get("requested_height") or "",
            "requested_fps": input_source.get("requested_fps") or "",
            "observed_width": width,
            "observed_height": height,
            "observed_fps": round(fps, 6),
            "frame_count": "",
            "duration_sec": "",
            "sha256": "",
        }

    video_path = str(input_source["video_path"])
    return {
        "source_type": "video",
        "video_path": str(Path(video_path)),
        "sha256": sha256_file(Path(video_path)),
        "fps": round(fps, 6),
        "frame_count": frame_count,
        "width": width,
        "height": height,
        "duration_sec": round(frame_count / fps, 6) if fps > 0 else "",
    }


def collect_device_info(gpu_index: int) -> Dict[str, Any]:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
        "hostname": platform.node(),
        "nvidia_smi_path": shutil.which("nvidia-smi") or "",
        "gpu": query_gpu_info(gpu_index),
        "packages": collect_package_versions(),
        "git": collect_git_info(),
    }


def query_gpu_info(gpu_index: int) -> Dict[str, Any]:
    cmd = [
        "nvidia-smi",
        "-i",
        str(gpu_index),
        "--query-gpu=index,name,uuid,driver_version,memory.total",
        "--format=csv,noheader,nounits",
    ]
    try:
        completed = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except Exception as exc:
        return {"error": f"nvidia-smi unavailable: {exc!r}"}

    values = [part.strip() for part in completed.stdout.strip().split(",")]
    keys = ["index", "name", "uuid", "driver_version", "memory_total_mb"]
    return dict(zip(keys, values))


def collect_package_versions() -> Dict[str, str]:
    try:
        from importlib import metadata
    except ImportError:  # pragma: no cover - Python 3.7 fallback only
        import importlib_metadata as metadata  # type: ignore

    versions: Dict[str, str] = {}
    for package in IMPORTANT_PACKAGES:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = ""
    return versions


def collect_git_info() -> Dict[str, Any]:
    return {
        "commit": run_git(["rev-parse", "HEAD"]),
        "branch": run_git(["rev-parse", "--abbrev-ref", "HEAD"]),
        "dirty": bool(run_git(["status", "--short"])),
    }


def collect_run_info(
    args: argparse.Namespace,
    run_dir: Path,
    started_at: datetime,
) -> Dict[str, Any]:
    git = collect_git_info()
    return {
        "run_id": run_dir.name,
        "created_at_utc": started_at.isoformat(),
        "command": " ".join(sys.argv),
        "git_commit": git.get("commit", ""),
        "git_branch": git.get("branch", ""),
        "git_dirty": git.get("dirty", ""),
    }


def write_run_config(
    output_path: Path,
    args: argparse.Namespace,
    model_settings: Dict[str, Any],
    input_source: Dict[str, Any],
) -> None:
    payload = {
        "benchmark": vars(args),
        "input_source": input_source,
        "model": {
            "model_type": model_settings["model_type"],
            "model_path": model_settings["model_path"],
            "confidence_threshold": model_settings["confidence_threshold"],
            "trapezoid_coords": model_settings["trapezoid_coords"].tolist(),
        },
    }
    output_path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def load_yaml(path: str) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_resize(value: Optional[str]) -> Optional[Tuple[int, int]]:
    if not value:
        return None
    if "x" not in value.lower():
        raise ValueError("--resize must use WIDTHxHEIGHT format")
    width_text, height_text = value.lower().split("x", 1)
    width = int(width_text)
    height = int(height_text)
    if width <= 0 or height <= 0:
        raise ValueError("--resize dimensions must be positive")
    return width, height


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_git(args: List[str]) -> str:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
        return completed.stdout.strip()
    except Exception:
        return ""


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 6)


def _synchronize_cuda() -> None:
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.synchronize()
    except Exception:
        return


def _slug(value: object) -> str:
    text = str(value).strip().lower()
    return "".join(ch if ch.isalnum() else "-" for ch in text).strip("-")


if __name__ == "__main__":
    main()
