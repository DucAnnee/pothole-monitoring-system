# Edge Segmentation Benchmarks

This folder contains runtime benchmarks for the segmentation model only. It is
kept separate from `main.py` so uploader, surface-area estimation, Kafka, MinIO,
and OpenCV display do not distort model latency.

## RTX/Linux Benchmark

Run the default YOLO benchmark against the sample video:

```bash
python -m benchmarks.benchmark_model \
  --config config.yaml \
  --video assets/test.mp4 \
  --model-type yolo \
  --model-path models/yolo11s.pt \
  --device 0 \
  --imgsz 640 \
  --warmup-frames 30 \
  --max-frames 300 \
  --telemetry nvidia-smi \
  --telemetry-interval-sec 1
```

Run the same benchmark against a connected Linux camera:

```bash
python -m benchmarks.benchmark_model \
  --config config.yaml \
  --camera-index 0 \
  --camera-backend v4l2 \
  --camera-width 1280 \
  --camera-height 720 \
  --camera-fps 30 \
  --model-type yolo \
  --model-path models/yolo11s.pt \
  --device 0 \
  --imgsz 640 \
  --warmup-frames 30 \
  --max-frames 300 \
  --telemetry nvidia-smi
```

For CPU-only smoke tests, use:

```bash
python -m benchmarks.benchmark_model \
  --config config.yaml \
  --video assets/test.mp4 \
  --device cpu \
  --telemetry none \
  --warmup-frames 2 \
  --max-frames 5
```

## Outputs

Each run writes a timestamped folder under `benchmarks/results/`:

```text
benchmarks/results/<timestamp>_<hardware>_<model-type>_<model-name>/
  device_info.json
  frame_metrics.jsonl
  hardware_metrics.csv        # when telemetry is enabled
  input_manifest.json
  model_info.json
  run_config.yaml
  summary.json
  summary.md
```

`frame_metrics.jsonl` records per-frame segmentation timings:

- `preprocess_ms`: detection-region masking and frame preparation
- `inference_ms`: model prediction time
- `postprocess_ms`: mask extraction and ROI filtering
- `total_ms`: total segmentation path for that frame
- `detections_raw`: valid model masks before ROI filtering
- `detections_after_roi`: masks accepted inside the trapezoid

`hardware_metrics.csv` records sampled CPU/RAM/GPU telemetry from `nvidia-smi`
and `psutil`. On Jetson, add a separate telemetry backend that writes the same
columns from `tegrastats` so RTX and Jetson benchmark reports remain comparable.

## Audit Notes

Keep benchmark reports with:

- git commit and dirty state from `device_info.json` / `summary.json`
- model SHA256 from `model_info.json`
- video SHA256 from `input_manifest.json`, or camera index/backend and observed
  capture properties for live camera runs
- full command and effective config from `summary.md` and `run_config.yaml`

RTX results are useful for validating the benchmark method and comparing model
choices, but they are not a substitute for final Jetson measurements.
