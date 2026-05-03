# Triton Integration — Implementation Notes

Notes and concerns raised during planning. These document decisions to make in the implementation session — they do not prescribe changes to the current pipeline logic.

---

## 1. Preprocessing: `infer_image` vs ONNX `forward()`

**Concern**: `DepthAnythingV2.infer_image()` (the current inference path) does the following before calling `forward()`:

```python
def image2tensor(self, raw_image, input_size=518):
    transform = Compose([
        Resize(
            width=input_size,
            height=input_size,
            resize_target=False,
            keep_aspect_ratio=True,   # <-- may produce non-518 spatial dims
            ...
        ),
        NormalizeImage(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        PrepareForNet(),
    ])
```

With `keep_aspect_ratio=True`, a wide image (e.g., 1280×720) resizes to 518×291, not 518×518. `infer_image` then calls `F.interpolate` to return the depth map at the *original* resolution. The model itself (`forward()`) therefore receives variable spatial dimensions per image.

**Impact on ONNX export**: If the ONNX model is exported with fixed input shape `[1, 3, 518, 518]`, but the preprocessing produces `[1, 3, 518, 291]` for wide images, there is a shape mismatch at inference time.

**Options to resolve — decision required during implementation**:

| Option | Description | Tradeoff |
|---|---|---|
| A. Export with dynamic axes | `dynamic_axes={"input": {2: "height", 3: "width"}, ...}` — allows any HxW | ONNX dynamic shapes work but may reduce onnxruntime optimization; Triton config.pbtxt needs `dims: [3, -1, -1]` |
| B. Force exact 518×518 resize | Preprocessing in depth service uses `cv2.resize(img, (518, 518))` (no aspect ratio) — fixed ONNX shape | Slight geometric distortion for non-square images; acceptable given center-ROI postprocessing is already an approximation |
| C. Letterbox padding | Resize to fit within 518×518 preserving aspect ratio, pad remainder with zeros | Matches `keep_aspect_ratio=True` behavior more closely; adds padding logic to preprocessor |

The choice affects `config.pbtxt` input dims and `ImagePreprocessor.preprocess()` implementation. Settle this before writing either.

---

## 2. Commit Strategy with Batching

**Concern**: Current pipeline: one poll → one message → one commit. Batching accumulates N messages before a single Triton call. If the Triton call fails partway, some messages in the batch may already have been published while others have not. With `enable.auto.commit: False`, uncommitted messages are replayed on restart — this is safe for at-least-once, but may cause duplicate depth records downstream.

**Decision required**: Whether to commit per-message after each individual publish within the batch (safest, more commit calls) or commit the entire batch as a unit after all publishes succeed (fewer commits, but any failure replays the whole batch).

Per-message commit is recommended for consistency with the existing pattern.

---

## 3. Triton Connection Retry on Startup

**Concern**: `DepthEstimator._load_model()` fails fast if weights file is missing. With Triton, the equivalent failure is Triton not yet ready (model still loading — ViT-L takes ~15s). The depth service must not exit on first connection failure.

**Decision required**: Add startup retry loop (e.g., `wait_for_triton_ready(host, port, model_name, retries=20, delay=5s)`) before entering the Kafka poll loop. Without this, the depth service container will crash-loop on startup if it starts before Triton finishes loading the model.

---

## 4. `surface_area_cm2` Passthrough in Batch Context

**Concern**: In the current single-message flow, `surface_area_cm2` is read from the event and directly placed in the output record. In the batch flow, N events are accumulated, sent as one tensor batch to Triton, and results are returned as `[N, 518, 518]`. The results must be re-associated with their source events by index to correctly attach each event's `surface_area_cm2` to its output record.

This is straightforward (zip `batch_events` with `depth_maps` by index) but must be kept explicit — do not rely on dict keying or event_id lookup to match results.

---

## 5. Kafka Poll Timeout vs Batch Timeout Interaction

**Concern**: The `BatchAccumulator` uses `time.monotonic()` to check whether the batch timeout has elapsed. If `consumer.poll(timeout=1.0)` is used (current value), the minimum batch latency is 1 second regardless of `batch_timeout_ms` setting.

**Decision required**: Set `consumer.poll(timeout=0.05)` (50ms) in the new main loop so the timeout check fires roughly every 50ms. This increases CPU polling frequency but keeps batch latency controlled by `batch_timeout_ms`.
