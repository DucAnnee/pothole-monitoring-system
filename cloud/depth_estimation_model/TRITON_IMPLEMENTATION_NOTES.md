# Triton Integration - Implementation Notes

The Triton migration is implemented. `cloud_pipeline.py` now uses
`TritonDepthClient`, `ImagePreprocessor`, `BatchAccumulator`, and a short-poll
Kafka loop to batch surface-area events before calling Triton over gRPC.

For current runtime context, read:

- `cloud/depth_estimation_model/.claude/contexts/depth_estimation_model-context.md`
- `cloud/triton_inference_server/.claude/contexts/triton-inference-server-context.md`

## Implemented Decisions

### Preprocessing Shape

The service uses exact `518x518` resize:

```python
cv2.resize(img, (518, 518))
```

The ONNX model and Triton `config.pbtxt` therefore use input dims
`[3, 518, 518]`. This trades aspect-ratio fidelity for a stable fixed-shape
serving contract.

### Commit Strategy

Kafka uses `enable.auto.commit: False`. Each source message is committed only
after its depth record has been successfully produced to `pothole.depth.v1`.

If a batch fails before publishing all events, uncommitted messages are replayed.
This preserves at-least-once behavior and can create duplicate depth records
downstream.

### Triton Startup

The depth service must wait for Triton and the `depth_anything_v2` model before
entering the Kafka poll loop. Triton is started with `--exit-on-error=false`, so
the server can be healthy while a model is still unavailable.

### Surface Area Passthrough

`surface_area_cm2` is copied from each `pothole.surface.area.v2` event to the
matching `pothole.depth.v1` output. Batch processing keeps this association by
list order:

```text
PendingEvent list -> preprocessed tensor stack -> Triton output batch -> zip back by index
```

Do not reorder events, tensors, or depth maps inside `_process_batch()`.

### Poll Timeout

The batch timeout relies on frequent loop iterations. Keep Kafka polling short
enough that `batch_timeout_ms` controls latency instead of the poll interval.

## Remaining Risks

- Exact square resize can distort wide images.
- Depth is still relative and postprocessed with a center-ROI heuristic.
- BEV failures fall back to raw image and can still produce downstream severity.
- Duplicate depth records are possible under replay.
- Model artifacts are external and must be generated before Triton can serve.
