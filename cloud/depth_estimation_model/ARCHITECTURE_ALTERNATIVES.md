# Depth Estimation — Architecture Alternatives

## Implemented: Option A — Thin Triton (current)

The depth service owns all pre/postprocessing. Triton owns only the neural net forward pass.

```
Depth Service                         Triton
─────────────────────────────         ────────────────────────────
image bytes (from MinIO)
  │
  ▼ ImagePreprocessor
  │  - cv2.resize(518, 518)
  │  - BGR→RGB
  │  - ImageNet normalize
  │  → [N, 3, 518, 518] float32
  │
  ▼ TritonDepthClient.infer_batch()  ──► ONNX onnxruntime backend
                                         forward(x) → depth_map
  ◄── [N, 518, 518] float32 ───────────
  │
  ▼ postprocess_depth_map()
     - center-ROI extraction
     - normalize to cm range
     - confidence from std
     → (depth_cm, confidence)
```

**Tradeoffs**:
- Preprocessing runs in CPU in the depth service process
- Pre/post logic is plain Python — easy to test, debug, and change
- Adding a second consumer of depth estimates does not reuse the preprocessing
- Model boundary is clean: Triton input/output are well-defined tensors

---

## Alternative: Option B — Fat Triton (Ensemble Pipeline)

Triton owns preprocessing, neural net, and postprocessing as a three-model ensemble.
The depth service sends raw image bytes and receives `(depth_cm, confidence)` directly.

```
Depth Service                         Triton Ensemble: depth_pipeline
─────────────────────────────         ─────────────────────────────────────
image bytes (from MinIO)             Step 1: preprocessing (Python backend)
  │                                    - accepts raw bytes tensor [N, H*W]
  ▼                                    - decodes + resizes + normalizes
  TritonDepthClient.infer()  ───────►  → [N, 3, 518, 518]
                                      Step 2: depth_anything_v2 (onnxruntime)
                                        - forward(x) → [N, 518, 518]
                                      Step 3: postprocessing (Python backend)
                                        - center-ROI, normalize, confidence
                                        → (depth_cm float32, confidence float32)
  ◄── (depth_cm, confidence) ───────
```

### ensemble config.pbtxt (depth_pipeline)

```protobuf
name: "depth_pipeline"
platform: "ensemble"
max_batch_size: 8

input [{ name: "image_bytes" data_type: TYPE_UINT8 dims: [-1] }]
output [
  { name: "depth_cm"    data_type: TYPE_FP32 dims: [1] },
  { name: "confidence"  data_type: TYPE_FP32 dims: [1] }
]

ensemble_scheduling {
  step [
    {
      model_name: "depth_preprocessing"
      model_version: -1
      input_map  { key: "image_bytes"  value: "image_bytes" }
      output_map { key: "image_tensor" value: "image_tensor" }
    },
    {
      model_name: "depth_anything_v2"
      model_version: -1
      input_map  { key: "input"     value: "image_tensor" }
      output_map { key: "depth_map" value: "depth_map" }
    },
    {
      model_name: "depth_postprocessing"
      model_version: -1
      input_map  { key: "depth_map"  value: "depth_map" }
      output_map {
        key: "depth_cm"   value: "depth_cm"
      }
      output_map {
        key: "confidence" value: "confidence"
      }
    }
  ]
}
```

### Model repo layout for Option B

```
triton_model_repo/
├── depth_anything_v2/          ← same ONNX model
│   ├── config.pbtxt
│   └── 1/model.onnx
├── depth_preprocessing/        ← Python backend
│   ├── config.pbtxt
│   └── 1/model.py
├── depth_postprocessing/       ← Python backend
│   ├── config.pbtxt
│   └── 1/model.py
└── depth_pipeline/             ← ensemble
    ├── config.pbtxt
    └── 1/.gitkeep
```

### Tradeoffs vs Option A

| | Option A (current) | Option B |
|---|---|---|
| Preprocessing location | Depth service (CPU) | Triton Python backend (CPU, but can be moved to C++ CUDA) |
| Depth service complexity | Medium (ImagePreprocessor class) | Low (just bytes in, scalars out) |
| Triton config complexity | Low (1 model) | High (3 models + ensemble) |
| Debugging inference issues | Easy (step through Python) | Harder (logs split across Triton models) |
| Reusability | Pre/post duplicated if second service needs depth | Pre/post centralized — any Triton client reuses |
| Path to GPU preprocessing | Requires C++ backend rewrite | Same Python, swap to C++ later |
| Testing | Unit test preprocessor directly | Must mock or run Triton for integration tests |

### When to prefer Option B

- A second service (e.g., BEV service) also needs depth estimates with same preprocessing
- GPU-accelerated preprocessing is required (image decode, resize on GPU)
- Preprocessing logic is stable and unlikely to change often
- Triton ensemble monitoring/tracing provides needed observability per stage
