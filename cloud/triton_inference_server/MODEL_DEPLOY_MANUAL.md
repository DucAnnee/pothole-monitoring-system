# Triton Model Deployment Manual

Reference for deploying and managing models in this project's Triton Inference Server.

---

## Prerequisites

### Host requirements

| Requirement | How to verify |
|---|---|
| NVIDIA driver ≥ 550 | `nvidia-smi` |
| Docker Engine ≥ 24 | `docker version` |
| NVIDIA Container Toolkit | `docker run --rm --gpus all nvidia/cuda:12.0-base nvidia-smi` |
| `docker-compose` plugin | `docker compose version` |

### Python environment

The export script runs in the `pot2` conda environment which has `torch`, `onnxscript`, and the `Depth-Anything-V2` source.

```bash
conda activate pot2
```

---

## Current Models

| Model name | Version | Backend | GPU | Source weights |
|---|---|---|---|---|
| `depth_anything_v2` | `1` | onnxruntime | RTX 5060 (GPU 0) | `cloud/depth_estimation_model/Depth-Anything-V2/depth_anything_v2_vitl.pth` |

---

## First-time Setup

### 1. Export model to ONNX

Model weights are not in the repo. Run the export script once per machine (or after changing model type/weights):

```bash
conda activate pot2
python cloud/depth_estimation_model/export_to_onnx.py
```

Expected output:
```
[INFO] Device: cuda
[INFO] Loading vitl weights from .../depth_anything_v2_vitl.pth ...
[INFO] Exporting to .../triton_model_repo/depth_anything_v2/1/model.onnx ...
[SUCCESS] ONNX model written to ...
          Input:  [N, 3, 518, 518] float32 (ImageNet normalized)
          Output: [N, 518, 518] float32 (relative depth)
```

This produces two files in `triton_model_repo/depth_anything_v2/1/`:
- `model.onnx` — 1.8 MB index
- `model.onnx.data` — ~1.3 GB weights

Both are gitignored. Both must be present.

**Optional args:**
```bash
python export_to_onnx.py --weights /path/to/custom.pth --model-type vitb --output /path/to/model.onnx
```

Supported `--model-type` values: `vits`, `vitb`, `vitl`, `vitg`

### 2. Start Triton

```bash
docker compose up triton-inference-server -d
```

### 3. Verify Triton is healthy

```bash
# Server ready
curl http://localhost:8000/v2/health/ready

# Model loaded and READY
curl http://localhost:8000/v2/models/depth_anything_v2
```

Expected model response (truncated):
```json
{
  "name": "depth_anything_v2",
  "versions": ["1"],
  "state": "READY",
  ...
}
```

> **Note:** ViT-L takes ~15 seconds to load on GPU. If the model is `LOADING`, wait and retry.

---

## Day-to-Day Operations

### Start / stop

```bash
docker compose up triton-inference-server -d      # start
docker compose stop triton-inference-server        # stop (preserves container)
docker compose down                                # stop + remove all containers
```

### View logs

```bash
docker logs triton-inference-server -f
```

### Check metrics (Prometheus)

```bash
curl http://localhost:8002/metrics | grep nv_inference_request_success
```

Key metrics:
| Metric | Meaning |
|---|---|
| `nv_inference_request_success` | Total successful inference requests |
| `nv_inference_queue_duration_us` | Time requests spent in dynamic batch queue |
| `nv_inference_exec_duration_us` | GPU execution time per request |
| `nv_gpu_utilization` | GPU utilization % |

---

## Adding a New Model

### 1. Create model directory

```
triton_model_repo/
└── <model_name>/
    ├── config.pbtxt
    └── 1/
        └── model.onnx       ← your ONNX file (+ model.onnx.data if external data)
```

### 2. Write `config.pbtxt`

Minimum required config for an ONNX model:

```protobuf
name: "<model_name>"
backend: "onnxruntime"
max_batch_size: 8

input [{
  name: "<input_tensor_name>"    # must match ONNX graph input name
  data_type: TYPE_FP32
  dims: [ <C>, <H>, <W> ]        # per-sample shape, exclude batch dim
}]

output [{
  name: "<output_tensor_name>"   # must match ONNX graph output name
  data_type: TYPE_FP32
  dims: [ <H>, <W> ]
}]

dynamic_batching {
  preferred_batch_size: [ 1, 4, 8 ]
  max_queue_delay_microseconds: 50000
}

instance_group [{ kind: KIND_GPU count: 1 gpus: [ 0 ] }]
```

To find ONNX input/output names:
```python
import onnx
model = onnx.load("model.onnx")
print([i.name for i in model.graph.input])
print([o.name for o in model.graph.output])
```

### 3. Load model without restarting Triton

Triton supports live model loading if `--model-control-mode=explicit` is set. Current setup uses the default (poll mode) — Triton auto-detects new directories on startup. Simplest path: restart the service.

```bash
docker compose restart triton-inference-server
```

### 4. Verify new model loaded

```bash
curl http://localhost:8000/v2/models/<model_name>
```

---

## Updating an Existing Model (new weights, same architecture)

1. Re-export ONNX to the same path (overwrites existing files):
   ```bash
   python cloud/depth_estimation_model/export_to_onnx.py
   ```

2. Restart Triton to reload:
   ```bash
   docker compose restart triton-inference-server
   ```

3. Verify model state:
   ```bash
   curl http://localhost:8000/v2/models/depth_anything_v2
   ```

> **Version directories**: To serve multiple versions simultaneously, create `2/model.onnx` alongside `1/model.onnx`. Triton serves all versions; the depth service pinpoints version `"1"` via `config_loader.get_triton_model_version()`.

---

## Troubleshooting

### Model shows `UNAVAILABLE` after startup

**Cause A**: `model.onnx` or `model.onnx.data` missing.
```bash
ls cloud/triton_inference_server/triton_model_repo/depth_anything_v2/1/
# Must show both model.onnx AND model.onnx.data
```
Fix: re-run `export_to_onnx.py`.

**Cause B**: `config.pbtxt` shape mismatch with actual ONNX graph.
```bash
docker logs triton-inference-server | grep -i "error\|fail\|unavailable"
```

**Cause C**: ONNX exported with torch 2.9 default (onnxscript) exporter — output batch dim is concretized to `1`.

Symptom in logs:
```
tensor 'depth_map': for the model to support batching the shape should have at least 1
dimension and the first dimension must be -1; but shape expected by the model is [1,518,518]
```

Fix: ensure `export_to_onnx.py` passes `dynamo=False` to `torch.onnx.export`, then re-export:
```bash
conda activate pot2
python cloud/depth_estimation_model/export_to_onnx.py
```

The script already includes `dynamo=False`. This only happens if the script is edited to remove it.

### Depth service crashes on startup with "model not ready"

`wait_for_triton_ready()` retries 20 × 5s = 100s before raising. If Triton takes longer (first pull, slow disk), increase retries or `start_period` in `healthcheck`.

### GPU OOM during inference

RTX 5060 has 8 GB VRAM. ViT-L ONNX uses ~1.3 GB. Reduce `max_batch_size` in `config.pbtxt` and `batching.max_batch_size` in `cloud/depth_estimation_model/config.yaml` (keep them in sync).

### Port 8001 already in use

Check for conflicting services:
```bash
netstat -ano | findstr :8001
```

Change the host port in `docker-compose.yml` (`"8001:8001"` → `"18001:8001"`) and update `triton.grpc_port` in `config.yaml`.

### `xFormers not available` warning during export

Harmless. DepthAnythingV2 prints this if xFormers is not installed. Export succeeds regardless.

---

## File Reference

| File | Purpose | In git? |
|------|---------|---------|
| `triton_model_repo/depth_anything_v2/config.pbtxt` | Triton model config | Yes |
| `triton_model_repo/depth_anything_v2/1/.gitkeep` | Keeps version dir tracked | Yes |
| `triton_model_repo/depth_anything_v2/1/model.onnx` | ONNX model index | **No** (gitignored) |
| `triton_model_repo/depth_anything_v2/1/model.onnx.data` | ONNX weights (~1.3 GB) | **No** (gitignored) |
| `cloud/depth_estimation_model/export_to_onnx.py` | Generates model.onnx from .pth | Yes |
| `docker-compose.yml` → `triton-inference-server` | Container definition | Yes |
