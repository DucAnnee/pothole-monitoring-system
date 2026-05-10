# SAM3 LoRA API Guide

This document is a practical API reference for developers who want to use SAM3 in this folder without deep AI specialization.

It covers:
- Inference API entry points
- Training API entry points
- Validation API entry points
- LoRA variant selection (all configs in configs/)

## 1) What Is The API Surface?

The operational entry points are these scripts:
- train.py: fine-tune SAM3 with LoRA
- validate.py: run full evaluation (mAP and cgF1)
- inference.py: run prediction and visualization on images

There are also Python-callable entry points:
- inference.py -> class SAM3LoRAInference
- validate.py -> function validate(...)

## 2) LoRA Variants (All Supported)

Each variant is selected by passing a config file via --config.

Available configs:
- configs/minimal_lora_config.yaml
- configs/light_lora_config.yaml
- configs/full_lora_config.yaml
- configs/crack_detection_config.yaml
- configs/base_config.yaml
- configs/sam3_lora_standalone.yaml (legacy format, not aligned with current train.py keys)

Recommended variants for current scripts:
- Minimal: fastest and lowest memory, adapts mainly DETR decoder
- Light: balanced memory and quality
- Full: maximum adaptation, highest compute and memory
- Crack detection: tuned for crack-like defects and text support

Important compatibility note:
- train.py expects training.data_dir (root containing train/ and valid/ subfolders).
- Some older configs use train_data_path or val_data_path; update them to training.data_dir for train.py.

## 3) Data Contract

Training data root (used by train.py) must look like:

```text
<data_root>/
  train/
    _annotations.coco.json
    <images...>
  valid/
    _annotations.coco.json
    <images...>
```

Validation data dir (used by validate.py --val_data_dir) must look like:

```text
<val_data_dir>/
  _annotations.coco.json
  <images...>
```

COCO segmentation accepted:
- Polygon segmentation lists
- COCO RLE segmentation objects

## 4) Training API

## CLI entry point

```bash
python3 train.py --config configs/minimal_lora_config.yaml
```

### Key arguments
- --config: YAML config path (default: configs/minimal_lora_config.yaml)
- --sam-checkpoint: local path to the original SAM checkpoint (default: ./asset/sam.pt)
- --device: one or more GPU ids (examples: --device 0, --device 0 1)
- --master_port: DDP port for multi-GPU launch (default: 29500)

### Multi-GPU behavior
- If multiple IDs are passed to --device, train.py auto-launches torch.distributed.run.
- No separate torchrun command is required for the common case.

### Outputs
Saved under output.output_dir from config:
- best_lora_weights.pt
- last_lora_weights.pt
- val_stats.json

### Runtime behavior
- Training computes train loss each epoch.
- If valid/ exists, it computes validation loss and picks best_lora_weights.pt by lowest val loss.
- Full metrics are not computed during training.

## 5) Validation API

## CLI entry point

```bash
python3 validate.py \
  --config configs/minimal_lora_config.yaml \
  --weights outputs/sam3_lora_minimal/best_lora_weights.pt \
  --val_data_dir /path/to/data/valid
```

### Key arguments
- --config: LoRA config (required unless --use-base-model)
- --weights: LoRA weights path (required unless --use-base-model)
- --val_data_dir: required, directory containing _annotations.coco.json
- --use-base-model: evaluate SAM3 without LoRA for baseline
- --num-samples: limit sample count for quick debugging
- --prob-threshold: prediction score threshold (default: 0.3)
- --nms-iou: NMS IoU threshold (default: 0.7)
- --merge: aggressively merge overlapping segments (useful for crack-like structures)
- --merge-iou: merge IoU threshold (default: 0.15)
- --sam-checkpoint: local SAM checkpoint path

### Metrics produced
- COCO mAP (0.50:0.95)
- mAP@50
- mAP@75
- cgF1 (including @50 and @75)

## Python entry point

```python
from validate import validate

validate(
    config_path="configs/minimal_lora_config.yaml",
    weights_path="outputs/sam3_lora_minimal/best_lora_weights.pt",
    val_data_dir="/path/to/data/valid",
    sam_checkpoint="./asset/sam.pt",
    prob_threshold=0.3,
    nms_iou=0.7,
    merge_cracks=False,
)
```

## 6) Inference API

## CLI entry point (text prompt)

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image /path/to/image.jpg \
  --prompt pothole \
  --output ./asset/output.png
```

### Key arguments
- --config: LoRA config YAML (required)
- --weights: optional LoRA weights path
  - if omitted, inference.py auto-resolves to output.output_dir/best_lora_weights.pt
- --image: input image path (required)
- --prompt: one or more text prompts (default: object)
- --box: repeatable visual box prompt (x1 y1 x2 y2)
- --point: repeatable point prompt (x y label), label must be 0 or 1
- --interactive: true/false, opens Tkinter prompt UI
- --threshold: detection confidence threshold
- --nms-iou: IoU threshold for NMS
- --resolution: model input resolution
- --boundingbox: true/false, controls box drawing in output image
- --no-masks: disable mask rendering
- --sam-checkpoint: local SAM checkpoint path

### Interactive mode

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image /path/to/image.jpg \
  --interactive true
```

UI controls:
- F1: point mode
- F2: box mode
- Left click: foreground point
- Right click: background point
- Drag: box prompt
- Enter: run inference

## Python entry point

```python
from inference import SAM3LoRAInference

inferencer = SAM3LoRAInference(
    config_path="configs/minimal_lora_config.yaml",
    weights_path="outputs/sam3_lora_minimal/best_lora_weights.pt",
    sam_checkpoint="./asset/sam.pt",
    resolution=672,
    detection_threshold=0.5,
    nms_iou_threshold=0.5,
)

results = inferencer.predict(
    image_path="/path/to/image.jpg",
    text_prompts=["pothole"],
    input_boxes=[[100, 120, 240, 300]],
    input_points=[[180, 210, 1]],
)

inferencer.visualize(results, output_path="./asset/output.png")
```

## 7) Minimal Integration Playbooks

### A) Train then validate
1. Run train.py with your chosen config.
2. Take best_lora_weights.pt from output.output_dir.
3. Run validate.py against your valid folder.

### B) Deploy-style inference
1. Keep a fixed config file and matching LoRA weights.
2. Build one SAM3LoRAInference instance at process start.
3. Call predict(...) per request.
4. Optionally skip visualize(...) for API-only environments.

## 8) Common Failure Modes

- File not found for LoRA weights:
  - Pass --weights explicitly or ensure output.output_dir/best_lora_weights.pt exists.
- Validation config error:
  - When not using --use-base-model, both --config and --weights are required.
- No detections:
  - Lower --threshold, verify prompt text matches your category vocabulary.
- Bad dataset structure:
  - Ensure required _annotations.coco.json exists exactly where scripts expect it.
