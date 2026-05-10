# SAM3-LoRA: Fine-Tuning, Validation, and Inference

This folder contains the active SAM3 LoRA workflow:
- train.py: LoRA fine-tuning
- validate.py: full offline evaluation (mAP and cgF1)
- inference.py: image prediction with text and visual prompts

For an API-oriented guide, see SAM_API.md.

## Quick Start

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Train

```bash
python3 train.py --config configs/minimal_lora_config.yaml
```

### 3) Validate best checkpoint

```bash
python3 validate.py \
  --config configs/minimal_lora_config.yaml \
  --weights outputs/sam3_lora_minimal/best_lora_weights.pt \
  --val_data_dir /path/to/data/valid
```

### 4) Run inference

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image /path/to/image.jpg \
  --prompt pothole \
  --output ./asset/output.png
```

## Dataset Layout

train.py expects a data root with train and valid splits:

```text
data_root/
  train/
    _annotations.coco.json
    <images...>
  valid/
    _annotations.coco.json
    <images...>
```

validate.py expects a direct validation directory:

```text
val_data_dir/
  _annotations.coco.json
  <images...>
```

Supported segmentation formats in COCO annotations:
- Polygon lists
- RLE dictionaries

## LoRA Config Variants

Current config variants in configs/:
- minimal_lora_config.yaml
- light_lora_config.yaml
- full_lora_config.yaml
- crack_detection_config.yaml
- base_config.yaml
- sam3_lora_standalone.yaml (legacy format)

Note on compatibility:
- train.py reads training.data_dir.
- Legacy configs that use training.train_data_path or training.val_data_path must be updated to training.data_dir when used with train.py.

## Training (train.py)

### Main behavior
- Loads SAM3 backbone, applies LoRA from config, and trains only LoRA params.
- Uses train split for optimization.
- If valid split exists, computes validation loss each epoch and saves best checkpoint.
- Saves:
  - best_lora_weights.pt
  - last_lora_weights.pt
  - val_stats.json

### Key CLI options
- --config: config YAML path (default: configs/minimal_lora_config.yaml)
- --sam-checkpoint: path to the original SAM checkpoint (default: ./asset/sam.pt)
- --device: one or multiple GPU ids, for example:
  - --device 0
  - --device 0 1
- --master_port: port for distributed launch (default: 29500)

### Multi-GPU
Passing multiple GPU ids in --device triggers automatic distributed launch via torch.distributed.run.

## Validation (validate.py)

validate.py computes full offline metrics:
- mAP (0.50:0.95)
- mAP@50
- mAP@75
- cgF1, cgF1@50, cgF1@75

### LoRA model validation

```bash
python3 validate.py \
  --config configs/minimal_lora_config.yaml \
  --weights outputs/sam3_lora_minimal/best_lora_weights.pt \
  --val_data_dir /path/to/data/valid \
  --prob-threshold 0.3 \
  --nms-iou 0.7
```

### Base SAM3 baseline validation (no LoRA)

```bash
python3 validate.py \
  --use-base-model \
  --val_data_dir /path/to/data/valid
```

### Additional options
- --merge: enable aggressive merge of overlapping crack-like segments
- --merge-iou: merge IoU threshold (default: 0.15)
- --num-samples: limit sample count for debugging
- --sam-checkpoint: SAM checkpoint path

## Inference (inference.py)

### Text prompt

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image /path/to/image.jpg \
  --prompt crack \
  --output ./asset/output.png
```

### Text + box + point prompts

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image /path/to/image.jpg \
  --prompt crack defect \
  --box 100 120 240 300 \
  --point 180 210 1 \
  --output ./asset/output.png
```

### Interactive UI mode

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image /path/to/image.jpg \
  --interactive true
```

Controls in interactive mode:
- F1: point mode
- F2: box mode
- Left click: foreground point
- Right click: background point
- Drag: draw box
- Enter: run inference

### Useful inference options
- --weights: optional; if omitted, auto-loads output.output_dir/best_lora_weights.pt from config
- --threshold: confidence threshold
- --nms-iou: IoU threshold for NMS
- --resolution: input resolution
- --boundingbox true|false: include boxes in output visualization
- --no-masks: disable mask overlay
- --sam-checkpoint: SAM checkpoint path

## Important Current Notes

- Older script names in comments/docs such as train_sam3_lora_native.py and validate_sam3_lora.py map to the current files train.py and validate.py.
- train.py does not compute mAP/cgF1 during training; use validate.py after training.

## Troubleshooting

### Hugging Face access issues
Request access to facebook/sam3 and authenticate, or use a valid local checkpoint via --sam-checkpoint.

### Missing COCO annotations
Ensure _annotations.coco.json is present in required folders.

### Out-of-memory
Try smaller training.batch_size, lower training.resolution, or use minimal/light LoRA config.

### No detections in inference
Lower --threshold and verify prompts match training categories.

## Acknowledgements

This project is based on:
- SAM3_LoRA by Sompote: https://github.com/Sompote/SAM3_LoRA