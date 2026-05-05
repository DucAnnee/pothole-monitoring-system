# SAM3-LoRA: LoRA Fine-Tuning for SAM3

This repository fine-tunes SAM3 with LoRA and supports text, box, and point prompts during training and inference.

## Quick Start

- Use minimal_lora_config for ~12GB VRAM

### 1) Install

```bash
pip install -e .
```

### 2) Train

```bash
python3 train.py --config configs/minimal_lora_config.yaml
```

### 3) Validate (full metrics)

```bash
python3 validate.py \
  --config configs/minimal_lora_config.yaml \
  --weights outputs/sam3_lora_full/best_lora_weights.pt \
  --val_data_dir /path/to/data/valid
```

### 4) Inference

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image path/to/image.jpg \
  --prompt "pothole" \
  --output output.png
```

## Data Setup

Use COCO format with one annotation file per split:

```
data/
  train/
    img001.jpg
    img002.jpg
    _annotations.coco.json
  valid/
    img101.jpg
    _annotations.coco.json
  test/
    img201.jpg
    _annotations.coco.json
```

Supported segmentation formats:
- Polygon lists (COCO style)
- RLE dicts (COCO style)

## Training

Run training with `train_sam3_lora_native.py`. This script:
- Uses category names as text prompts
- Derives box prompts from mask tight boxes
- Derives point prompts from mask centroids

### Single GPU

```bash
python3 train.py --config configs/minimal_lora_config.yaml
```

### Multi-GPU

```bash
python3 train.py --config configs/minimal_lora_config.yaml --device 0 1
```

## Validation

Training computes validation loss only (fast). Full metrics are computed with `validate_sam3_lora.py`:

```bash
python3 validate.py \
  --config configs/minimal_lora_config.yaml \
  --weights outputs/sam3_lora_full/best_lora_weights.pt \
  --val_data_dir /path/to/data/valid
```

## Inference

Text-only:

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image path/to/image.jpg \
  --prompt "crack" \
  --output output.png
```

Text + box:

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image path/to/image.jpg \
  --prompt "crack" \
  --box 100 120 240 300 \
  --output output.png
```

Text + point:

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image path/to/image.jpg \
  --prompt "crack" \
  --point 180 210 1 \
  --output output.png
```

Text + box + point:

```bash
python3 inference.py \
  --config configs/minimal_lora_config.yaml \
  --image path/to/image.jpg \
  --prompt "crack" \
  --box 100 120 240 300 \
  --point 180 210 1 \
  --output output.png
```

## Interactive Inference UI

Use the `--interactive` flag to open a simple Tkinter UI for adding visual prompts and text prompts live:

- Run:

```bash
python3 inference.py --config configs/minimal_lora_config.yaml --image path/to/image.jpg --interactive true
```

- Controls:
  - F1: Point mode (left-click adds foreground point, right-click adds background point)
  - F2: Box mode (click-drag to draw a bounding box)
  - Text entry: enter one or multiple prompts (comma-separated) in the text bar; leave empty to use the CLI/default prompts
  - Enter: submit prompts and geometry to run inference

- The CLI also accepts `--sam-checkpoint ./asset/sam3.pt` to load a local SAM checkpoint instead of downloading from HF.

### Prompt Formats

- Box prompt: `x1 y1 x2 y2` in pixel coordinates
- Point prompt: `x y label` in pixel coordinates; label is `1` (foreground) or `0` (background)

The inference pipeline handles resizing/normalization internally. For best results, set `--resolution` to the same value you used during training.

## Metrics

`validate_sam3_lora.py` reports:
- COCO mAP (0.50:0.95)
- mAP@50
- mAP@75
- cgF1 (concept-level F1)

## Configuration

Key fields used by `train.py`:

```yaml
lora:
  rank: 16
  alpha: 32
  dropout: 0.0
  target_modules: ["q_proj", "k_proj", "v_proj", "out_proj"]
  apply_to_vision_encoder: true
  apply_to_text_encoder: true
  apply_to_geometry_encoder: false
  apply_to_detr_encoder: true
  apply_to_detr_decoder: true
  apply_to_mask_decoder: false

training:
  data_dir: "/path/to/data"
  resolution: 672
  batch_size: 8
  num_workers: 4
  learning_rate: 5e-5
  weight_decay: 0.01
  num_epochs: 20
  logging_steps: 10

output:
  output_dir: "outputs/sam3_lora_full"

logging:
  use_wandb: false
  wandb_project: "sam3_lora"
  wandb_run_name: null
```

## Troubleshooting

### Hugging Face access error
You must request access to `facebook/sam3` on Hugging Face and login with a token.

### COCO file not found
Ensure each split folder contains `_annotations.coco.json` next to the images.

### Out of memory
Reduce `training.batch_size`, lower `lora.rank`, or lower `training.resolution`.

### No detections during inference
Try lowering `--threshold` or verify the prompt matches your training categories.

### Mismatched resolution
Use the same `training.resolution` during inference via `--resolution`.


## Acknowledgements

This project is based on the following work:

- SAM3_LoRA by Sompote  
  https://github.com/Sompote/SAM3_LoRA

I modified the original implementation to:
- Support box, and point prompts