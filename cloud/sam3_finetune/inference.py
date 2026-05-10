#!/usr/bin/env python3
"""
SAM3 + LoRA Inference Script

Based on official SAM3 batched inference patterns.
Supports text prompts and visual prompts with LoRA fine-tuned weights.

Usage:
    # Text prompt inference
    python3 infer_sam.py \
        --config configs/minimal_lora_config.yaml \
        --image path/to/image.jpg \
        --prompt "crack" \
        --output output.png

    # Multiple prompts
    python3 infer_sam.py \
        --config configs/minimal_lora_config.yaml \
        --image path/to/image.jpg \
        --prompt "crack" "defect" "damage" \
        --output output.png
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import torch
import numpy as np
from PIL import Image as PILImage
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import yaml
from torchvision.ops import nms

# SAM3 imports
from sam3.model_builder import build_sam3_image_model
from sam3.train.data.sam3_image_dataset import (
    Datapoint,
    Image as SAMImage,
    FindQueryLoaded,
    InferenceMetadata,
)
from sam3.train.data.collator import collate_fn_api
from sam3.model.utils.misc import copy_data_to_device
from sam3.train.transforms.basic_for_api import (
    ComposeAPI,
    RandomResizeAPI,
    ToTensorAPI,
    NormalizeAPI,
)
from sam3.eval.postprocessors import PostProcessImage

# LoRA imports
from lora_layers import LoRAConfig, apply_lora_to_model, load_lora_weights


def _parse_bool(value: str) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


def _split_prompts(prompt_text: str, default_prompts: List[str]) -> List[str]:
    if prompt_text is None:
        return default_prompts
    raw = prompt_text.strip()
    if not raw:
        return default_prompts

    normalized = raw.replace(";", ",")
    if "," in normalized:
        prompts = [p.strip() for p in normalized.split(",") if p.strip()]
        return prompts or default_prompts
    return [normalized]


def run_interactive_prompt(
    image_path: str, default_prompts: List[str], max_display: int = 1024
) -> Tuple[List[str], Optional[List[List[float]]], Optional[List[List[float]]]]:
    try:
        import tkinter as tk
        from PIL import ImageTk
    except Exception as exc:
        raise RuntimeError(
            "Tkinter and Pillow ImageTk are required for --interactive"
        ) from exc

    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Image not found: {image_path}")

    pil_image = PILImage.open(image_path).convert("RGB")
    orig_w, orig_h = pil_image.size
    scale = min(1.0, max_display / max(orig_w, orig_h))
    disp_w = max(1, int(orig_w * scale))
    disp_h = max(1, int(orig_h * scale))
    display_image = (
        pil_image
        if scale == 1.0
        else pil_image.resize((disp_w, disp_h), PILImage.BILINEAR)
    )

    root = tk.Tk()
    root.title("SAM3 Interactive Prompt")

    info_text = (
        "F1=Point, F2=Box | Left click=fg point, Right click=bg point | "
        "Drag to draw box | Enter to run"
    )
    info_label = tk.Label(root, text=info_text)
    info_label.pack(padx=8, pady=(8, 2))

    canvas = tk.Canvas(root, width=disp_w, height=disp_h)
    canvas.pack(padx=8, pady=4)
    tk_img = ImageTk.PhotoImage(display_image)
    canvas.image = tk_img
    canvas.create_image(0, 0, anchor="nw", image=tk_img)

    default_label = ", ".join(default_prompts)
    prompt_label = tk.Label(root, text=f"Prompt (empty uses: {default_label})")
    prompt_label.pack(padx=8, pady=(4, 2))

    prompt_entry = tk.Entry(root, width=80)
    prompt_entry.pack(padx=8, pady=(0, 6))
    prompt_entry.focus_set()

    status_var = tk.StringVar()
    status_label = tk.Label(root, textvariable=status_var)
    status_label.pack(padx=8, pady=(0, 8))

    mode = {"value": "point"}
    points: List[List[float]] = []
    boxes: List[List[float]] = []
    drag_state = {"x": None, "y": None, "rect_id": None}
    result = {"prompts": None}

    def update_status() -> None:
        status_var.set(
            f"Mode: {mode['value']} | Points: {len(points)} | Boxes: {len(boxes)}"
        )

    def set_mode(new_mode: str) -> None:
        mode["value"] = new_mode
        update_status()

    def to_orig_coords(x_disp: float, y_disp: float) -> tuple[float, float]:
        x_orig = x_disp / scale
        y_orig = y_disp / scale
        x_orig = max(0.0, min(orig_w, x_orig))
        y_orig = max(0.0, min(orig_h, y_orig))
        return x_orig, y_orig

    def on_left_click(event) -> None:
        if mode["value"] != "point":
            return
        x_orig, y_orig = to_orig_coords(event.x, event.y)
        points.append([x_orig, y_orig, 1])
        r = 6
        # Draw foreground point (red with larger marker)
        canvas.create_oval(
            event.x - r,
            event.y - r,
            event.x + r,
            event.y + r,
            outline="red",
            fill="red",
            width=2,
        )
        # Add label
        canvas.create_text(
            event.x + r + 5,
            event.y - r,
            text=f"FG{len(points)}",
            fill="red",
            font=("Arial", 8),
        )
        update_status()

    def on_right_click(event) -> None:
        if mode["value"] != "point":
            return
        x_orig, y_orig = to_orig_coords(event.x, event.y)
        points.append([x_orig, y_orig, 0])
        r = 6
        # Draw background point (blue with larger marker)
        canvas.create_oval(
            event.x - r,
            event.y - r,
            event.x + r,
            event.y + r,
            outline="blue",
            fill="blue",
            width=2,
        )
        # Add label
        canvas.create_text(
            event.x + r + 5,
            event.y + r,
            text=f"BG{len(points)}",
            fill="blue",
            font=("Arial", 8),
        )
        update_status()

    def on_mouse_down(event) -> None:
        if mode["value"] != "box":
            return
        drag_state["x"] = event.x
        drag_state["y"] = event.y
        drag_state["rect_id"] = canvas.create_rectangle(
            event.x, event.y, event.x, event.y, outline="yellow", width=3
        )

    def on_mouse_drag(event) -> None:
        if mode["value"] != "box" or drag_state["rect_id"] is None:
            return
        canvas.coords(
            drag_state["rect_id"],
            drag_state["x"],
            drag_state["y"],
            event.x,
            event.y,
        )

    def on_mouse_up(event) -> None:
        if mode["value"] != "box" or drag_state["rect_id"] is None:
            return
        x1, y1 = drag_state["x"], drag_state["y"]
        x2, y2 = event.x, event.y
        x1, x2 = sorted([x1, x2])
        y1, y2 = sorted([y1, y2])
        orig_x1, orig_y1 = to_orig_coords(x1, y1)
        orig_x2, orig_y2 = to_orig_coords(x2, y2)
        boxes.append([orig_x1, orig_y1, orig_x2, orig_y2])
        # Add label to the finalized box
        box_center_x = (x1 + x2) // 2
        box_center_y = y1 - 5
        canvas.create_text(
            box_center_x,
            box_center_y,
            text=f"Box{len(boxes)}",
            fill="yellow",
            font=("Arial", 8, "bold"),
            bg="black",
        )
        drag_state["rect_id"] = None
        update_status()

    def on_submit(event=None) -> None:
        raw_prompt = prompt_entry.get().strip()
        result["prompts"] = _split_prompts(raw_prompt, default_prompts)
        root.quit()

    def on_close() -> None:
        result["prompts"] = result["prompts"] or default_prompts
        root.quit()

    root.bind("<F1>", lambda e: set_mode("point"))
    root.bind("<F2>", lambda e: set_mode("box"))
    root.bind("<Return>", on_submit)
    root.protocol("WM_DELETE_WINDOW", on_close)

    canvas.bind("<Button-1>", on_left_click)
    canvas.bind("<Button-3>", on_right_click)
    canvas.bind("<ButtonPress-1>", on_mouse_down)
    canvas.bind("<B1-Motion>", on_mouse_drag)
    canvas.bind("<ButtonRelease-1>", on_mouse_up)

    update_status()
    root.mainloop()
    root.destroy()

    prompts = result["prompts"] or default_prompts
    return prompts, (boxes or None), (points or None)


class SAM3LoRAInference:
    """SAM3 model with LoRA for inference."""

    def __init__(
        self,
        config_path: str,
        weights_path: Optional[str] = None,
        sam_checkpoint: Optional[str] = "./asset/sam.pt",
        resolution: int = 1008,
        detection_threshold: float = 0.5,
        nms_iou_threshold: float = 0.5,
        device: str = "cuda",
    ):
        """
        Initialize SAM3 with LoRA.

        Args:
            config_path: Path to training config YAML
            weights_path: Path to LoRA weights (optional, auto-detected from config)
            resolution: Input image resolution (default: 1008)
            detection_threshold: Confidence threshold for detections (default: 0.5)
            nms_iou_threshold: IoU threshold for NMS (default: 0.5)
            device: Device to run on (default: "cuda")
        """
        # Load config
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Auto-detect weights if not provided
        if weights_path is None:
            output_dir = self.config.get("output", {}).get(
                "output_dir", "outputs/sam3_lora_full"
            )
            weights_path = os.path.join(output_dir, "best_lora_weights.pt")
            print(f"ℹ️  Auto-detected weights: {weights_path}")

        if not os.path.exists(weights_path):
            raise FileNotFoundError(f"LoRA weights not found: {weights_path}")

        self.weights_path = weights_path
        self.resolution = resolution
        self.detection_threshold = detection_threshold
        self.nms_iou_threshold = nms_iou_threshold
        self.device = torch.device(device if torch.cuda.is_available() else "cpu")
        self.sam_checkpoint = str(Path(sam_checkpoint).expanduser()) if sam_checkpoint else sam_checkpoint
        if self.sam_checkpoint and not os.path.exists(self.sam_checkpoint):
            raise FileNotFoundError(f"SAM checkpoint not found: {self.sam_checkpoint}")

        print(f"🔧 Initializing SAM3 + LoRA...")
        print(f"   Device: {self.device}")
        print(f"   Resolution: {resolution}x{resolution}")
        print(f"   Confidence threshold: {detection_threshold}")
        print(f"   NMS IoU threshold: {nms_iou_threshold}")

        # Build base model
        print("\n📦 Building SAM3 model...")
        self.model = build_sam3_image_model(
            device=self.device.type,
            compile=False,
            load_from_HF=True,
            bpe_path="sam3/assets/bpe_simple_vocab_16e6.txt.gz",
            checkpoint_path=self.sam_checkpoint,
            eval_mode=True,
        )

        # Apply LoRA configuration
        print("🔗 Applying LoRA configuration...")
        lora_cfg = self.config["lora"]
        lora_config = LoRAConfig(
            rank=lora_cfg["rank"],
            alpha=lora_cfg["alpha"],
            dropout=0.0,  # No dropout during inference
            target_modules=lora_cfg["target_modules"],
            apply_to_vision_encoder=lora_cfg["apply_to_vision_encoder"],
            apply_to_text_encoder=lora_cfg["apply_to_text_encoder"],
            apply_to_geometry_encoder=lora_cfg["apply_to_geometry_encoder"],
            apply_to_detr_encoder=lora_cfg["apply_to_detr_encoder"],
            apply_to_detr_decoder=lora_cfg["apply_to_detr_decoder"],
            apply_to_mask_decoder=lora_cfg["apply_to_mask_decoder"],
        )
        self.model = apply_lora_to_model(self.model, lora_config)

        # Load LoRA weights
        print(f"💾 Loading LoRA weights from {weights_path}...")
        load_lora_weights(self.model, weights_path)

        self.model.to(self.device)
        self.model.eval()

        # Setup transforms (official SAM3 pattern)
        self.transform = ComposeAPI(
            transforms=[
                RandomResizeAPI(
                    sizes=resolution,
                    max_size=resolution,
                    square=True,
                    consistent_transform=False,
                ),
                ToTensorAPI(),
                NormalizeAPI(mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5]),
            ]
        )

        # Setup postprocessor
        # Note: Using simpler manual postprocessing instead of PostProcessImage
        # because PostProcessImage may have additional filtering logic
        self.use_manual_postprocess = True

        print("✅ SAM3 + LoRA ready for inference!\n")

    def create_datapoint(
        self,
        pil_image: PILImage.Image,
        text_prompts: List[str],
        input_boxes: Optional[List[List[float]]] = None,
        input_points: Optional[List[List[float]]] = None,
    ) -> Datapoint:
        """
        Create a SAM3 datapoint from image and text prompts.

        Args:
            pil_image: PIL Image
            text_prompts: List of text queries
            input_boxes: Optional list of [x1, y1, x2, y2] box prompts in pixels
            input_points: Optional list of [x, y, label] point prompts in pixels

        Returns:
            Datapoint with image and queries
        """
        w, h = pil_image.size

        # Create SAM Image
        sam_image = SAMImage(data=pil_image, objects=[], size=[h, w])

        input_bbox_tensor = None
        input_bbox_label = None
        if input_boxes:
            input_bbox_tensor = torch.tensor(input_boxes, dtype=torch.float32)
            input_bbox_label = torch.ones(len(input_boxes), dtype=torch.long)

        input_points_tensor = None
        if input_points:
            input_points_tensor = torch.tensor(input_points, dtype=torch.float32)
            input_points_tensor = input_points_tensor.unsqueeze(0)

        # Create queries for each text prompt
        queries = []
        for idx, text_query in enumerate(text_prompts):
            query_input_bbox = (
                input_bbox_tensor.clone() if input_bbox_tensor is not None else None
            )
            query_input_bbox_label = (
                input_bbox_label.clone() if input_bbox_label is not None else None
            )
            query_input_points = (
                input_points_tensor.clone() if input_points_tensor is not None else None
            )
            query = FindQueryLoaded(
                query_text=text_query,
                image_id=0,
                object_ids_output=[],
                is_exhaustive=True,
                query_processing_order=idx,
                input_bbox=query_input_bbox,
                input_bbox_label=query_input_bbox_label,
                input_points=query_input_points,
                inference_metadata=InferenceMetadata(
                    coco_image_id=idx,
                    original_image_id=idx,
                    original_category_id=1,
                    original_size=[w, h],
                    object_id=0,
                    frame_index=0,
                ),
            )
            queries.append(query)

        return Datapoint(find_queries=queries, images=[sam_image])

    @torch.no_grad()
    def predict(
        self,
        image_path: str,
        text_prompts: List[str],
        input_boxes: Optional[List[List[float]]] = None,
        input_points: Optional[List[List[float]]] = None,
    ) -> dict:
        """
        Run inference on an image with text prompts.

        Args:
            image_path: Path to input image
            text_prompts: List of text queries (e.g., ["crack", "defect"])
            input_boxes: Optional list of [x1, y1, x2, y2] box prompts in pixels
            input_points: Optional list of [x, y, label] point prompts in pixels

        Returns:
            Dictionary mapping prompt index to predictions:
            {
                0: {'boxes': [...], 'scores': [...], 'masks': [...]},
                1: {'boxes': [...], 'scores': [...], 'masks': [...]}
            }
        """
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"Image not found: {image_path}")

        # Load image
        pil_image = PILImage.open(image_path).convert("RGB")
        print(f"📷 Loaded image: {image_path}")
        print(f"   Size: {pil_image.size}")
        print(f"   Prompts: {text_prompts}")

        print("\n🔮 Running inference...")

        results = {}

        # Process each prompt separately (SAM3 expects one query per forward pass)
        for query_idx, prompt in enumerate(text_prompts):
            # Create datapoint with single prompt
            datapoint = self.create_datapoint(
                pil_image,
                [prompt],
                input_boxes=input_boxes,
                input_points=input_points,
            )

            # Apply transforms
            datapoint = self.transform(datapoint)

            # Collate into batch
            batch = collate_fn_api([datapoint], dict_key="input")["input"]

            # Move to device
            batch = copy_data_to_device(batch, self.device, non_blocking=True)

            # Forward pass
            outputs = self.model(batch)

            # Manual post-processing
            last_output = outputs[-1]
            pred_logits = last_output[
                "pred_logits"
            ]  # [batch, num_queries, num_classes]
            pred_boxes = last_output["pred_boxes"]  # [batch, num_queries, 4]
            pred_masks = last_output.get(
                "pred_masks", None
            )  # [batch, num_queries, H, W]

            # Get probabilities
            out_probs = pred_logits.sigmoid()  # [batch, num_queries, num_classes]

            # Get scores for this query
            scores = out_probs[0, :, :].max(dim=-1)[0]  # [num_queries]

            # Filter by threshold
            keep = scores > self.detection_threshold
            num_keep = keep.sum().item()

            if num_keep > 0:
                # Get boxes and convert from cxcywh to xyxy
                boxes_cxcywh = pred_boxes[0, keep]  # [num_keep, 4]
                kept_scores = scores[keep]
                cx, cy, w, h = boxes_cxcywh.unbind(-1)

                # Convert to xyxy and scale to original image size
                orig_w, orig_h = pil_image.size
                x1 = (cx - w / 2) * orig_w
                y1 = (cy - h / 2) * orig_h
                x2 = (cx + w / 2) * orig_w
                y2 = (cy + h / 2) * orig_h

                boxes_xyxy = torch.stack([x1, y1, x2, y2], dim=-1)

                # Apply NMS to remove overlapping boxes
                keep_nms = nms(boxes_xyxy, kept_scores, self.nms_iou_threshold)
                boxes_xyxy = boxes_xyxy[keep_nms]
                kept_scores = kept_scores[keep_nms]
                num_keep = len(keep_nms)

                # Get masks and resize to original size
                if pred_masks is not None:
                    # Apply NMS filtering to masks too
                    masks_small = (
                        pred_masks[0, keep][keep_nms].sigmoid() > 0.5
                    )  # [num_keep_nms, H, W]

                    # Resize masks to original image size
                    import torch.nn.functional as F

                    masks_resized = (
                        F.interpolate(
                            masks_small.unsqueeze(0).float(),
                            size=(orig_h, orig_w),
                            mode="bilinear",
                            align_corners=False,
                        ).squeeze(0)
                        > 0.5
                    )

                    masks_np = masks_resized.cpu().numpy()
                else:
                    masks_np = None

                results[query_idx] = {
                    "prompt": prompt,
                    "boxes": boxes_xyxy.cpu().numpy(),
                    "scores": kept_scores.cpu().numpy(),
                    "masks": masks_np,
                    "num_detections": num_keep,
                }
                print(
                    f"   '{prompt}': {num_keep} detections after NMS (max score: {kept_scores.max().item():.3f})"
                )
            else:
                results[query_idx] = {
                    "prompt": prompt,
                    "boxes": None,
                    "scores": None,
                    "masks": None,
                    "num_detections": 0,
                }
                print(f"   '{prompt}': 0 detections")

        # Store original image for visualization
        results["_image"] = pil_image

        return results

    def visualize(
        self,
        results: dict,
        output_path: str,
        show_boxes: bool = True,
        show_masks: bool = True,
    ):
        """
        Visualize predictions on image.

        Args:
            results: Results from predict()
            output_path: Where to save visualization
            show_boxes: Whether to show bounding boxes
            show_masks: Whether to show segmentation masks
        """
        pil_image = results["_image"]

        # Create figure
        fig, ax = plt.subplots(1, figsize=(12, 8))
        ax.imshow(pil_image)

        # Colors for different prompts
        colors = ["red", "blue", "green", "yellow", "cyan", "magenta"]

        total_detections = 0

        # Draw results for each prompt
        for idx in sorted([k for k in results.keys() if k != "_image"]):
            result = results[idx]
            prompt = result["prompt"]
            color = colors[idx % len(colors)]

            if result["num_detections"] == 0:
                continue

            total_detections += result["num_detections"]

            boxes = result["boxes"]
            scores = result["scores"]
            masks = result["masks"]

            for i in range(result["num_detections"]):
                # Draw mask
                if show_masks and masks is not None:
                    mask = masks[i]
                    colored_mask = np.zeros((*mask.shape, 4))
                    # Use different colors for different prompts
                    if color == "red":
                        colored_mask[mask] = [1, 0, 0, 0.4]
                    elif color == "blue":
                        colored_mask[mask] = [0, 0, 1, 0.4]
                    elif color == "green":
                        colored_mask[mask] = [0, 1, 0, 0.4]
                    else:
                        colored_mask[mask] = [1, 1, 0, 0.4]
                    ax.imshow(colored_mask)

                # Draw box
                if show_boxes and boxes is not None:
                    box = boxes[i]  # [x1, y1, x2, y2]
                    x1, y1, x2, y2 = box

                    # Clamp to image bounds
                    img_w, img_h = pil_image.size
                    x1 = max(0, min(img_w, x1))
                    y1 = max(0, min(img_h, y1))
                    x2 = max(0, min(img_w, x2))
                    y2 = max(0, min(img_h, y2))

                    width = x2 - x1
                    height = y2 - y1

                    # Draw rectangle
                    rect = patches.Rectangle(
                        (x1, y1),
                        width,
                        height,
                        linewidth=2,
                        edgecolor=color,
                        facecolor="none",
                    )
                    ax.add_patch(rect)

                    # Add label
                    score = scores[i] if scores is not None else 0
                    label = f"{prompt}: {score:.2f}"
                    ax.text(
                        x1,
                        y1 - 5,
                        label,
                        bbox=dict(facecolor=color, alpha=0.5),
                        fontsize=10,
                        color="white",
                    )

        ax.axis("off")

        # Add title with all prompts
        prompts_str = ", ".join(
            [
                f'"{results[k]["prompt"]}"'
                for k in sorted([k for k in results.keys() if k != "_image"])
            ]
        )
        plt.suptitle(f"Text Prompts: {prompts_str}", fontsize=12, y=0.98)

        plt.tight_layout()
        plt.savefig(output_path, bbox_inches="tight", dpi=150)
        plt.close()

        print(f"\n✅ Saved visualization to {output_path}")
        print(f"   Total detections: {total_detections}")


def main():
    parser = argparse.ArgumentParser(description="SAM3 + LoRA Inference")
    parser.add_argument(
        "--config", type=str, required=True, help="Path to training config YAML"
    )
    parser.add_argument(
        "--weights",
        type=str,
        default=None,
        help="Path to LoRA weights (auto-detected if not provided)",
    )
    parser.add_argument("--image", type=str, required=True, help="Path to input image")
    parser.add_argument(
        "--prompt",
        type=str,
        nargs="+",
        default=["object"],
        help='Text prompt(s) to guide segmentation (e.g., "crack" or "crack" "defect")',
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./asset/output.png",
        help="Output visualization path",
    )
    parser.add_argument(
        "--box",
        type=float,
        nargs=4,
        action="append",
        metavar=("X1", "Y1", "X2", "Y2"),
        help="Box prompt in pixel coords: x1 y1 x2 y2 (repeatable)",
    )
    parser.add_argument(
        "--point",
        type=float,
        nargs=3,
        action="append",
        metavar=("X", "Y", "LABEL"),
        help="Point prompt in pixel coords with label 1 (fg) or 0 (bg) (repeatable)",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5, help="Detection confidence threshold"
    )
    parser.add_argument(
        "--resolution", type=int, default=672, help="Input resolution (default: 1008)"
    )
    parser.add_argument(
        "--boundingbox",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=False,
        help="Show bounding boxes: True or False (default: False)",
    )
    parser.add_argument(
        "--no-masks", action="store_true", help="Don't show segmentation masks"
    )
    parser.add_argument(
        "--nms-iou",
        type=float,
        default=0.5,
        help="NMS IoU threshold (default: 0.5, lower = fewer overlapping boxes)",
    )
    parser.add_argument(
        "--interactive",
        type=_parse_bool,
        default=False,
        help="Enable interactive prompt UI (true/false)",
    )
    parser.add_argument(
        "--sam-checkpoint",
        type=str,
        default="./asset/sam.pt",
        help="Path to the original SAM checkpoint to initialize the backbone (default: ./asset/sam.pt)",
    )

    args = parser.parse_args()

    # Initialize model
    inferencer = SAM3LoRAInference(
        config_path=args.config,
        weights_path=args.weights,
        sam_checkpoint=args.sam_checkpoint,
        resolution=args.resolution,
        detection_threshold=args.threshold,
        nms_iou_threshold=args.nms_iou,
    )

    # Run inference
    if args.interactive:
        print("Interactive mode enabled. Using UI prompts and geometry.")
        default_prompts = args.prompt if args.prompt else ["object"]
        prompts, input_boxes, input_points = run_interactive_prompt(
            args.image, default_prompts
        )
    else:
        prompts = args.prompt
        input_boxes = args.box if args.box else None
        input_points = args.point if args.point else None

    if input_points:
        for p in input_points:
            if int(p[2]) not in (0, 1):
                raise ValueError("Point label must be 0 (bg) or 1 (fg).")

    results = inferencer.predict(
        args.image,
        prompts,
        input_boxes=input_boxes,
        input_points=input_points,
    )

    # Visualize
    inferencer.visualize(
        results, args.output, show_boxes=args.boundingbox, show_masks=not args.no_masks
    )

    # Print summary
    print("\n" + "=" * 60)
    print("📊 Summary:")
    for idx in sorted([k for k in results.keys() if k != "_image"]):
        result = results[idx]
        print(f"   Prompt '{result['prompt']}': {result['num_detections']} detections")
        if result["num_detections"] > 0 and result["scores"] is not None:
            print(f"      Max confidence: {result['scores'].max():.3f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
