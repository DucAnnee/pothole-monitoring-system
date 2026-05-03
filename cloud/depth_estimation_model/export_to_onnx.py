"""
One-shot ONNX export for Depth-Anything-V2.

Exports model.forward() — not infer_image() — so preprocessing stays in the service.
Output written to ../triton_inference_server/triton_model_repo/depth_anything_v2/1/model.onnx

Usage:
    python export_to_onnx.py [--weights path/to/weights.pth] [--model-type vitl]
"""

import argparse
import os
import sys

import torch

sys.path.append(os.path.join(os.path.dirname(__file__), "Depth-Anything-V2"))
from depth_anything_v2.dpt import DepthAnythingV2

MODEL_CONFIGS = {
    "vits": {"encoder": "vits", "features": 64,  "out_channels": [48, 96, 192, 384]},
    "vitb": {"encoder": "vitb", "features": 128, "out_channels": [96, 192, 384, 768]},
    "vitl": {"encoder": "vitl", "features": 256, "out_channels": [256, 512, 1024, 1024]},
    "vitg": {"encoder": "vitg", "features": 384, "out_channels": [1536, 1536, 1536, 1536]},
}

INPUT_SIZE = 518


def export(weights_path: str, model_type: str, output_path: str) -> None:
    if model_type not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model_type: {model_type}. Choose from {list(MODEL_CONFIGS)}")

    if not os.path.exists(weights_path):
        raise FileNotFoundError(f"Weights not found: {weights_path}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Device: {device}")

    print(f"[INFO] Loading {model_type} weights from {weights_path} ...")
    model = DepthAnythingV2(**MODEL_CONFIGS[model_type])
    state_dict = torch.load(weights_path, map_location=device)
    model.load_state_dict(state_dict)
    model = model.to(device).eval()

    dummy = torch.zeros(1, 3, INPUT_SIZE, INPUT_SIZE, device=device)

    print(f"[INFO] Exporting to {output_path} ...")
    # dynamo=False forces the legacy TorchScript exporter which correctly propagates
    # dynamic_axes through the output; torch 2.9's default onnxscript exporter
    # concretizes the output batch dim to 1.
    torch.onnx.export(
        model,
        dummy,
        output_path,
        dynamo=False,
        opset_version=18,
        input_names=["input"],
        output_names=["depth_map"],
        dynamic_axes={
            "input":     {0: "batch_size"},
            "depth_map": {0: "batch_size"},
        },
    )

    print(f"[SUCCESS] ONNX model written to {output_path}")
    print(f"          Input:  [N, 3, {INPUT_SIZE}, {INPUT_SIZE}] float32 (ImageNet normalized)")
    print(f"          Output: [N, {INPUT_SIZE}, {INPUT_SIZE}] float32 (relative depth)")


def main():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    default_weights = os.path.join(this_dir, "Depth-Anything-V2", "depth_anything_v2_vitl.pth")
    default_output = os.path.join(
        this_dir, "..", "triton_inference_server",
        "triton_model_repo", "depth_anything_v2", "1", "model.onnx",
    )

    parser = argparse.ArgumentParser(description="Export Depth-Anything-V2 to ONNX for Triton")
    parser.add_argument("--weights", default=default_weights, help="Path to .pth weights file")
    parser.add_argument("--model-type", default="vitl", choices=list(MODEL_CONFIGS))
    parser.add_argument("--output", default=default_output, help="Output path for model.onnx")
    args = parser.parse_args()

    export(args.weights, args.model_type, os.path.normpath(args.output))


if __name__ == "__main__":
    main()
