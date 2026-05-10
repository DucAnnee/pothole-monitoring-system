import json
import os
import sys

import cv2
import numpy as np
import torch
import triton_python_backend_utils as pb_utils
from minio import Minio

# SAM3 package lives at /opt/sam3, lora_layers.py at /opt/lora_layers.py
sys.path.insert(0, "/opt")

CANVAS_W = 600
CANVAS_H = 400
MIN_POLYGON_POINTS = 4
TEXT_PROMPT = "pothole"
DETECTION_THRESHOLD = 0.3
NMS_IOU_THRESHOLD = 0.7
SAM3_RESOLUTION = 672


class TritonPythonModel:
    def initialize(self, args):
        self._ready = False
        device = "cuda" if torch.cuda.is_available() else "cpu"
        pb_utils.Logger.log_info(f"[sam3_assist] Loading SAM3 on {device}")

        hf_token = os.environ.get("HUGGING_FACE_TOKEN", "").strip()
        if hf_token:
            os.environ["HF_TOKEN"] = hf_token
            os.environ["HUGGINGFACE_TOKEN"] = hf_token

        # Local checkpoint path takes priority over HF download
        local_ckpt = os.environ.get("SAM3_CHECKPOINT_PATH", "")
        load_from_hf = not bool(local_ckpt)

        from sam3.model_builder import build_sam3_image_model
        from lora_layers import LoRAConfig, apply_lora_to_model, load_lora_weights

        bpe_path = "/opt/sam3/assets/bpe_simple_vocab_16e6.txt.gz"
        try:
            self.model = build_sam3_image_model(
                device=device,
                compile=False,
                load_from_HF=load_from_hf,
                checkpoint_path=local_ckpt if local_ckpt else None,
                bpe_path=bpe_path,
                eval_mode=True,
            )
        except Exception as exc:
            pb_utils.Logger.log_warn(
                f"[sam3_assist] Failed to load SAM3 backbone: {exc}. "
                "Set HUGGING_FACE_TOKEN or SAM3_CHECKPOINT_PATH. Running in stub mode."
            )
            return  # self._ready stays False → execute returns stub

        pb_utils.Logger.log_info("[sam3_assist] Base SAM3 loaded, applying LoRA")

        lora_config = LoRAConfig(
            rank=4,
            alpha=8,
            dropout=0.0,
            target_modules=["q_proj", "k_proj", "v_proj"],
            apply_to_vision_encoder=False,
            apply_to_text_encoder=False,
            apply_to_geometry_encoder=False,
            apply_to_detr_encoder=False,
            apply_to_detr_decoder=True,
            apply_to_mask_decoder=False,
        )
        self.model = apply_lora_to_model(self.model, lora_config)

        lora_weights_path = os.environ.get(
            "SAM3_LORA_WEIGHTS",
            "/lora_weights/best_lora_weights.pt",
        )
        if os.path.exists(lora_weights_path):
            load_lora_weights(self.model, lora_weights_path)
            pb_utils.Logger.log_info(f"[sam3_assist] Loaded LoRA weights: {lora_weights_path}")
        else:
            pb_utils.Logger.log_warn(
                f"[sam3_assist] LoRA weights not found at {lora_weights_path}, using base SAM3"
            )

        self.model.to(device)
        self.model.eval()
        self.device = torch.device(device)
        self._ready = True

        from sam3.train.transforms.basic_for_api import (
            ComposeAPI,
            NormalizeAPI,
            RandomResizeAPI,
            ToTensorAPI,
        )

        self.transform = ComposeAPI(
            transforms=[
                RandomResizeAPI(
                    sizes=SAM3_RESOLUTION,
                    max_size=SAM3_RESOLUTION,
                    square=True,
                    consistent_transform=False,
                ),
                ToTensorAPI(),
                NormalizeAPI(mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5]),
            ]
        )

        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
        self.minio_client = Minio(
            minio_endpoint,
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )
        self.minio_bucket = os.environ.get("MINIO_BUCKET", "warehouse")
        pb_utils.Logger.log_info("[sam3_assist] SAM3 ready")

    def execute(self, requests):
        responses = []
        for request in requests:
            try:
                if not self._ready:
                    result_json = json.dumps({
                        "polygon": [],
                        "error": "SAM3 not loaded (missing HUGGING_FACE_TOKEN or SAM3_CHECKPOINT_PATH)",
                    })
                else:
                    result_json = self._infer(request)
            except Exception as exc:
                pb_utils.Logger.log_warn(f"[sam3_assist] inference error: {exc}")
                result_json = json.dumps({"polygon": [], "error": str(exc)})

            out = pb_utils.Tensor(
                "polygon_json",
                np.array([result_json.encode("utf-8")], dtype=object),
            )
            responses.append(pb_utils.InferenceResponse(output_tensors=[out]))
        return responses

    def _infer(self, request):
        key_np = pb_utils.get_input_tensor_by_name(request, "image_object_key").as_numpy()
        image_key = key_np.flat[0]
        if isinstance(image_key, bytes):
            image_key = image_key.decode("utf-8")

        bbox_np = pb_utils.get_input_tensor_by_name(request, "bbox").as_numpy()
        x1, y1, x2, y2 = bbox_np.flatten().tolist()

        x1 = max(0.0, min(x1, CANVAS_W))
        y1 = max(0.0, min(y1, CANVAS_H))
        x2 = max(0.0, min(x2, CANVAS_W))
        y2 = max(0.0, min(y2, CANVAS_H))

        object_name = image_key
        if object_name.startswith("s3://"):
            without_scheme = object_name[len("s3://"):]
            slash = without_scheme.find("/")
            object_name = without_scheme[slash + 1:] if slash >= 0 else without_scheme
        else:
            object_name = object_name.lstrip("/")

        response = self.minio_client.get_object(self.minio_bucket, object_name)
        try:
            image_bytes = response.read()
        finally:
            response.close()
            response.release_conn()

        nparr = np.frombuffer(image_bytes, np.uint8)
        img_bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img_bgr is None:
            raise ValueError(f"Failed to decode image: {object_name}")

        img_bgr = cv2.resize(img_bgr, (CANVAS_W, CANVAS_H))

        from PIL import Image as PILImage

        pil_image = PILImage.fromarray(cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB))
        orig_w, orig_h = pil_image.size

        mask, score = self._run_sam3(pil_image, [[x1, y1, x2, y2]], orig_w, orig_h)

        if mask is None:
            polygon = [[x1, y1], [x2, y1], [x2, y2], [x1, y2]]
            return json.dumps({"polygon": polygon, "confidence": 0.0})

        mask_uint8 = (mask * 255).astype(np.uint8)
        contours, _ = cv2.findContours(mask_uint8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        if not contours:
            polygon = [[x1, y1], [x2, y1], [x2, y2], [x1, y2]]
            return json.dumps({"polygon": polygon, "confidence": float(score)})

        largest = max(contours, key=cv2.contourArea)
        perimeter = cv2.arcLength(largest, True)
        epsilon = 0.015 * perimeter
        approx = cv2.approxPolyDP(largest, epsilon, True)

        while len(approx) < MIN_POLYGON_POINTS and epsilon > 0.5:
            epsilon *= 0.5
            approx = cv2.approxPolyDP(largest, epsilon, True)

        polygon = [[float(pt[0][0]), float(pt[0][1])] for pt in approx]
        return json.dumps({"polygon": polygon, "confidence": float(score)})

    def _run_sam3(self, pil_image, input_boxes, orig_w, orig_h):
        from sam3.model.utils.misc import copy_data_to_device
        from sam3.train.data.collator import collate_fn_api
        from sam3.train.data.sam3_image_dataset import (
            Datapoint,
            FindQueryLoaded,
            Image as SAMImage,
            InferenceMetadata,
        )

        sam_image = SAMImage(data=pil_image, objects=[], size=[orig_h, orig_w])
        box_tensor = torch.tensor(input_boxes, dtype=torch.float32)
        box_label = torch.ones(len(input_boxes), dtype=torch.long)

        query = FindQueryLoaded(
            query_text=TEXT_PROMPT,
            image_id=0,
            object_ids_output=[],
            is_exhaustive=True,
            query_processing_order=0,
            input_bbox=box_tensor,
            input_bbox_label=box_label,
            input_points=None,
            inference_metadata=InferenceMetadata(
                coco_image_id=0,
                original_image_id=0,
                original_category_id=1,
                original_size=[orig_w, orig_h],
                object_id=0,
                frame_index=0,
            ),
        )

        datapoint = Datapoint(find_queries=[query], images=[sam_image])
        datapoint = self.transform(datapoint)
        batch = collate_fn_api([datapoint], dict_key="input")["input"]
        batch = copy_data_to_device(batch, self.device, non_blocking=True)

        with torch.inference_mode():
            outputs = self.model(batch)

        last_output = outputs[-1]
        pred_logits = last_output["pred_logits"]
        pred_masks = last_output.get("pred_masks", None)

        scores = pred_logits.sigmoid()[0, :, :].max(dim=-1)[0]
        keep = scores > DETECTION_THRESHOLD

        if not keep.any() or pred_masks is None:
            return None, 0.0

        kept_indices = keep.nonzero(as_tuple=False).squeeze(-1)
        kept_scores = scores[keep]
        best_local_idx = kept_scores.argmax()
        best_global_idx = kept_indices[best_local_idx]
        best_score = kept_scores[best_local_idx].item()

        import torch.nn.functional as F

        mask_logit = pred_masks[0, best_global_idx].sigmoid() > 0.5
        mask_resized = (
            F.interpolate(
                mask_logit.unsqueeze(0).unsqueeze(0).float(),
                size=(orig_h, orig_w),
                mode="bilinear",
                align_corners=False,
            ).squeeze()
            > 0.5
        )

        return mask_resized.cpu().numpy(), best_score

    def finalize(self):
        pass
