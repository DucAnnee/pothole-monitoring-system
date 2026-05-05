"""BEV image processing: download raw image, run PotholeAreaEstimator, upload BEV."""

import json
from io import BytesIO
from typing import Optional, Tuple

import cv2
import numpy as np
from minio import Minio

from pothole_area_estimator import PotholeAreaEstimator


class BEVProcessor:
    def __init__(self, minio_client: Minio, bucket: str, estimator: PotholeAreaEstimator):
        self.minio = minio_client
        self.bucket = bucket
        self.estimator = estimator

    def process(
        self, event_id: str, raw_image_object_key: str, original_mask: list
    ) -> Tuple[Optional[str], Optional[str], float, float]:
        """
        Download raw image, compute BEV + surface area, upload BEV image.

        Returns:
            (bev_object_key, bev_mask_json, surface_area_cm2, confidence)
            On failure: (None, None, 0.0, 0.0)
        """
        image_bytes = self._download(raw_image_object_key)
        if image_bytes is None:
            return None, None, 0.0, 0.0

        try:
            image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR)
            if image is None:
                raise ValueError("Failed to decode image bytes")
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            mask_np = np.array(original_mask, dtype=np.float32)
            area_cm2, bev_img, _H, bev_mask_np = self.estimator.compute_pothole_area(
                image_rgb, mask_np
            )

            bev_object_key = f"bev_images/{event_id}.jpg"
            self._upload_bev(bev_img, bev_object_key)

            bev_mask_json = json.dumps(bev_mask_np.tolist())
            return bev_object_key, bev_mask_json, float(area_cm2), 1.0

        except Exception as e:
            print(f"[ERROR] BEV processing failed for {event_id}: {e}")
            return None, None, 0.0, 0.0

    def _download(self, object_key: str) -> Optional[bytes]:
        """Download object from MinIO. Strips s3://bucket/ prefix if present."""
        key = object_key
        if object_key.startswith("s3://"):
            parts = object_key[5:].split("/", 1)
            key = parts[1] if len(parts) > 1 else ""
        try:
            resp = self.minio.get_object(self.bucket, key)
            data = resp.read()
            resp.close()
            resp.release_conn()
            return data
        except Exception as e:
            print(f"[ERROR] MinIO download failed ({key}): {e}")
            return None

    def _upload_bev(self, bev_img_rgb: np.ndarray, object_key: str):
        """Encode BEV image as JPEG and upload to MinIO."""
        bev_bgr = cv2.cvtColor(bev_img_rgb, cv2.COLOR_RGB2BGR)
        _, buf = cv2.imencode(".jpg", bev_bgr)
        data = buf.tobytes()
        self.minio.put_object(
            bucket_name=self.bucket,
            object_name=object_key,
            data=BytesIO(data),
            length=len(data),
            content_type="image/jpeg",
        )
