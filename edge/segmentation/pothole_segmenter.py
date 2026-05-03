from __future__ import annotations

from abc import ABC, abstractmethod
import os
from typing import Any, Sequence, TypeAlias

import cv2
import numpy as np

from data_models import BoundingBox, ModelType, SegmentedPothole
from pipeline_logger import get_pipeline_logger, log_event

MaskResult: TypeAlias = list[SegmentedPothole]
LOGGER = get_pipeline_logger("segmenter")


class PotholeSegmenter(ABC):
    def __init__(
        self,
        model_path: str,
        trapezoid_coords: np.ndarray,
        confidence_threshold: float = 0.25,
        frame_interval: int = 30,
    ) -> None:
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.frame_interval = frame_interval
        self.normalized_trapezoid = trapezoid_coords.astype(np.float32)
        self.model = self.load_model()

    @staticmethod
    def create(
        model_type: ModelType,
        model_path: str,
        trapezoid_coords: np.ndarray,
        confidence_threshold: float = 0.25,
        frame_interval: int = 30,
    ) -> PotholeSegmenter:
        """
        Factory method to load the appropriate pothole segmenter.

        Args:
            model_type: Type of segmentation model ("yolo" or "rfdetr")
            model_path: Path to the model weights
            trapezoid_coords: np.array of shape (4, 2) - detection region normalized coordinates
            confidence_threshold: Confidence threshold for detections
            frame_interval: Process every Nth frame
        """
        if model_type == "yolo":
            return YOLOSegmenter(
                model_path,
                trapezoid_coords,
                confidence_threshold,
                frame_interval,
            )
        elif model_type == "rfdetr":
            return RFDETRSegmenter(
                model_path,
                trapezoid_coords,
                confidence_threshold,
                frame_interval,
            )
        else:
            raise ValueError(
                f"Unsupported model type: {model_type}. Choose 'yolo' or 'rfdetr'."
            )

    @abstractmethod
    def load_model(self) -> Any:
        """
        Load the corresponding model of the class
        """
        raise NotImplementedError("Class has to implement the load_model() method")

    @abstractmethod
    def segment(self, frame_rgb: np.ndarray) -> MaskResult:
        """
        Segment the input RGB frame

        Args:
        - frame_rgb (np.ndarray)

        Return:
        - List of segmentation results with mask, confidence, and bbox
        """
        raise NotImplementedError("Class has to implement the segment() method")

    def create_masked_image(self, frame_rgb: np.ndarray) -> np.ndarray:
        """
        Create a masked image where pixels outside the trapezoid are black.
        This focuses the model on the detection region.
        Uses normalized coordinates that adapt to frame resolution.
        """
        h, w = frame_rgb.shape[:2]

        # denormalize trapezoid for current frame dimensions
        current_coords = (self.normalized_trapezoid * np.array([w, h])).astype(np.int32)

        # create mask for the trapezoid region
        mask = np.zeros((h, w), dtype=np.uint8)
        cv2.fillPoly(mask, [current_coords], 255)

        # apply mask to the image
        masked_image = frame_rgb.copy()
        masked_image[mask == 0] = 0  # set pixels outside trapezoid to black

        return masked_image

    def point_in_polygon(self, point: Sequence[float], polygon: np.ndarray) -> bool:
        """Check if a point is inside a polygon"""
        return (
            cv2.pointPolygonTest(
                polygon.astype(np.float32),
                (float(point[0]), float(point[1])),
                False,
            )
            >= 0
        )

    def pothole_in_trapezoid(
        self,
        pothole_mask: np.ndarray,
        frame_shape: tuple[int, ...],
    ) -> bool:
        """
        Check if pothole mask is within the trapezoid detection area.

        Args:
            pothole_mask: np.array of shape (N, 2) - pothole coordinates
            frame_shape: (height, width) tuple of the frame
        """
        h, w = frame_shape[:2]

        # denormalize trapezoid for current frame
        trapezoid = (self.normalized_trapezoid * np.array([w, h])).astype(np.float32)

        # check if all points of the pothole are inside the trapezoid
        for point in pothole_mask:
            if not self.point_in_polygon(point, trapezoid):
                return False
        return True

    def cleanup(self) -> None:
        """Cleanup resources"""
        if hasattr(self, "model"):
            del self.model


class YOLOSegmenter(PotholeSegmenter):
    def __init__(
        self,
        model_path: str,
        trapezoid_coords: np.ndarray,
        confidence_threshold: float = 0.25,
        frame_interval: int = 30,
    ) -> None:
        """
        Initialize the pothole segmentation processor using YOLO.

        Args:
            model_path: Path to YOLO segmentation model weights
            trapezoid_coords: np.array of shape (4, 2) - detection region normalized coordinates
            confidence_threshold: Confidence threshold for detections
            frame_interval: Process every Nth frame
        """
        # import here to avoid dependency if class is not used
        try:
            from ultralytics.models.yolo import YOLO

            self.YOLO = YOLO
        except ImportError:
            raise ImportError(
                "ultralytics package is not installed. "
                "Install with: pip install ultralytics"
            )

        super().__init__(
            model_path, trapezoid_coords, confidence_threshold, frame_interval
        )

    def load_model(self) -> Any:
        """Load YOLO segmentation model"""
        log_event(LOGGER, "model_load_start", model_type="yolo", model_path=self.model_path)
        model = self.YOLO(self.model_path)
        log_event(LOGGER, "model_load_complete", model_type="yolo")
        return model

    def segment(self, frame_rgb: np.ndarray) -> MaskResult:
        """
        Segment potholes using YOLO segmentation model on masked image.

        Args:
            frame_rgb: Input frame in RGB format
        Returns: List of model outputs with mask, confidence, and bbox.
        """
        # create masked image
        masked_image = self.create_masked_image(frame_rgb)

        # run YOLO segmentation
        results = self.model.predict(
            masked_image, conf=self.confidence_threshold, verbose=False
        )

        pothole_masks: MaskResult = []

        # process each detection
        if results and results[0].masks is not None:
            masks_data = results[0].masks.xy if hasattr(results[0].masks, "xy") else []
            boxes = results[0].boxes
            confidences = boxes.conf if boxes is not None else []
            bboxes = boxes.xyxy if boxes is not None else []

            for i, contour in enumerate(masks_data):
                if i >= len(confidences) or i >= len(bboxes):
                    continue

                try:
                    bbox = _coerce_bbox(bboxes[i])
                except ValueError:
                    continue
                confidence = float(confidences[i].item())

                if not isinstance(contour, np.ndarray):
                    contour = np.array(contour)

                if contour.shape[0] > 2:
                    pothole_masks.append(
                        SegmentedPothole(
                            mask=contour.astype(np.float32),
                            confidence=confidence,
                            bbox=bbox,
                        )
                    )

        return pothole_masks


class RFDETRSegmenter(PotholeSegmenter):
    def __init__(
        self,
        model_path: str,
        trapezoid_coords: np.ndarray,
        confidence_threshold: float = 0.25,
        frame_interval: int = 30,
    ) -> None:
        """
        Initialize the pothole segmentation processor using RF-DETR.

        Args:
            model_path: Path to RF-DETR segmentation model weights
            trapezoid_coords: np.array of shape (4, 2) - detection region normalized coordinates
            confidence_threshold: Confidence threshold for detections
            frame_interval: Process every Nth frame
        """
        # import here to avoid dependency if class is not used
        try:
            from rfdetr import RFDETRSegPreview

            self.RFDETRSegPreview = RFDETRSegPreview
        except ImportError:
            raise ImportError(
                "rfdetr package is not installed. " "Install with: pip install rfdetr"
            )
        super().__init__(
            model_path, trapezoid_coords, confidence_threshold, frame_interval
        )

    def load_model(self) -> Any:
        """Load RF-DETR segmentation model"""
        log_event(
            LOGGER,
            "model_load_start",
            model_type="rfdetr",
            model_path=self.model_path,
        )

        # RF-DETR accepts a custom pretrain_weights path for local artifacts.
        if self.model_path and os.path.exists(self.model_path):
            log_event(
                LOGGER,
                "model_custom_weights_selected",
                model_type="rfdetr",
                model_path=self.model_path,
            )
            model = self.RFDETRSegPreview(pretrain_weights=self.model_path)
        else:
            log_event(LOGGER, "model_default_weights_selected", model_type="rfdetr")
            model = self.RFDETRSegPreview()

        log_event(LOGGER, "model_load_complete", model_type="rfdetr")
        return model

    def segment(self, frame_rgb: np.ndarray) -> MaskResult:
        """
        Segment potholes using RF-DETR segmentation model on masked image.

        Args:
            frame_rgb: Input frame in RGB format

        Returns: List of model outputs with mask, confidence, and bbox.
        """
        # create masked image
        masked_image = self.create_masked_image(frame_rgb)

        # run segmentation
        results = self.model.predict(masked_image, threshold=self.confidence_threshold)

        pothole_masks: MaskResult = []

        # process each detection
        if results is not None and len(results) > 0:
            # RF-DETR returns sv.Detections object
            # results.mask contains binary masks of shape (N, H, W)
            # results.confidence contains confidence scores
            if hasattr(results, "mask") and results.mask is not None:
                masks = results.mask  # shape: (N, H, W)
                bboxes = getattr(results, "bbox", None)
                if bboxes is None:
                    return pothole_masks
                confidences = results.confidence

                for i in range(len(masks)):
                    try:
                        bbox = _coerce_bbox(bboxes[i])
                    except ValueError:
                        continue
                    confidence = float(confidences[i])
                    binary_mask = masks[i]  # shape: (H, W)

                    # Convert binary mask to contour coordinates
                    contour = self._mask_to_contour(binary_mask)

                    if contour is not None and len(contour) > 2:
                        pothole_masks.append(
                            SegmentedPothole(
                                mask=contour,
                                confidence=confidence,
                                bbox=bbox,
                            )
                        )

        return pothole_masks

    def _mask_to_contour(self, binary_mask: np.ndarray) -> np.ndarray | None:
        """
        Convert a binary mask to contour coordinates.

        Args:
            binary_mask: np.array of shape (H, W) with boolean or 0/1 values

        Returns:
            np.array of shape (N, 2) containing contour coordinates, or None if no contour found
        """
        # ensure mask is uint8
        mask_uint8 = (binary_mask > 0).astype(np.uint8) * 255

        # find contours
        contours, _ = cv2.findContours(
            mask_uint8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
        )

        if not contours:
            return None

        # get the largest contour (in case there are multiple)
        largest_contour = max(contours, key=cv2.contourArea)

        # reshape from (N, 1, 2) to (N, 2)
        contour_points = largest_contour.reshape(-1, 2).astype(np.float32)

        return contour_points


def _coerce_bbox(value: Any) -> BoundingBox:
    """Convert a model-provided box to `(x1, y1, x2, y2)` floats."""
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if hasattr(value, "numpy"):
        value = value.numpy()

    try:
        coords = np.asarray(value, dtype=np.float32).reshape(-1)
    except (TypeError, ValueError) as exc:
        raise ValueError("Bounding box must contain numeric coordinates") from exc
    if coords.size < 4:
        raise ValueError("Bounding box must contain at least four coordinates")

    x1, y1, x2, y2 = coords[:4]
    return float(x1), float(y1), float(x2), float(y2)
