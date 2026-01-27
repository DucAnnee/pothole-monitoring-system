import os
from typing import Literal
import cv2
import numpy as np
from abc import ABC


class PotholeSegmenter(ABC):
    def __init__(
        self,
        model_path,
        trapezoid_coords,
        confidence_threshold=0.25,
        frame_interval=30,
    ):
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.frame_interval = frame_interval
        self.normalized_trapezoid = trapezoid_coords.astype(np.float32)
        self.model = self.load_model()

    @staticmethod
    def create(
        model_type: Literal["yolo", "rfdetr"],
        model_path,
        trapezoid_coords,
        confidence_threshold=0.25,
        frame_interval=30,
    ):
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

    def load_model(self):
        """
        Load the corresponding model of the class
        """
        raise NotImplementedError("Class has to implement the load_model() method")

    def segment(self, frame_rgb: np.ndarray):
        """
        Segment the input RGB frame

        Args:
        - frame_rgb (np.ndarray)

        Return:
        - List of tuples of segmentation masks and confidences
        """
        raise NotImplementedError("Class has to implement the segment() method")

    def create_masked_image(self, frame_rgb):
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

    def point_in_polygon(self, point, polygon):
        """Check if a point is inside a polygon"""
        return (
            cv2.pointPolygonTest(polygon.astype(np.float32), tuple(point), False) >= 0
        )

    def pothole_in_trapezoid(self, pothole_mask, frame_shape):
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

    def cleanup(self):
        """Cleanup resources"""
        del self.model


class YOLOSegmenter(PotholeSegmenter):
    def __init__(
        self,
        model_path,
        trapezoid_coords,
        confidence_threshold=0.25,
        frame_interval=30,
    ):
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

    def load_model(self):
        """Load YOLO segmentation model"""
        print(f"[INFO] Loading YOLO model from {self.model_path}...")
        model = self.YOLO(self.model_path)
        print("[INFO] YOLO model loaded successfully")
        return model

    def segment(self, frame_rgb) -> list[tuple[np.ndarray, float]]:
        """
        Segment potholes using YOLO segmentation model on masked image.

        Args:
            frame_rgb: Input frame in RGB format

        Returns: List of tuples (mask, confidence), where mask is np.array of shape (N, 2)
        """
        # create masked image
        masked_image = self.create_masked_image(frame_rgb)

        # run YOLO segmentation
        results = self.model.predict(
            masked_image, conf=self.confidence_threshold, verbose=False
        )

        pothole_masks = []

        # process each detection
        if results and results[0].masks is not None:
            print("=" * 70)
            print(f"[INFO] Detections found: {len(results[0].masks)}")
            print("=" * 70)
            masks_data = results[0].masks.xy if hasattr(results[0].masks, "xy") else []
            print(masks_data)
            confidences = results[0].boxes.conf if results[0].boxes is not None else []
            print(confidences)

            for i, contour in enumerate(masks_data):
                if i < len(confidences):
                    confidence = confidences[i].item()

                    # convert to numpy array
                    if not isinstance(contour, np.ndarray):
                        contour = np.array(contour)

                    # ensure shape (N, 2)
                    if contour.shape[0] > 2:  # need at least 3 points for a polygon
                        pothole_masks.append((contour.astype(np.float32), confidence))

        return pothole_masks


class RFDETRSegmenter(PotholeSegmenter):
    def __init__(
        self,
        model_path,
        trapezoid_coords,
        confidence_threshold=0.25,
        frame_interval=30,
    ):
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

    def load_model(self):
        """Load RF-DETR segmentation model"""
        print(f"[INFO] Loading RF-DETR model from {self.model_path}...")

        # if model_path is provided and exists, use it as pretrain_weights
        if self.model_path and os.path.exists(self.model_path):
            print(f"[INFO] Loading custom weights from {self.model_path}")
            model = self.RFDETRSegPreview(pretrain_weights=self.model_path)
        else:
            # use default pretrained weights
            print("Loading pretrained RF-DETR-Seg-Preview weights")
            model = self.RFDETRSegPreview()

        print("[INFO] RF-DETR model loaded successfully")
        print(model)
        return model

    def segment(self, frame_rgb) -> list[tuple[np.ndarray, float]]:
        """
        Segment potholes using RF-DETR segmentation model on masked image.

        Args:
            frame_rgb: Input frame in RGB format

        Returns: List of tuples (mask, confidence), where mask is np.array of shape (N, 2)
        """
        # create masked image
        masked_image = self.create_masked_image(frame_rgb)

        # run segmentation
        results = self.model.predict(masked_image, threshold=self.confidence_threshold)

        pothole_masks = []

        # process each detection
        if results is not None and len(results) > 0:
            print("=" * 70)
            print(f"[INFO] Detections found: {len(results)}")
            print("=" * 70)

            # RF-DETR returns sv.Detections object
            # results.mask contains binary masks of shape (N, H, W)
            # results.confidence contains confidence scores
            # results.class_id contains class IDs
            if hasattr(results, "mask") and results.mask is not None:
                masks = results.mask  # shape: (N, H, W)
                confidences = results.confidence
                # class_ids = results.class_id

                print(f"Masks shape: {masks.shape}")
                print(f"Confidences: {confidences}")

                for i in range(len(masks)):
                    confidence = float(confidences[i])
                    binary_mask = masks[i]  # shape: (H, W)

                    # Convert binary mask to contour coordinates
                    contour = self._mask_to_contour(binary_mask)

                    if contour is not None and len(contour) > 2:
                        # Need at least 3 points for a polygon
                        pothole_masks.append((contour, confidence))
            else:
                print("No masks in detections (detection mode only)")

        return pothole_masks

    def _mask_to_contour(self, binary_mask):
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
