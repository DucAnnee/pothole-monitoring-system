import cv2
import numpy as np
import os
import json
from datetime import datetime
from rfdetr import RFDETRSegPreview


class PotholeSegmentationRFDETR:
    """
    Pothole segmentation using RF-DETR from Roboflow.

    This class provides the same interface as PotholeSegmentationYOLO
    to ensure compatibility with the existing pipeline.
    """

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
            model_path: Path to RF-DETR segmentation model weights (or None for pretrained)
            trapezoid_coords: np.array of shape (4, 2) - detection region normalized coordinates
            confidence_threshold: Confidence threshold for detections
            frame_interval: Process every Nth frame
        """
        if RFDETRSegPreview is None:
            raise ImportError(
                "rfdetr package is not installed. " "Install with: pip install rfdetr"
            )

        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.frame_interval = frame_interval

        # Load the normalized trapezoid coordinates
        self.normalized_trapezoid = trapezoid_coords.astype(np.float32)

        # Load segmentation model
        self.model = self.load_model()

    def load_model(self):
        """Load RF-DETR segmentation model"""
        print(f"Loading RF-DETR model...")

        # If model_path is provided and exists, use it as pretrain_weights
        if self.model_path and os.path.exists(self.model_path):
            print(f"Loading custom weights from {self.model_path}")
            model = RFDETRSegPreview(pretrain_weights=self.model_path)
        else:
            # Use default pretrained weights
            print("Loading pretrained RF-DETR-Seg-Preview weights")
            model = RFDETRSegPreview()

        # Optionally optimize for inference (gives ~2x speedup)
        try:
            model.optimize_for_inference()
            print("✓ Model optimized for inference")
        except Exception as e:
            print(f"Warning: Could not optimize model for inference: {e}")

        print("✓ RF-DETR model loaded successfully")
        return model

    def create_masked_image(self, frame_rgb):
        """
        Create a masked image where pixels outside the trapezoid are black.
        This focuses the model on the detection region.
        Uses normalized coordinates that adapt to frame resolution.
        """
        h, w = frame_rgb.shape[:2]

        # Denormalize trapezoid for current frame dimensions
        current_coords = (self.normalized_trapezoid * np.array([w, h])).astype(np.int32)

        # Create mask for the trapezoid region
        mask = np.zeros((h, w), dtype=np.uint8)
        cv2.fillPoly(mask, [current_coords], 255)

        # Apply mask to the image
        masked_image = frame_rgb.copy()
        masked_image[mask == 0] = 0  # Set pixels outside trapezoid to black

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
        # Denormalize trapezoid for current frame
        trapezoid = (self.normalized_trapezoid * np.array([w, h])).astype(np.float32)

        # Check if all points of the pothole are inside the trapezoid
        for point in pothole_mask:
            if not self.point_in_polygon(point, trapezoid):
                return False
        return True

    def _mask_to_contour(self, binary_mask):
        """
        Convert a binary mask to contour coordinates.

        Args:
            binary_mask: np.array of shape (H, W) with boolean or 0/1 values

        Returns:
            np.array of shape (N, 2) containing contour coordinates, or None if no contour found
        """
        # Ensure mask is uint8
        mask_uint8 = (binary_mask > 0).astype(np.uint8) * 255

        # Find contours
        contours, _ = cv2.findContours(
            mask_uint8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
        )

        if not contours:
            return None

        # Get the largest contour (in case there are multiple)
        largest_contour = max(contours, key=cv2.contourArea)

        # Reshape from (N, 1, 2) to (N, 2)
        contour_points = largest_contour.reshape(-1, 2).astype(np.float32)

        return contour_points

    def segment_potholes(self, frame_rgb) -> list[tuple[np.ndarray, float]]:
        """
        Segment potholes using RF-DETR on masked image.

        Args:
            frame_rgb: Input frame in RGB format

        Returns: List of tuples (mask, confidence), where mask is np.array of shape (N, 2)
                 This matches the output format of PotholeSegmentationYOLO.segment_potholes()
        """
        # Create masked image (black outside trapezoid)
        masked_image = self.create_masked_image(frame_rgb)

        # Run RF-DETR prediction
        # RF-DETR expects RGB images
        detections = self.model.predict(
            masked_image, threshold=self.confidence_threshold
        )

        pothole_masks = []

        # Process detections
        if detections is not None and len(detections) > 0:
            print("=" * 50)
            print(f"Detections found: {len(detections)}")
            print("=" * 50)

            # RF-DETR returns sv.Detections object
            # detections.mask contains binary masks of shape (N, H, W)
            # detections.confidence contains confidence scores
            # detections.class_id contains class IDs

            if hasattr(detections, "mask") and detections.mask is not None:
                masks = detections.mask  # Shape: (N, H, W)
                confidences = detections.confidence
                class_ids = detections.class_id

                print(f"Masks shape: {masks.shape}")
                print(f"Confidences: {confidences}")

                for i in range(len(masks)):
                    confidence = float(confidences[i])
                    binary_mask = masks[i]  # Shape: (H, W)

                    # Convert binary mask to contour coordinates
                    contour = self._mask_to_contour(binary_mask)

                    if contour is not None and len(contour) > 2:
                        # Need at least 3 points for a polygon
                        pothole_masks.append((contour, confidence))
            else:
                print("No masks in detections (detection mode only)")

        return pothole_masks

    def process_video(self, video_path, output_dir):
        """
        Process video and save frames with detected potholes.

        Args:
            video_path: Path to the input video
            output_dir: Directory to save frames with potholes

        Returns: Number of frames with potholes detected
        """
        if not video_path:
            raise ValueError("Video path must be provided. Currently None.")

        os.makedirs(output_dir, exist_ok=True)

        cap = cv2.VideoCapture(video_path)

        if not cap.isOpened():
            print(f"Error: Could not open video {video_path}")
            return 0

        # Get video properties
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        print(f"Processing video: {video_path}")
        print(f"FPS: {fps}, Total frames: {total_frames}")
        print(f"Processing every {self.frame_interval} frames")
        print(f"Press 'q' to quit\n")

        frame_idx = 0
        saved_count = 0

        while cap.isOpened():
            ret, frame = cap.read()

            if not ret:
                break

            # Convert BGR to RGB
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

            # Create display frame (BGR for cv2.imshow)
            display_frame = frame.copy()
            h, w = frame.shape[:2]

            # Denormalize trapezoid for display
            display_trapezoid = (self.normalized_trapezoid * np.array([w, h])).astype(
                np.int32
            )

            # Draw trapezoid detection area (not filled)
            cv2.polylines(
                display_frame,
                [display_trapezoid],
                True,
                (0, 255, 0),
                2,
            )

            # Process frame at intervals
            if frame_idx % self.frame_interval == 0:
                # Segment potholes
                pothole_masks = self.segment_potholes(frame_rgb)

                # Check if any potholes are in detection area
                valid_masks = []
                for pothole_mask, confidence in pothole_masks:
                    if self.pothole_in_trapezoid(pothole_mask, frame.shape):
                        valid_masks.append((pothole_mask, confidence))

                        # Draw pothole on display frame
                        cv2.fillPoly(
                            display_frame, [pothole_mask.astype(np.int32)], (255, 0, 0)
                        )  # Blue fill
                        cv2.polylines(
                            display_frame,
                            [pothole_mask.astype(np.int32)],
                            True,
                            (0, 0, 255),
                            2,
                        )  # Red outline

                # Save frame if potholes detected
                if len(valid_masks) > 0:
                    # Iso format timestamp
                    timestamp = datetime.now().isoformat(
                        timespec="milliseconds", sep="T"
                    )
                    base_name = f"frame_{frame_idx:06d}_{timestamp}"

                    # Save original frame
                    frame_path = os.path.join(output_dir, f"{base_name}.jpg")
                    cv2.imwrite(frame_path, frame)

                    # Save all pothole masks for this frame into a single JSON file
                    mask_path = os.path.join(output_dir, f"{base_name}_mask.json")
                    masks_to_save = []
                    for mask, conf in valid_masks:
                        masks_to_save.append(
                            {
                                "conf": conf,
                                "coordinates": mask.tolist(),
                            }
                        )
                    with open(mask_path, "w") as f:
                        json.dump(masks_to_save, f)

                    saved_count += 1
                    print(
                        f"✓ Saved frame {frame_idx}: {len(valid_masks)} pothole(s) detected"
                    )

            # Display frame
            cv2.imshow("Pothole Segmentation (RF-DETR)", display_frame)

            # Check for quit
            if cv2.waitKey(1) & 0xFF == ord("q"):
                print("\nStopping video processing...")
                break

            frame_idx += 1

            # Progress
            if frame_idx % 100 == 0:
                print(
                    f"Processed {frame_idx}/{total_frames} frames "
                    f"({100*frame_idx/total_frames:.1f}%)"
                )

        cap.release()
        cv2.destroyAllWindows()

        print(f"\n{'='*50}")
        print(f"Segmentation complete!")
        print(f"Total frames with potholes: {saved_count}")
        print(f"Results saved to: {output_dir}")
        print(f"{'='*50}")

        return saved_count
