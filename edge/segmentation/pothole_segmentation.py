import cv2
import numpy as np
import os
import json
from datetime import datetime
from ultralytics import YOLO


class PotholeSegmentationYOLO:
    def __init__(
        self,
        model_path,
        trapezoid_coords,
        confidence_threshold=0.25,
        frame_interval=30,
    ):
        """
        Initialize the pothole segmentation processor.

        Args:
            model_path: Path to YOLOv11 segmentation model weights
            trapezoid_coords: np.array of shape (4, 2) - detection region normalized coordinates
            confidence_threshold: Confidence threshold for YOLO detections
            frame_interval: Process every Nth frame
        """
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.frame_interval = frame_interval

        # Load the normalized trapezoid coordinates
        self.normalized_trapezoid = trapezoid_coords.astype(np.float32)

        # Load segmentation model
        self.model = self.load_model()

    def load_model(self):
        """Load YOLOv11 segmentation model"""
        print(f"Loading YOLOv11 model from {self.model_path}...")
        model = YOLO(self.model_path)
        print("✓ Model loaded successfully")
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

    def segment_potholes(self, frame_rgb) -> list[tuple[np.ndarray, float]]:
        """
        Segment potholes using YOLOv11 on masked image.

        Args:
            frame_rgb: Input frame in RGB format

        Returns: List of tuples (mask, confidence), where mask is np.array of shape (N, 2)
        """
        # Create masked image (black outside trapezoid)
        masked_image = self.create_masked_image(frame_rgb)

        # Run YOLO segmentation
        results = self.model.predict(
            masked_image, conf=self.confidence_threshold, verbose=False
        )

        pothole_masks = []

        # Process each detection
        if results and results[0].masks is not None:
            print("=" * 50)
            print(f"Detections found: {len(results[0].masks)}")
            print("=" * 50)
            masks_data = results[0].masks.xy if hasattr(results[0].masks, "xy") else []
            confidences = results[0].boxes.conf if results[0].boxes is not None else []

            for i, contour in enumerate(masks_data):
                if i < len(confidences):
                    confidence = confidences[i].item()

                    # Convert to numpy array if needed
                    if not isinstance(contour, np.ndarray):
                        contour = np.array(contour)

                    # Ensure it's in the right format (N, 2)
                    if contour.shape[0] > 2:  # Need at least 3 points for a polygon
                        pothole_masks.append((contour.astype(np.float32), confidence))

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
            cv2.imshow("Pothole Segmentation", display_frame)

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
