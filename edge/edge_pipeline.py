"""
Main edge device pipeline orchestrator.
Coordinates inference, processing, and uploading with in-memory queues.
"""

import queue
import threading
import time
import signal
import sys
import numpy as np
from uuid import uuid4
from datetime import datetime

from config_loader import load_config
from segmentation import PotholeSegmentationYOLO, PotholeSegmentationRFDETR
from processing_uploader import ProcessingUploader
from data_models import DetectionData, DetectionMask


class EdgePipeline:
    """Main pipeline orchestrator."""

    def __init__(self, config_path="config.yaml"):
        """
        Initialize the edge pipeline.

        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.vehicle_id = f"vehicle-{uuid4().hex[:8]}"

        # In-memory queue (max 100 items to prevent OOM)
        self.detection_queue = queue.Queue(maxsize=100)

        # Initialize modules
        self.segmenter = self._initialize_segmenter()
        self.processor = ProcessingUploader(self.config, self.vehicle_id)

        # Control flags
        self.running = False
        self.threads = []

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _initialize_segmenter(self):
        """Initialize the segmentation model."""
        model_type = self.config.get_model_type()
        self.normalized_trapezoid = self.config.get_trapezoid_coords().astype(float)
        self.confidence_threshold = self.config.get_confidence_threshold()
        print("[INFO] Initializing segmentation model...")

        if model_type == "yolo":
            return PotholeSegmentationYOLO(
                model_path=self.config.get_model_path(),
                trapezoid_coords=self.config.get_trapezoid_coords(),
                confidence_threshold=self.config.get_confidence_threshold(),
                frame_interval=self.config.get_frame_interval(),
            )
        elif model_type == "rfdetr":
            return PotholeSegmentationRFDETR(
                model_path=self.config.get_model_path(),
                trapezoid_coords=self.config.get_trapezoid_coords(),
                confidence_threshold=self.config.get_confidence_threshold(),
                frame_interval=self.config.get_frame_interval(),
            )
        else:
            raise ValueError(f"Unsupported model type: {model_type}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print("\n[INFO] Shutdown signal received. Stopping pipeline...")
        self.stop()

    def inference_worker(self):
        """Inference worker thread - processes video and produces detections."""
        print("[INFO] Inference worker started")

        video_path = self.config.get_video_path()
        if not video_path:
            print("[ERROR] can't resolve video path from configuration")
            self.running = False
            return

        try:
            import cv2

            cap = cv2.VideoCapture(video_path)

            if not cap.isOpened():
                print(f"[ERROR] Could not open video: {video_path}")
                return

            frame_count = 0
            frame_interval = self.config.get_frame_interval()

            # Create named window with proper flags
            window_name = "Pothole Segmentation"
            cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
            cv2.resizeWindow(window_name, 1280, 720)

            while self.running and cap.isOpened():
                ret, frame = cap.read()

                if not ret:
                    print("[INFO] Video ended, stopping...")
                    self.running = False
                    break

                frame_count += 1

                # Sample frames based on interval
                if frame_count % frame_interval != 0:
                    continue

                # Run inference
                display_frame = frame.copy()
                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

                # Use unified segment_potholes method (works for both YOLO and RF-DETR)
                pothole_masks = self.segmenter.segment_potholes(frame_rgb)

                # Denormalize trapezoid for display
                w, h = frame.shape[1], frame.shape[0]
                display_trapezoid = (
                    self.normalized_trapezoid * np.array([w, h])
                ).astype(np.int32)

                # Draw trapezoid detection area (not filled)
                cv2.polylines(
                    display_frame,
                    [display_trapezoid],
                    True,
                    (0, 255, 0),
                    2,
                )

                # Extract masks - filter for those in trapezoid
                masks = []
                for mask_data, confidence in pothole_masks:
                    if self.segmenter.pothole_in_trapezoid(mask_data, frame.shape):
                        # Draw pothole on display frame
                        cv2.fillPoly(
                            display_frame,
                            [mask_data.astype(np.int32)],
                            (255, 0, 0),
                        )
                        coordinates = mask_data.tolist()
                        masks.append(
                            DetectionMask(
                                conf=float(confidence), coordinates=coordinates
                            )
                        )

                # Only queue if potholes detected
                if masks:
                    detection = DetectionData(
                        frame_id=f"frame_{frame_count:06d}",
                        timestamp=datetime.now(),
                        frame=frame_rgb,
                        masks=masks,
                    )

                    try:
                        self.detection_queue.put(detection, timeout=1.0)
                        print(
                            f"[INFERENCE] Queued {len(masks)} detections from {detection.frame_id}"
                        )
                    except queue.Full:
                        print("[WARN] Detection queue full, dropping frame")

                # Display frame
                cv2.imshow(window_name, display_frame)

                # Check for quit key - MUST have waitKey for window to update
                key = cv2.waitKey(1) & 0xFF
                if key == ord("q"):
                    print("\n[INFO] 'q' pressed, stopping inference...")
                    self.stop()

            cap.release()
            cv2.destroyAllWindows()

        except Exception as e:
            print(f"[ERROR] Inference worker failed: {e}")

    def processing_worker(self):
        """Processing and upload worker thread."""
        print("[INFO] Processing worker started")

        # First, clear persistent storage if online
        if self.processor.is_online:
            print("[INFO] Clearing persistent storage...")
            self.processor.process_persistent_storage()
            self.processor.flush()

        # Process detection queue
        while self.running:
            try:
                # Get detection from queue (timeout to check running flag)
                detection = self.detection_queue.get(timeout=1.0)

                # Process detection
                enriched_list = self.processor.process_detection(detection)
                self.processor.stats["processed"] += len(enriched_list)

                # Upload or store
                for enriched in enriched_list:
                    if self.processor.is_online:
                        success = self.processor.upload_to_cloud(enriched)
                        if not success:
                            # Connection lost, store to disk
                            print("[WARN] Upload failed, storing to disk")
                            self.processor.store_to_disk(enriched)
                            self.processor.is_online = False
                    else:
                        # Offline mode, store to disk
                        self.processor.store_to_disk(enriched)

                        # Periodically check if back online
                        if self.processor.stats["stored"] % 10 == 0:
                            print("[INFO] Checking connection status...")
                            self.processor._initialize_connections()
                            if self.processor.is_online:
                                print("[INFO] Back online! Processing stored data...")
                                self.processor.process_persistent_storage()

                self.detection_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[ERROR] Processing worker failed: {e}")

    def start(self):
        """Start the pipeline."""
        print("=" * 70)
        print("EDGE DEVICE PIPELINE")
        print("=" * 70)
        print(f"Vehicle ID: {self.vehicle_id}")
        print(f"Model: {self.config.get_model_type().upper()}")
        print(f"Video: {self.config.get_video_path()}")
        print(f"Online: {self.processor.is_online}")
        print("=" * 70)

        self.running = True

        # Start worker threads
        inference_thread = threading.Thread(target=self.inference_worker, daemon=True)
        processing_thread = threading.Thread(target=self.processing_worker, daemon=True)

        inference_thread.start()
        processing_thread.start()

        self.threads = [inference_thread, processing_thread]

        print("[INFO] Pipeline started. Press Ctrl+C to stop.\n")

        # Monitor threads
        try:
            while self.running:
                time.sleep(1)

                # Print stats every 30 seconds
                if int(time.time()) % 30 == 0:
                    self.processor.print_stats()
                    print(f"Queue depth: {self.detection_queue.qsize()}")

        except KeyboardInterrupt:
            pass

    def stop(self):
        """Stop the pipeline."""
        self.running = False

        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5.0)

        # Flush Kafka producer
        self.processor.flush()

        # Print final stats
        self.processor.print_stats()

        print("[INFO] Pipeline stopped")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Edge Device Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    args = parser.parse_args()

    pipeline = EdgePipeline(config_path=args.config)
    pipeline.start()


if __name__ == "__main__":
    main()
