"""
Main edge device pipeline orchestrator.
Coordinates inference and uploading with in-memory queues.
"""

import time
import queue
import signal
import threading
import numpy as np
from uuid import uuid4
from datetime import datetime

from uploader import Uploader
from config_loader import load_config
from data_models import DetectionData, DetectionMask
from segmentation import PotholeSegmenter


# ============================================================================
# MAIN EDGE PIPELINE ORCHESTRATOR
# ============================================================================
class EdgePipeline:
    """Main Edge pipeline orchestrator."""

    def __init__(self, config_path="config.yaml", video_path=None):
        """
        Initialize the edge pipeline.

        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.video_path = video_path
        self.vehicle_id = f"vehicle-{uuid4().hex[:8]}"

        # in-memory queue
        self.detection_queue = queue.Queue(maxsize=100)

        # modules init
        self.segmenter = self._initialize_segmenter()
        self.uploader = Uploader(self.config, self.vehicle_id)

        # control flags
        self.running = False
        self.threads = []

        # signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _initialize_segmenter(self):
        """Initialize the segmentation model."""
        model_type = self.config.get_model_type()
        self.normalized_trapezoid = self.config.get_trapezoid_coords().astype(float)
        self.confidence_threshold = self.config.get_confidence_threshold()
        print("[INFO] Initializing segmentation model...")

        return PotholeSegmenter.create(
            model_type=model_type,
            model_path=self.config.get_model_path(),
            trapezoid_coords=self.config.get_trapezoid_coords(),
            confidence_threshold=self.config.get_confidence_threshold(),
        )

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print("\n[INFO] Shutdown signal received. Stopping pipeline...")
        self.stop()

    def inference_worker(self):
        """Inference worker thread - processes video and produces detections."""
        print("[INFO] Inference worker started")

        try:
            import cv2

            if self.video_path:
                print("[INFO] Video path provided, running inference on video file")
                cap = cv2.VideoCapture(self.video_path)
            else:
                print("[INFO] Running inference on camera (Device 0)")
                # TODO: allow camera selection from config & poll available cameras
                cap = cv2.VideoCapture(0)

            if not cap.isOpened():
                print(f"[ERROR] Could not open video: {self.video_path}")
                return

            frame_count = 0
            frame_interval = self.config.get_frame_interval()

            # monitor window init
            enable_monitoring = self.config.get_enable_monitoring()
            if enable_monitoring:
                window_name = "Pothole Segmentation"
                cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
                cv2.resizeWindow(window_name, 1280, 720)

            while self.running and cap.isOpened():
                ret, frame = cap.read()

                if not ret:
                    print("[INFO] End of video stream or cannot fetch frame.")
                    self.running = False
                    break

                frame_count += 1

                # sample frames based on interval
                if frame_count % frame_interval != 0:
                    continue

                # run inference
                display_frame = frame.copy()
                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                pothole_masks = self.segmenter.segment(frame_rgb)

                # denormalize trapezoid for display
                w, h = frame.shape[1], frame.shape[0]
                display_trapezoid = (
                    self.normalized_trapezoid * np.array([w, h])
                ).astype(np.int32)

                # draw trapezoid detection area
                cv2.polylines(
                    display_frame,
                    [display_trapezoid],
                    True,
                    (0, 255, 0),
                    2,
                )

                # get segmentation masks and filter for those in trapezoid area
                masks = []
                for mask_data, confidence in pothole_masks:
                    if self.segmenter.pothole_in_trapezoid(mask_data, frame.shape):
                        # draw pothole on display frame
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

                # only queue if potholes detected
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

                # display frame
                if enable_monitoring:
                    cv2.imshow(window_name, display_frame)  # type: ignore

                # check for quit key
                key = cv2.waitKey(1) & 0xFF
                if key == ord("q"):
                    print("\n[INFO] 'q' pressed, stopping inference...")
                    self.stop()

            cap.release()
            cv2.destroyAllWindows()

        except Exception as e:
            print(f"[ERROR] Inference worker failed: {e}")

    def uploading_worker(self):
        """Uploading worker thread."""
        print("[INFO] Uploading worker started")

        # process local storage if online
        if self.uploader.is_online:
            print("[INFO] Processing local storage...")
            self.uploader.process_local_storage()
            self.uploader.flush()

        # process detection queue
        while self.running:
            try:
                # get detection from queue
                detection = self.detection_queue.get(timeout=1.0)

                # process detection
                enriched_list = self.uploader.process_detection(detection)
                self.uploader.stats["processed"] += len(enriched_list)

                # upload or store
                for enriched in enriched_list:
                    if self.uploader.is_online:
                        success = self.uploader.upload_to_cloud(enriched)
                        if not success:
                            # connection lost, store to disk
                            print("[WARN] Upload failed, storing to disk")
                            self.uploader.store_to_disk(enriched)
                            self.uploader.is_online = False
                    else:
                        # offline mode, store to disk
                        self.uploader.store_to_disk(enriched)

                        # periodically check if back online
                        if self.uploader.stats["stored"] % 10 == 0:
                            print("[INFO] Checking connection status...")
                            self.uploader._initialize_connections()
                            if self.uploader.is_online:
                                print("[INFO] Back online! Processing stored data...")
                                self.uploader.process_local_storage()

                self.detection_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[ERROR] Uploading worker failed: {e}")

    def start(self):
        """Start the pipeline."""
        print("=" * 70)
        print("EDGE DEVICE WORKER")
        print("=" * 70)
        print(f"Vehicle ID: {self.vehicle_id}")
        print(f"Model: {self.config.get_model_type().upper()}")
        print(f"Online: {self.uploader.is_online}")
        print("=" * 70)

        self.running = True

        # start worker threads
        inference_thread = threading.Thread(target=self.inference_worker, daemon=True)
        uploading_thread = threading.Thread(target=self.uploading_worker, daemon=True)
        inference_thread.start()
        uploading_thread.start()

        self.threads = [inference_thread, uploading_thread]

        print("[INFO] Pipeline started. Press Ctrl+C to stop.\n")

        # threads monitoring
        try:
            while self.running:
                time.sleep(1)

                # print stats every 30 seconds
                if int(time.time()) % 30 == 0:
                    self.uploader.print_stats()
                    print(f"Queue depth: {self.detection_queue.qsize()}")

        except KeyboardInterrupt:
            pass

    def stop(self):
        """Stop the edge worker"""
        self.running = False

        # wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5.0)

        # flush Kafka producer
        self.uploader.flush()

        # print final stats
        self.uploader.print_stats()

        # delete model resources
        self.segmenter.cleanup()

        print("[INFO] Pipeline stopped")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Edge device worker")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--video",
        type=str,
        default="./assets/test.mp4",
        help="Path to input video file to run test on (default: ./assets/test.mp4)",
    )
    args = parser.parse_args()

    pipeline = EdgePipeline(config_path=args.config, video_path=args.video)
    pipeline.start()


if __name__ == "__main__":
    main()
