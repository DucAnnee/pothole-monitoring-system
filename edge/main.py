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
from pathlib import Path
from typing import Any, Mapping

from uploader import Uploader
from config_loader import ConfigLoader, load_config
from data_models import DetectionData, DetectionMask, ModelType, RuntimeModel
from mlops.model_manifest import ManifestError
from mlops.model_registry import ModelRegistry, RegistryError
from mlops.model_updater import UpdateError, deploy_from_manifest
from mlops.manifest_signature import SignatureError
from segmentation import PotholeSegmenter


# ============================================================================
# MAIN EDGE PIPELINE ORCHESTRATOR
# ============================================================================
class EdgePipeline:
    """Main Edge pipeline orchestrator."""

    def __init__(self, config_path: str = "config.yaml", video_path: str | None = None):
        """
        Initialize the edge pipeline.

        Args:
            config_path: Path to configuration file
            video_path: Optional video path. When omitted, the camera is used.
        """
        self.config: ConfigLoader = load_config(config_path)
        self.video_path: str | None = video_path
        self.vehicle_id = f"vehicle-{uuid4().hex[:8]}"
        self._sync_latest_stable_model()
        self.runtime_model: RuntimeModel = self._resolve_runtime_model()

        # control flags
        self.running = False
        self.threads = []
        self._stop_lock = threading.Lock()
        self._stopped = False

        # in-memory queue
        self.detection_queue = queue.Queue(maxsize=100)

        # modules init
        self.segmenter = self._initialize_segmenter()
        self.uploader = Uploader(self.config, self.vehicle_id)

        # signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _initialize_segmenter(self) -> PotholeSegmenter:
        """Initialize the segmenter from the resolved runtime model."""
        self.normalized_trapezoid = self.config.get_trapezoid_coords().astype(float)
        self.confidence_threshold = self.runtime_model.confidence_threshold
        print("[INFO] Initializing segmentation model...")
        print(
            "[INFO] Runtime model: "
            f"{self.runtime_model.model_id} "
            f"({self.runtime_model.model_type})"
        )
        print(f"[INFO] Model source: {self.runtime_model.source}")

        return PotholeSegmenter.create(
            model_type=self.runtime_model.model_type,
            model_path=self.runtime_model.model_path,
            trapezoid_coords=self.config.get_trapezoid_coords(),
            confidence_threshold=self.runtime_model.confidence_threshold,
        )

    # ========================================================================
    # Startup model lifecycle
    # ========================================================================
    def _sync_latest_stable_model(self) -> None:
        """Fetch, verify, and deploy the configured stable model manifest.

        When `fail_on_error` is false, update failures are logged and startup
        continues with the current registry model or static config fallback.
        """
        update_config = self.config.get_model_update_config()
        if not update_config.get("enabled", False):
            return

        manifest_uri = str(update_config.get("stable_manifest_uri") or "").strip()
        if not manifest_uri:
            message = (
                "mlops.model_update.enabled is true but stable_manifest_uri is empty"
            )
            if update_config.get("fail_on_error", False):
                raise RuntimeError(message)
            print(f"[WARN] {message}; skipping startup model update.")
            return

        print("[INFO] Checking latest stable model manifest...")
        try:
            deploy_kwargs: dict[str, Any] = {
                "manifest_uri": manifest_uri,
                "staging_dir": update_config.get("staging_dir", "models/staging"),
                "artifacts_dir": update_config.get("artifacts_dir", "models/artifacts"),
                "registry_path": self._registry_path(),
                "reason": update_config.get("reason", "Startup stable model update"),
                "operator": update_config.get("operator", "edge-startup"),
                "timeout_seconds": float(update_config.get("timeout_seconds", 30.0)),
                **self._manifest_signature_options(update_config),
            }
            deploy_kwargs["candidate_validator"] = self._smoke_load_candidate_model
            result = deploy_from_manifest(**deploy_kwargs)
        except (ManifestError, OSError, RegistryError, UpdateError, Exception) as exc:
            message = f"Startup model update failed: {exc}"
            if update_config.get("fail_on_error", False):
                raise RuntimeError(message) from exc
            print(f"[WARN] {message}. Continuing with local registry/config model.")
            return

        if result.get("action") == "already_active":
            print(f"[INFO] Stable model already active: {result.get('model_id')}")
            return

        deployment = result.get("deployment", {})
        print(f"[INFO] Deployed stable model: {deployment.get('model_id', 'unknown')}")

    def _resolve_runtime_model(self) -> RuntimeModel:
        """Resolve the model settings that the segmenter should load.

        The registry is preferred when enabled because it carries verified
        artifact metadata. If the registry is unavailable and fallback is
        allowed, the static `models` section in `config.yaml` is used.
        """
        registry_config = self.config.get_model_registry_config()
        if not registry_config.get("enabled", False):
            return self._config_runtime_model()

        registry_path = self._registry_path()
        try:
            registry = ModelRegistry(registry_path)
            active_model = registry.validate_model(registry.get_active_model())
        except (OSError, RegistryError) as exc:
            if not registry_config.get("fallback_to_config", True):
                raise RuntimeError(f"Model registry is not usable: {exc}") from exc
            print(f"[WARN] Model registry unavailable: {exc}. Falling back to config.")
            return self._config_runtime_model()

        return self._registry_runtime_model(active_model, registry_path)

    def _smoke_load_candidate_model(
        self,
        manifest: Mapping[str, Any],
        artifact_path: Path,
    ) -> None:
        """Load a candidate model before it is allowed to become active."""
        model_type = self._parse_model_type(manifest.get("model_type"))
        print(f"[INFO] Smoke-loading candidate model: {manifest['model_id']}")
        segmenter = PotholeSegmenter.create(
            model_type=model_type,
            model_path=str(artifact_path),
            trapezoid_coords=self.config.get_trapezoid_coords(),
            confidence_threshold=float(manifest["confidence_threshold"]),
            frame_interval=self.config.get_frame_interval(),
        )
        try:
            print(f"[INFO] Candidate model loaded successfully: {manifest['model_id']}")
        finally:
            segmenter.cleanup()

    def _registry_runtime_model(
        self,
        active_model: Mapping[str, Any],
        registry_path: str,
    ) -> RuntimeModel:
        """Adapt a validated registry model record to the runtime contract."""
        model_type = self._parse_model_type(active_model.get("model_type"))
        return RuntimeModel(
            model_id=str(active_model["model_id"]),
            model_type=model_type,
            model_path=str(active_model["artifact_path"]),
            confidence_threshold=float(active_model["confidence_threshold"]),
            source=f"registry:{registry_path}",
        )

    def _config_runtime_model(self) -> RuntimeModel:
        """Build runtime model settings from the static config fallback."""
        model_type = self.config.get_model_type()
        return RuntimeModel(
            model_id=f"{model_type}-config",
            model_type=model_type,
            model_path=self.config.get_model_path(),
            confidence_threshold=self.config.get_confidence_threshold(),
            source="config.yaml",
        )

    def _parse_model_type(self, model_type: Any) -> ModelType:
        """Validate a dynamic model type before assigning the typed contract."""
        if model_type not in ("yolo", "rfdetr"):
            raise RuntimeError(f"Unsupported runtime model_type: {model_type}")
        return model_type

    def _registry_path(self) -> str:
        """Return the configured model registry path."""
        registry_config = self.config.get_model_registry_config()
        return str(registry_config.get("registry_path", "mlops/model_registry.json"))

    def _manifest_signature_options(
        self,
        update_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Return manifest signature verification options for startup updates."""
        signature_config = update_config.get("manifest_signature", {})
        return {
            "signature_public_key_path": signature_config.get("public_key_path"),
            "expected_signature_key_id": signature_config.get("key_id", ""),
            "require_signature": bool(signature_config.get("required", False)),
        }

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        print("\n[INFO] Shutdown signal received. Stopping pipeline...")
        self.stop()

    def inference_worker(self) -> None:
        """Inference worker thread - processes video and produces detections."""
        print("[INFO] Inference worker started")
        cap = None

        try:
            import cv2

            if self.video_path:
                print("[INFO] Video path provided, running inference on video file")
                cap = cv2.VideoCapture(self.video_path)
                source = f"video: {self.video_path}"
            else:
                print("[INFO] Running inference on camera (Device 0)")
                # TODO: allow camera selection from config & poll available cameras
                cap = cv2.VideoCapture(0)
                source = "camera device 0"

            if not cap.isOpened():
                print(f"[ERROR] Could not open input source: {source}")
                self.running = False
                return

            frame_count = 0
            frame_interval = self.config.get_frame_interval()

            # monitor window init
            enable_monitoring = self.config.get_display_enabled()
            if enable_monitoring:
                window_name = self.config.get_display_window_name()
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
                        self.running = False
                        break

        except Exception as e:
            print(f"[ERROR] Inference worker failed: {e}")
            self.running = False
        finally:
            if cap is not None:
                cap.release()
            try:
                import cv2

                cv2.destroyAllWindows()
            except Exception:
                pass

    def uploading_worker(self) -> None:
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

    def start(self) -> None:
        """Start the pipeline."""
        print("=" * 70)
        print("EDGE DEVICE WORKER")
        print("=" * 70)
        print(f"Vehicle ID: {self.vehicle_id}")
        print(
            "Model: "
            f"{self.runtime_model.model_type.upper()} "
            f"({self.runtime_model.model_id})"
        )
        print(f"Model Path: {self.runtime_model.model_path}")
        print(f"Online: {self.uploader.is_online}")
        print("=" * 70)

        self.running = True

        # start worker threads
        inference_thread = threading.Thread(
            target=self.inference_worker,
            name="inference-worker",
            daemon=True,
        )
        uploading_thread = threading.Thread(
            target=self.uploading_worker,
            name="uploading-worker",
            daemon=True,
        )
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
            self.running = False
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop workers, flush pending uploads, and release model resources."""
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            self.running = False

            # wait for threads to finish, but never join the caller thread
            current_thread = threading.current_thread()
            for thread in self.threads:
                if thread is current_thread:
                    continue
                thread.join(timeout=5.0)

            alive_threads = [
                thread.name
                for thread in self.threads
                if thread is not current_thread and thread.is_alive()
            ]
            if alive_threads:
                print(
                    "[WARN] Worker thread(s) still running after shutdown timeout: "
                    f"{', '.join(alive_threads)}"
                )

            # flush Kafka producer
            self.uploader.flush()

            # print final stats
            self.uploader.print_stats()

            # delete model resources once workers are no longer using them
            if not alive_threads:
                self.segmenter.cleanup()

            print("[INFO] Pipeline stopped")


def main() -> None:
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
        help="Optional input video path. Omit this flag to use camera device 0.",
    )

    args = parser.parse_args()

    pipeline = EdgePipeline(config_path=args.config, video_path=args.video)
    pipeline.start()


if __name__ == "__main__":
    main()
