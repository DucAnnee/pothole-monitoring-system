import os
from typing import Any

import numpy as np
import yaml

from data_models import ModelType


class ConfigLoader:
    """
    Load and validate configuration from YAML file.
    Supports environment variable substitution for sensitive data.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize config loader.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: dict[str, Any] = self._load_config()
        self._validate_config()

    def _load_config(self) -> dict[str, Any]:
        """Load YAML configuration file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please create a config.yaml file or specify the correct path."
            )

        with open(self.config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        if not isinstance(config, dict):
            raise ValueError(
                f"Configuration root must be a mapping: {self.config_path}"
            )

        config = self._substitute_env_vars(config)

        return config

    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in config.
        Format: ${VAR_NAME} or ${VAR_NAME:default_value}
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Check if string contains environment variable
            if config.startswith("${") and config.endswith("}"):
                var_name = config[2:-1]
                # Support default values: ${VAR:default}
                if ":" in var_name:
                    var_name, default = var_name.split(":", 1)
                    return os.environ.get(var_name, default)
                else:
                    return os.environ.get(var_name, config)
        return config

    def _validate_config(self) -> None:
        """Validate required configuration fields"""
        required_fields = [
            "model_type",
            "models",
            "detection_region",
            "bev_calibration",
            "camera",
            "output",
            "processing",
        ]

        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")

        for field in ("detection_region", "bev_calibration", "camera", "output"):
            if not isinstance(self.config[field], dict):
                raise ValueError(f"{field} must be a mapping")

        if self.config["model_type"] not in ("yolo", "rfdetr"):
            raise ValueError(
                f"Invalid model_type: {self.config['model_type']}. Must be 'yolo' or 'rfdetr'"
            )

        model_type = self.config["model_type"]
        models = self.config["models"]
        if not isinstance(models, dict):
            raise ValueError("models must be a mapping")
        if model_type not in models:
            raise ValueError(f"Missing model config for model_type: {model_type}")

        model_config = models[model_type]
        if not isinstance(model_config, dict):
            raise ValueError(f"models.{model_type} must be a mapping")
        if not str(model_config.get("weights_path", "")).strip():
            raise ValueError(f"models.{model_type}.weights_path must be set")

        try:
            threshold = float(model_config.get("confidence_threshold", 0.25))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"models.{model_type}.confidence_threshold must be a number"
            ) from exc
        if threshold < 0 or threshold > 1:
            raise ValueError(
                f"models.{model_type}.confidence_threshold must be between 0 and 1"
            )

        processing = self.config["processing"]
        if not isinstance(processing, dict):
            raise ValueError("processing must be a mapping")

        frame_interval = processing.get("frame_interval")
        if not isinstance(frame_interval, int) or frame_interval <= 0:
            raise ValueError("processing.frame_interval must be a positive integer")

        camera_index = self.config["camera"].get("device_index", 0)
        if not isinstance(camera_index, int) or camera_index < 0:
            raise ValueError("camera.device_index must be a non-negative integer")

        self._validate_point_array(
            self.config["detection_region"].get("trapezoid_coords"),
            "detection_region.trapezoid_coords",
        )
        self._validate_point_array(
            self.config["bev_calibration"].get("rectangle_coords"),
            "bev_calibration.rectangle_coords",
        )

        if not str(self.config["camera"].get("calibration_file", "")).strip():
            raise ValueError("camera.calibration_file must be set")

        mlops_config = self.config.get("mlops", {})
        if mlops_config and not isinstance(mlops_config, dict):
            raise ValueError("mlops must be a mapping")
        model_update = mlops_config.get("model_update", {}) if mlops_config else {}
        if model_update and not isinstance(model_update, dict):
            raise ValueError("mlops.model_update must be a mapping")
        if "timeout_seconds" in model_update:
            try:
                timeout_seconds = float(model_update["timeout_seconds"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "mlops.model_update.timeout_seconds must be a number"
                ) from exc
            if timeout_seconds <= 0:
                raise ValueError(
                    "mlops.model_update.timeout_seconds must be greater than 0"
                )

        logging_config = self.config.get("logging", {})
        if logging_config and not isinstance(logging_config, dict):
            raise ValueError("logging must be a mapping")
        if logging_config:
            if "terminal_output" in logging_config and not isinstance(
                logging_config["terminal_output"],
                bool,
            ):
                raise ValueError("logging.terminal_output must be a boolean")
            if "file_enabled" in logging_config and not isinstance(
                logging_config["file_enabled"],
                bool,
            ):
                raise ValueError("logging.file_enabled must be a boolean")
            level = str(logging_config.get("level", "INFO")).upper()
            if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                raise ValueError(
                    "logging.level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
                )
            if logging_config.get("file_enabled", True) and not str(
                logging_config.get("file_path", "")
            ).strip():
                raise ValueError(
                    "logging.file_path must be set when file logging is enabled"
                )

        dedup_config = self.config.get("deduplication", {})
        if dedup_config and not isinstance(dedup_config, dict):
            raise ValueError("deduplication must be a mapping")
        if dedup_config:
            if "enabled" in dedup_config and not isinstance(
                dedup_config["enabled"],
                bool,
            ):
                raise ValueError("deduplication.enabled must be a boolean")
            try:
                iou_threshold = float(dedup_config.get("iou_threshold", 0.5))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "deduplication.iou_threshold must be a number"
                ) from exc
            if iou_threshold < 0 or iou_threshold > 1:
                raise ValueError("deduplication.iou_threshold must be between 0 and 1")

            max_age_frames = dedup_config.get("max_age_frames", 15)
            if not isinstance(max_age_frames, int) or max_age_frames <= 0:
                raise ValueError(
                    "deduplication.max_age_frames must be a positive integer"
                )

    def _validate_point_array(self, value: Any, field_name: str) -> None:
        """Validate a 4-point coordinate array used by geometry code."""
        try:
            coords = np.array(value, dtype=np.float32)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must contain numeric coordinates") from exc

        if coords.shape != (4, 2):
            raise ValueError(f"{field_name} must contain exactly four [x, y] points")

    # ========================================================================
    # Convenience getters for accessing configuration values
    # ========================================================================
    def get_enable_monitoring(self) -> bool:
        """Check if monitoring is enabled.

        Kept as a compatibility alias for older configs that used the
        top-level `enable_monitoring` field.
        """
        return bool(self.config.get("enable_monitoring", self.get_display_enabled()))

    def get_model_type(self) -> ModelType:
        """Get selected model type"""
        model_type = self.config["model_type"]
        if model_type not in ("yolo", "rfdetr"):
            raise ValueError(f"Invalid model_type: {model_type}")
        return model_type

    def get_model_config(self) -> dict[str, Any]:
        """Get configuration for the selected model"""
        model_type = self.get_model_type()
        return self.config["models"][model_type]

    def get_model_path(self) -> str:
        """Get model weights path"""
        model_config = self.get_model_config()
        return str(model_config["weights_path"])

    def get_confidence_threshold(self) -> float:
        """Get confidence threshold for the selected model"""
        model_config = self.get_model_config()
        return float(model_config.get("confidence_threshold", 0.25))

    def get_trapezoid_coords(self) -> np.ndarray:
        """Get trapezoid coordinates as numpy array"""
        coords = self.config["detection_region"]["trapezoid_coords"]
        return np.array(coords, dtype=np.float32)

    def get_rectangle_coords(self) -> np.ndarray:
        """Get BEV rectangle coordinates as numpy array"""
        coords = self.config["bev_calibration"]["rectangle_coords"]
        return np.array(coords, dtype=np.float32)

    def get_calibration_path(self) -> str:
        """Get camera calibration file path"""
        return str(self.config["camera"]["calibration_file"])

    def get_camera_index(self) -> int:
        """Get OpenCV camera device index."""
        return int(self.config["camera"].get("device_index", 0))

    def get_output_dirs(self) -> dict[str, str]:
        """Get output directories based on selected model"""
        model_type = self.get_model_type()
        output = self.config["output"]

        if model_type == "rfdetr":
            return {
                "segmentation": str(output["segmentation_dir_rfdetr"]),
                "area_estimation": str(output["area_estimation_dir_rfdetr"]),
            }
        else:
            return {
                "segmentation": str(output["segmentation_dir"]),
                "area_estimation": str(output["area_estimation_dir"]),
            }

    def get_frame_interval(self) -> int:
        """Get frame sampling interval"""
        return int(self.config["processing"]["frame_interval"])

    def get_display_enabled(self) -> bool:
        """Check if real-time display is enabled"""
        return bool(self.config["processing"].get("enable_display", True))

    def get_display_window_name(self) -> str:
        """Get display window name"""
        return str(
            self.config["processing"].get("display_window_name", "Pothole Detection")
        )

    def get_video_path(self) -> str | None:
        """Get optional default video path for local runs."""
        video_config = self.config.get("video", {})
        path = video_config.get("path")
        return str(path) if path else None

    def get_logging_config(self) -> dict[str, Any]:
        """Get pipeline event logging configuration."""
        return self.config.get("logging", {})

    def get_terminal_output_enabled(self) -> bool:
        """Return whether pipeline logs should be emitted to the terminal."""
        return bool(self.get_logging_config().get("terminal_output", True))

    def get_deduplication_config(self) -> dict[str, Any]:
        """Get IoU-based detection deduplication configuration."""
        return self.config.get("deduplication", {})

    def get_mlops_config(self) -> dict[str, Any]:
        """Get MLOps model lifecycle configuration."""
        return self.config.get("mlops", {})

    def get_model_registry_config(self) -> dict[str, Any]:
        """Get local model registry configuration."""
        return self.get_mlops_config().get("model_registry", {})

    def get_model_update_config(self) -> dict[str, Any]:
        """Get startup stable model update configuration."""
        return self.get_mlops_config().get("model_update", {})

    def get_api_config(self) -> dict[str, Any] | None:
        """Get API configuration (if available)"""
        return self.config.get("api", None)

    def get_database_config(self) -> dict[str, Any] | None:
        """Get database configuration (if available)"""
        return self.config.get("database", None)

    def get_kafka_config(self) -> dict[str, Any]:
        """Get Kafka configuration"""
        return self.config.get("kafka", {})

    def get_kafka_topic(self) -> str:
        """Get Kafka topic name"""
        return str(self.config.get("kafka", {}).get("topic", "pothole.raw.events.v2"))

    def get_video_path(self):
        """Get optional video path from config.

        The normal runtime path is supplied by edge/main.py --video. This getter
        keeps older config-summary/debug usage compatible without making video a
        required YAML field.
        """
        return self.config.get("video")

    def get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        return str(
            self.config.get("kafka", {}).get(
                "bootstrap_servers", "localhost:19092,localhost:29092,localhost:39092"
            )
        )

    def get_kafka_schema_registry_url(self) -> str:
        """Get Schema Registry URL"""
        return str(
            self.config.get("kafka", {}).get(
                "schema_registry_url", "http://localhost:8082"
            )
        )

    def get_minio_config(self) -> dict[str, Any]:
        """Get MinIO configuration"""
        return self.config.get("minio", {})

    def get_gps_bounds(self) -> dict[str, Any]:
        """Get GPS simulation bounds"""
        return self.config.get("gps", {})

    def get_uploader_config(self) -> dict[str, Any]:
        """Get uploader configuration"""
        return self.config.get("uploader", {})

    def print_config_summary(self) -> None:
        """Print a summary of the loaded configuration"""
        print("=" * 70)
        print("CONFIGURATION SUMMARY")
        print("=" * 70)
        print(f"Model Type: {self.get_model_type().upper()}")
        print(f"Model Path: {self.get_model_path()}")
        video_path = self.get_video_path() or f"camera device {self.get_camera_index()}"
        print(f"Default Input: {video_path}")
        print(f"Camera Device Index: {self.get_camera_index()}")
        print(f"Confidence Threshold: {self.get_confidence_threshold()}")
        print(f"Frame Interval: {self.get_frame_interval()}")
        logging_config = self.get_logging_config()
        print(
            "Terminal Output: "
            f"{'enabled' if self.get_terminal_output_enabled() else 'disabled'}"
        )
        if logging_config.get("file_enabled", True):
            log_path = logging_config.get("file_path", "logs/edge_pipeline.log")
            print(f"Log File: {log_path}")
        dedup_config = self.get_deduplication_config()
        if dedup_config:
            print(
                "Deduplication: "
                f"{'enabled' if dedup_config.get('enabled', False) else 'disabled'} "
                f"(IoU={dedup_config.get('iou_threshold', 0.5)}, "
                f"max_age_frames={dedup_config.get('max_age_frames', 15)})"
            )
        print(f"Calibration File: {self.get_calibration_path()}")

        output_dirs = self.get_output_dirs()
        print(f"Segmentation Output: {output_dirs['segmentation']}")
        print(f"Area Estimation Output: {output_dirs['area_estimation']}")

        # Show Kafka config if available
        if "kafka" in self.config:
            kafka_config = self.get_kafka_config()
            print(f"Kafka Topic: {kafka_config.get('topic', 'N/A')}")
            print(f"Kafka Brokers: {kafka_config.get('bootstrap_servers', 'N/A')}")

        # Show MinIO config if available
        if "minio" in self.config:
            minio_config = self.get_minio_config()
            print(f"MinIO Endpoint: {minio_config.get('endpoint', 'N/A')}")
            print(f"MinIO Bucket: {minio_config.get('bucket', 'N/A')}")

        print("=" * 70)
        print()


def load_config(config_path: str = "config.yaml") -> ConfigLoader:
    """
    Convenience function to load configuration.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        ConfigLoader instance
    """
    return ConfigLoader(config_path)


if __name__ == "__main__":
    # test configuration loader
    try:
        config = load_config()
        config.print_config_summary()
        print("Configuration loaded successfully!")
    except Exception as e:
        print(f"Configuration error: {e}")
