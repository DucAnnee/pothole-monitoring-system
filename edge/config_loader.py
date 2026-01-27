from typing import Any, Dict, Literal
import yaml
import os
import numpy as np


class ConfigLoader:
    """
    Load and validate configuration from YAML file.
    Supports environment variable substitution for sensitive data.
    """

    def __init__(self, config_path="config.yaml"):
        """
        Initialize config loader.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = self._load_config()
        self._validate_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please create a config.yaml file or specify the correct path."
            )

        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        # Substitute environment variables
        config = self._substitute_env_vars(config)

        return config

    def _substitute_env_vars(self, config) -> Any:
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

    def _validate_config(self):
        """Validate required configuration fields"""
        required_fields = [
            "model_type",
            "video",
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

        # Validate model type
        if self.config["model_type"] not in ["yolo", "rfdetr"]:
            raise ValueError(
                f"Invalid model_type: {self.config['model_type']}. Must be 'yolo' or 'rfdetr'"
            )

    # ========================================================================
    # Convenience getters for accessing configuration values
    # ========================================================================
    def get_enable_monitoring(self) -> bool:
        """Check if monitoring is enabled"""
        return self.config.get("enable_monitoring", False)

    def get_model_type(self) -> Literal["yolo", "rfdetr"]:
        """Get selected model type"""
        return self.config["model_type"]

    def get_model_config(self) -> dict:
        """Get configuration for the selected model"""
        model_type = self.get_model_type()
        return self.config["models"][model_type]

    def get_model_path(self) -> str:
        """Get model weights path"""
        model_config = self.get_model_config()
        return model_config["weights_path"]

    def get_confidence_threshold(self) -> float:
        """Get confidence threshold for the selected model"""
        model_config = self.get_model_config()
        return model_config.get("confidence_threshold", 0.25)

    def get_trapezoid_coords(self) -> np.ndarray:
        """Get trapezoid coordinates as numpy array"""
        coords = self.config["detection_region"]["trapezoid_coords"]
        return np.array(coords, dtype=np.float32)

    def get_rectangle_coords(self) -> np.ndarray:
        """Get BEV rectangle coordinates as numpy array"""
        coords = self.config["bev_calibration"]["rectangle_coords"]
        return np.array(coords, dtype=np.float32)

    def get_calibration_path(self):
        """Get camera calibration file path"""
        return self.config["camera"]["calibration_file"]

    def get_output_dirs(self) -> dict:
        """Get output directories based on selected model"""
        model_type = self.get_model_type()
        output = self.config["output"]

        if model_type == "rfdetr":
            return {
                "segmentation": output["segmentation_dir_rfdetr"],
                "area_estimation": output["area_estimation_dir_rfdetr"],
            }
        else:
            return {
                "segmentation": output["segmentation_dir"],
                "area_estimation": output["area_estimation_dir"],
            }

    def get_frame_interval(self) -> int:
        """Get frame sampling interval"""
        return self.config["processing"]["frame_interval"]

    def get_display_enabled(self) -> bool:
        """Check if real-time display is enabled"""
        return self.config["processing"].get("enable_display", True)

    def get_display_window_name(self) -> str:
        """Get display window name"""
        return self.config["processing"].get("display_window_name", "Pothole Detection")

    def get_api_config(self):
        """Get API configuration (if available)"""
        return self.config.get("api", None)

    def get_database_config(self):
        """Get database configuration (if available)"""
        return self.config.get("database", None)

    def get_kafka_config(self):
        """Get Kafka configuration"""
        return self.config.get("kafka", {})

    def get_kafka_topic(self) -> str:
        """Get Kafka topic name"""
        return self.config.get("kafka", {}).get("topic", "pothole.raw.events.v1")

    def get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        return self.config.get("kafka", {}).get(
            "bootstrap_servers", "localhost:19092,localhost:29092,localhost:39092"
        )

    def get_kafka_schema_registry_url(self) -> str:
        """Get Schema Registry URL"""
        return self.config.get("kafka", {}).get(
            "schema_registry_url", "http://localhost:8082"
        )

    def get_minio_config(self):
        """Get MinIO configuration"""
        return self.config.get("minio", {})

    def get_gps_bounds(self):
        """Get GPS simulation bounds"""
        return self.config.get("gps", {})

    def get_uploader_config(self):
        """Get uploader configuration"""
        return self.config.get("uploader", {})

    def print_config_summary(self):
        """Print a summary of the loaded configuration"""
        print("=" * 70)
        print("CONFIGURATION SUMMARY")
        print("=" * 70)
        print(f"Model Type: {self.get_model_type().upper()}")
        print(f"Model Path: {self.get_model_path()}")
        print(f"Video Path: {self.get_video_path()}")
        print(f"Confidence Threshold: {self.get_confidence_threshold()}")
        print(f"Frame Interval: {self.get_frame_interval()}")
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


def load_config(config_path="config.yaml"):
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
