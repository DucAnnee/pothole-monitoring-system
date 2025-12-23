"""
Configuration loader for cloud depth estimation service.
Loads and validates configuration from YAML file with environment variable support.
"""

from typing import Any, Dict
import yaml
import os


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
            "kafka",
            "minio",
            "depth_model",
            "processing",
        ]

        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")

        # Validate Kafka config
        kafka_fields = [
            "source_topic",
            "output_topic",
            "bootstrap_servers",
            "schema_registry_url",
        ]
        for field in kafka_fields:
            if field not in self.config["kafka"]:
                raise ValueError(f"Missing required Kafka configuration: {field}")

        # Validate MinIO config
        minio_fields = ["endpoint", "access_key", "secret_key", "bucket"]
        for field in minio_fields:
            if field not in self.config["minio"]:
                raise ValueError(f"Missing required MinIO configuration: {field}")

        # Validate model config
        model_fields = ["model_type", "model_path"]
        for field in model_fields:
            if field not in self.config["depth_model"]:
                raise ValueError(f"Missing required model configuration: {field}")

    # ========================================================================
    # Convenience getters for Kafka configuration
    # ========================================================================

    def get_kafka_source_topic(self) -> str:
        """Get Kafka source topic"""
        return self.config["kafka"]["source_topic"]

    def get_kafka_output_topic(self) -> str:
        """Get Kafka output topic"""
        return self.config["kafka"]["output_topic"]

    def get_kafka_consumer_group_id(self) -> str:
        """Get Kafka consumer group ID"""
        return self.config["kafka"]["consumer_group_id"]

    def get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        return self.config["kafka"]["bootstrap_servers"]

    def get_kafka_schema_registry_url(self) -> str:
        """Get Schema Registry URL"""
        return self.config["kafka"]["schema_registry_url"]

    def get_kafka_max_retries(self) -> int:
        """Get Kafka max retries"""
        return self.config["kafka"].get("max_retries", 5)

    def get_kafka_retry_delay(self) -> int:
        """Get Kafka retry delay"""
        return self.config["kafka"].get("retry_delay", 2)

    # ========================================================================
    # Convenience getters for MinIO configuration
    # ========================================================================

    def get_minio_endpoint(self) -> str:
        """Get MinIO endpoint"""
        return self.config["minio"]["endpoint"]

    def get_minio_access_key(self) -> str:
        """Get MinIO access key"""
        return self.config["minio"]["access_key"]

    def get_minio_secret_key(self) -> str:
        """Get MinIO secret key"""
        return self.config["minio"]["secret_key"]

    def get_minio_bucket(self) -> str:
        """Get MinIO bucket"""
        return self.config["minio"]["bucket"]

    def get_minio_secure(self) -> bool:
        """Get MinIO secure flag"""
        return self.config["minio"].get("secure", False)

    def get_minio_max_retries(self) -> int:
        """Get MinIO max retries"""
        return self.config["minio"].get("max_retries", 5)

    def get_minio_retry_delay(self) -> int:
        """Get MinIO retry delay"""
        return self.config["minio"].get("retry_delay", 2)

    # ========================================================================
    # Convenience getters for model configuration
    # ========================================================================

    def get_model_type(self) -> str:
        """Get model type (vits, vitb, vitl, vitg)"""
        return self.config["depth_model"]["model_type"]

    def get_model_path(self) -> str:
        """Get model path"""
        return self.config["depth_model"]["model_path"]

    def get_model_input_size(self) -> int:
        """Get model input size"""
        return self.config["depth_model"].get("input_size", 518)

    def get_model_device(self) -> str:
        """Get model device (auto, cuda, mps, cpu)"""
        return self.config["depth_model"].get("device", "auto")

    def get_model_config(self) -> dict:
        """Get model architecture configuration"""
        model_type = self.get_model_type()
        return self.config["depth_model"]["model_configs"].get(model_type, {})

    # ========================================================================
    # Convenience getters for processing configuration
    # ========================================================================

    def get_depth_min_cm(self) -> float:
        """Get minimum depth in cm"""
        return self.config["processing"].get("depth_min_cm", 1.0)

    def get_depth_max_cm(self) -> float:
        """Get maximum depth in cm"""
        return self.config["processing"].get("depth_max_cm", 15.0)

    def get_confidence_min(self) -> float:
        """Get minimum confidence"""
        return self.config["processing"].get("confidence_min", 0.75)

    def get_confidence_max(self) -> float:
        """Get maximum confidence"""
        return self.config["processing"].get("confidence_max", 0.98)

    def get_use_bev_image(self) -> bool:
        """Check if BEV image should be used"""
        return self.config["processing"].get("use_bev_image", True)

    def get_fallback_to_regular_image(self) -> bool:
        # TODO generate bev_image from original_image + homography
        """Check if fallback to regular image is enabled"""
        return self.config["processing"].get("fallback_to_regular_image", True)

    # ========================================================================
    # Convenience getters for logging configuration
    # ========================================================================

    def get_logging_level(self) -> str:
        """Get logging level"""
        return self.config.get("logging", {}).get("level", "INFO")

    def get_show_progress(self) -> bool:
        """Check if progress should be shown"""
        return self.config.get("logging", {}).get("show_progress", True)

    # ========================================================================
    # Get entire config sections
    # ========================================================================

    def get_kafka_config(self) -> dict:
        """Get entire Kafka configuration"""
        return self.config["kafka"]

    def get_minio_config(self) -> dict:
        """Get entire MinIO configuration"""
        return self.config["minio"]

    def get_depth_model_config(self) -> dict:
        """Get entire depth model configuration"""
        return self.config["depth_model"]

    def get_processing_config(self) -> dict:
        """Get entire processing configuration"""
        return self.config["processing"]
