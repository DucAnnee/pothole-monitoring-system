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

    def __init__(self, config_path=None):
        if config_path is None:
            config_path = os.environ.get(
                "DEPTH_SERVICE_CONFIG",
                os.environ.get(
                    "POTHOLE_CONFIG_PATH",
                    os.path.join(os.path.dirname(__file__), "config.yaml"),
                ),
            )

        self.config_path = config_path
        self.config: Dict[str, Any] = self._load_config()
        self._validate_config()

    def _load_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please create a config.yaml file or specify the correct path."
            )

        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        return self._substitute_env_vars(config)

    def _substitute_env_vars(self, config) -> Any:
        """Recursively substitute ${VAR} or ${VAR:default} placeholders."""
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            if config.startswith("${") and config.endswith("}"):
                var_name = config[2:-1]
                if ":" in var_name:
                    var_name, default = var_name.split(":", 1)
                    return os.environ.get(var_name, default)
                else:
                    return os.environ.get(var_name, config)
        return config

    def _validate_config(self):
        required_sections = ["kafka", "minio", "triton", "batching", "processing"]
        for field in required_sections:
            if field not in self.config:
                raise ValueError(f"Missing required configuration section: {field}")

        kafka_fields = ["source_topic", "output_topic", "bootstrap_servers", "schema_registry_url"]
        for field in kafka_fields:
            if field not in self.config["kafka"]:
                raise ValueError(f"Missing required Kafka configuration: {field}")

        minio_fields = ["endpoint", "access_key", "secret_key", "bucket"]
        for field in minio_fields:
            if field not in self.config["minio"]:
                raise ValueError(f"Missing required MinIO configuration: {field}")

        triton_fields = ["host", "grpc_port", "model_name", "model_version"]
        for field in triton_fields:
            if field not in self.config["triton"]:
                raise ValueError(f"Missing required Triton configuration: {field}")

    # ========================================================================
    # Kafka
    # ========================================================================

    def get_kafka_source_topic(self) -> str:
        return self.config["kafka"]["source_topic"]

    def get_kafka_output_topic(self) -> str:
        return self.config["kafka"]["output_topic"]

    def get_kafka_consumer_group_id(self) -> str:
        return self.config["kafka"]["consumer_group_id"]

    def get_kafka_bootstrap_servers(self) -> str:
        return self.config["kafka"]["bootstrap_servers"]

    def get_kafka_schema_registry_url(self) -> str:
        return self.config["kafka"]["schema_registry_url"]

    def get_kafka_max_retries(self) -> int:
        return int(self.config["kafka"].get("max_retries", 5))

    def get_kafka_retry_delay(self) -> int:
        return int(self.config["kafka"].get("retry_delay", 2))

    # ========================================================================
    # MinIO
    # ========================================================================

    def get_minio_endpoint(self) -> str:
        return self.config["minio"]["endpoint"]

    def get_minio_access_key(self) -> str:
        return self.config["minio"]["access_key"]

    def get_minio_secret_key(self) -> str:
        return self.config["minio"]["secret_key"]

    def get_minio_bucket(self) -> str:
        return self.config["minio"]["bucket"]

    def get_minio_secure(self) -> bool:
        return bool(self.config["minio"].get("secure", False))

    def get_minio_max_retries(self) -> int:
        return int(self.config["minio"].get("max_retries", 5))

    def get_minio_retry_delay(self) -> int:
        return int(self.config["minio"].get("retry_delay", 2))

    # ========================================================================
    # Triton
    # ========================================================================

    def get_triton_host(self) -> str:
        return str(self.config["triton"]["host"])

    def get_triton_grpc_port(self) -> int:
        return int(self.config["triton"]["grpc_port"])

    def get_triton_model_name(self) -> str:
        return str(self.config["triton"]["model_name"])

    def get_triton_model_version(self) -> str:
        return str(self.config["triton"]["model_version"])

    def get_triton_timeout_seconds(self) -> int:
        return int(self.config["triton"].get("timeout_seconds", 30))

    # ========================================================================
    # Batching
    # ========================================================================

    def get_batching_max_batch_size(self) -> int:
        return int(self.config["batching"].get("max_batch_size", 8))

    def get_batching_batch_timeout_ms(self) -> int:
        return int(self.config["batching"].get("batch_timeout_ms", 200))

    # ========================================================================
    # Processing
    # ========================================================================

    def get_model_input_size(self) -> int:
        return int(self.config["processing"].get("model_input_size", 518))

    def get_depth_min_cm(self) -> float:
        return float(self.config["processing"].get("depth_min_cm", 1.0))

    def get_depth_max_cm(self) -> float:
        return float(self.config["processing"].get("depth_max_cm", 50.0))

    def get_confidence_min(self) -> float:
        return float(self.config["processing"].get("confidence_min", 0.75))

    def get_confidence_max(self) -> float:
        return float(self.config["processing"].get("confidence_max", 0.98))

    # ========================================================================
    # Logging
    # ========================================================================

    def get_logging_level(self) -> str:
        return str(self.config.get("logging", {}).get("level", "INFO"))

    def get_show_progress(self) -> bool:
        return bool(self.config.get("logging", {}).get("show_progress", True))

    # ========================================================================
    # Entire sections
    # ========================================================================

    def get_kafka_config(self) -> dict:
        return self.config["kafka"]

    def get_minio_config(self) -> dict:
        return self.config["minio"]

    def get_triton_config(self) -> dict:
        return self.config["triton"]

    def get_batching_config(self) -> dict:
        return self.config["batching"]

    def get_processing_config(self) -> dict:
        return self.config["processing"]
