"""
Configuration loader for Final Enrichment Service.
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

    def __init__(self, config_path: str = None):
        """
        Initialize config loader.

        Args:
            config_path: Path to YAML configuration file. 
                         If None, looks for config.yaml in same directory as this file.
        """
        if config_path is None:
            # Default to config.yaml in the same directory as this file
            config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        
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
                    value = os.environ.get(var_name, default)
                    # Try to convert to int if it looks like a number
                    try:
                        return int(value)
                    except ValueError:
                        try:
                            return float(value)
                        except ValueError:
                            return value
                else:
                    return os.environ.get(var_name, config)
        return config

    def _validate_config(self):
        """Validate required configuration fields"""
        required_fields = [
            "kafka",
            "trino",
            "redis",
            "h3",
            "aggregation",
        ]

        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required configuration field: {field}")

        # Validate Kafka config
        kafka_fields = [
            "bootstrap_servers",
            "schema_registry_url",
            "consumer_group_id",
            "raw_events_topic",
            "severity_score_topic",
        ]
        for field in kafka_fields:
            if field not in self.config["kafka"]:
                raise ValueError(f"Missing required Kafka configuration: {field}")

        # Validate Trino config
        trino_fields = ["host", "port", "user", "catalog", "schema"]
        for field in trino_fields:
            if field not in self.config["trino"]:
                raise ValueError(f"Missing required Trino configuration: {field}")

        # Validate Redis config
        redis_fields = ["host", "port"]
        for field in redis_fields:
            if field not in self.config["redis"]:
                raise ValueError(f"Missing required Redis configuration: {field}")

        # Validate aggregation config
        aggregation_fields = ["timeout_seconds", "cleanup_interval_seconds"]
        for field in aggregation_fields:
            if field not in self.config["aggregation"]:
                raise ValueError(f"Missing required aggregation configuration: {field}")

    # Property accessors for common configuration values
    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.config["kafka"]["bootstrap_servers"]

    @property
    def schema_registry_url(self) -> str:
        return self.config["kafka"]["schema_registry_url"]

    @property
    def kafka_group_id(self) -> str:
        return self.config["kafka"]["consumer_group_id"]

    @property
    def raw_events_topic(self) -> str:
        return self.config["kafka"]["raw_events_topic"]

    @property
    def severity_score_topic(self) -> str:
        return self.config["kafka"]["severity_score_topic"]

    @property
    def trino_host(self) -> str:
        return self.config["trino"]["host"]

    @property
    def trino_port(self) -> int:
        return self.config["trino"]["port"]

    @property
    def trino_user(self) -> str:
        return self.config["trino"]["user"]

    @property
    def trino_catalog(self) -> str:
        return self.config["trino"]["catalog"]

    @property
    def trino_schema(self) -> str:
        return self.config["trino"]["schema"]

    @property
    def redis_host(self) -> str:
        return self.config["redis"]["host"]

    @property
    def redis_port(self) -> int:
        return self.config["redis"]["port"]

    @property
    def h3_dedup_resolution(self) -> int:
        return self.config["h3"]["dedup_resolution"]

    @property
    def h3_cache_resolution(self) -> int:
        return self.config["h3"]["cache_resolution"]

    @property
    def cache_key_prefix(self) -> str:
        return self.config["cache"]["key_prefix"]

    @property
    def cache_ttl_seconds(self) -> int:
        return self.config["cache"]["ttl_seconds"]

    @property
    def osm_min_request_interval(self) -> float:
        return self.config["osm"]["min_request_interval"]

    @property
    def aggregation_timeout_seconds(self) -> int:
        return self.config["aggregation"]["timeout_seconds"]

    @property
    def cleanup_interval_seconds(self) -> int:
        return self.config["aggregation"]["cleanup_interval_seconds"]

    @property
    def log_timeout_warnings(self) -> bool:
        return self.config.get("logging", {}).get("log_timeout_warnings", True)

    @property
    def log_level(self) -> str:
        return self.config.get("logging", {}).get("level", "INFO")
