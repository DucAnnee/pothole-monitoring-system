"""Configuration loader for BEV Surface Service."""

import os
from typing import Any, Dict

import yaml


class ConfigLoader:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.environ.get(
                "BEV_SURFACE_CONFIG",
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
            raise FileNotFoundError(f"Config not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)
        return self._substitute_env_vars(config)

    def _substitute_env_vars(self, config) -> Any:
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        if isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        if isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            var = config[2:-1]
            if ":" in var:
                name, default = var.split(":", 1)
                return os.environ.get(name, default)
            return os.environ.get(var, config)
        return config

    def _validate_config(self):
        for section in ("kafka", "minio", "bev"):
            if section not in self.config:
                raise ValueError(f"Missing required config section: {section}")

    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.config["kafka"]["bootstrap_servers"]

    @property
    def schema_registry_url(self) -> str:
        return self.config["kafka"]["schema_registry_url"]

    @property
    def kafka_consumer_group_id(self) -> str:
        return self.config["kafka"]["consumer_group_id"]

    @property
    def kafka_source_topic(self) -> str:
        return self.config["kafka"]["source_topic"]

    @property
    def kafka_output_topic(self) -> str:
        return self.config["kafka"]["output_topic"]

    @property
    def minio_endpoint(self) -> str:
        return self.config["minio"]["endpoint"]

    @property
    def minio_access_key(self) -> str:
        return self.config["minio"]["access_key"]

    @property
    def minio_secret_key(self) -> str:
        return self.config["minio"]["secret_key"]

    @property
    def minio_bucket(self) -> str:
        return self.config["minio"]["bucket"]

    @property
    def minio_secure(self) -> bool:
        return self.config["minio"].get("secure", False)

    @property
    def trapezoid_coords(self) -> list:
        return self.config["bev"]["trapezoid_coords"]

    @property
    def rectangle_coords(self) -> list:
        return self.config["bev"]["rectangle_coords"]

    @property
    def calibration_file(self) -> str:
        path = self.config["bev"]["calibration_file"]
        if os.path.isabs(path) or os.path.exists(path):
            return path

        service_relative = os.path.join(os.path.dirname(__file__), path)
        if os.path.exists(service_relative):
            return service_relative

        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        repo_relative = os.path.join(repo_root, path)
        if os.path.exists(repo_relative):
            return repo_relative

        edge_relative = os.path.join(repo_root, "edge", path)
        if os.path.exists(edge_relative):
            return edge_relative

        return path
