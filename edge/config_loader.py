import yaml
import os
import numpy as np
from pathlib import Path


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
        self.config = self._load_config()
        self._validate_config()

    def _load_config(self):
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

    def _substitute_env_vars(self, config):
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

    def get_model_type(self):
        """Get selected model type"""
        return self.config["model_type"]

    def get_video_path(self):
        """Get video input path"""
        return self.config["video"]["input_path"]

    def get_model_config(self):
        """Get configuration for the selected model"""
        model_type = self.get_model_type()
        return self.config["models"][model_type]

    def get_model_path(self):
        """Get model weights path"""
        model_config = self.get_model_config()
        return model_config["weights_path"]

    def get_confidence_threshold(self):
        """Get confidence threshold for the selected model"""
        model_config = self.get_model_config()
        return model_config.get("confidence_threshold", 0.25)

    def get_trapezoid_coords(self):
        """Get trapezoid coordinates as numpy array"""
        coords = self.config["detection_region"]["trapezoid_coords"]
        return np.array(coords, dtype=np.float32)

    def get_reference_resolution(self):
        """Get reference resolution as tuple (width, height)"""
        res = self.config["detection_region"]["reference_resolution"]
        return tuple(res)

    def get_rectangle_coords(self):
        """Get BEV rectangle coordinates as numpy array"""
        coords = self.config["bev_calibration"]["rectangle_coords"]
        return np.array(coords, dtype=np.float32)

    def get_calibration_path(self):
        """Get camera calibration file path"""
        return self.config["camera"]["calibration_file"]

    def get_output_dirs(self):
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

    def get_frame_interval(self):
        """Get frame sampling interval"""
        return self.config["processing"]["frame_interval"]

    def get_display_enabled(self):
        """Check if real-time display is enabled"""
        return self.config["processing"].get("enable_display", True)

    def get_display_window_name(self):
        """Get display window name"""
        return self.config["processing"].get("display_window_name", "Pothole Detection")

    def get_api_config(self):
        """Get API configuration (if available)"""
        return self.config.get("api", None)

    def get_database_config(self):
        """Get database configuration (if available)"""
        return self.config.get("database", None)

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
        print(f"Reference Resolution: {self.get_reference_resolution()}")
        print(f"Calibration File: {self.get_calibration_path()}")

        output_dirs = self.get_output_dirs()
        print(f"Segmentation Output: {output_dirs['segmentation']}")
        print(f"Area Estimation Output: {output_dirs['area_estimation']}")
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
    # Test configuration loader
    try:
        config = load_config()
        config.print_config_summary()
        print("✓ Configuration loaded successfully!")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
