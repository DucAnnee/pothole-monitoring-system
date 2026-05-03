import textwrap

import pytest


@pytest.mark.unit
def test_edge_config_does_not_require_video_key(tmp_path, repo_root, load_module):
    config_path = tmp_path / "edge.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            model_type: yolo
            models:
              yolo:
                weights_path: models/yolo.pt
                confidence_threshold: 0.2
            detection_region:
              trapezoid_coords: [[0, 0], [1, 0], [1, 1], [0, 1]]
            bev_calibration:
              rectangle_coords: [[0, 10], [10, 10], [10, 0], [0, 0]]
            camera:
              calibration_file: .conf/camera_calibration.json
            output:
              segmentation_dir: segmentation/results
              area_estimation_dir: surface_area/results
              segmentation_dir_rfdetr: segmentation/results_rfdetr
              area_estimation_dir_rfdetr: surface_area/results_rfdetr
            processing:
              frame_interval: 1
              enable_display: false
            kafka: {}
            minio:
              endpoint: localhost:9000
              access_key: minioadmin
              secret_key: minioadmin
              bucket: warehouse
            gps:
              lat_min: 10
              lat_max: 11
              lon_min: 106
              lon_max: 107
            """
        ),
        encoding="utf-8",
    )
    module = load_module(
        "edge_config_loader_unit",
        repo_root / "edge" / "config_loader.py",
        repo_root / "edge",
    )

    config = module.ConfigLoader(str(config_path))

    assert config.get_video_path() is None
    assert config.get_kafka_topic() == "pothole.raw.events.v2"
    assert config.get_display_enabled() is False


@pytest.mark.unit
def test_edge_config_env_default_substitution(repo_root, load_module):
    module = load_module(
        "edge_config_loader_substitution",
        repo_root / "edge" / "config_loader.py",
        repo_root / "edge",
    )
    loader = object.__new__(module.ConfigLoader)

    assert loader._substitute_env_vars("${MISSING_TEST_VAR:fallback}") == "fallback"
