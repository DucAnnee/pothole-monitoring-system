"""Unit tests for BEVProcessor."""

import json
from unittest.mock import MagicMock

import numpy as np

from bev_processor import BEVProcessor


def make_processor(estimator_result=None):
    minio = MagicMock()
    minio.get_object.return_value.__enter__ = lambda s: s
    minio.get_object.return_value.__exit__ = MagicMock(return_value=False)

    import cv2

    img = np.ones((100, 100, 3), dtype=np.uint8) * 255
    _, buf = cv2.imencode(".jpg", img)
    mock_resp = MagicMock()
    mock_resp.read.return_value = buf.tobytes()
    mock_resp.close = MagicMock()
    mock_resp.release_conn = MagicMock()
    minio.get_object.return_value = mock_resp

    estimator = MagicMock()
    bev_img = np.ones((100, 100, 3), dtype=np.uint8) * 128
    bev_mask = np.array([[10, 10], [20, 10], [20, 20], [10, 20]], dtype=np.float32)
    if estimator_result is None:
        estimator.compute_pothole_area.return_value = (500.0, bev_img, np.eye(3), bev_mask)
    else:
        estimator.compute_pothole_area.return_value = estimator_result

    return BEVProcessor(minio, "warehouse", estimator), minio, estimator


def test_process_success_returns_bev_key_and_area():
    proc, minio, _ = make_processor()
    bev_key, bev_mask_json, area, confidence = proc.process(
        "evt-1", "raw_images/evt-1.jpg", [[10.0, 10.0], [20.0, 10.0]]
    )
    assert bev_key == "bev_images/evt-1.jpg"
    assert area == 500.0
    assert confidence == 1.0
    assert json.loads(bev_mask_json) is not None
    minio.put_object.assert_called_once()


def test_process_download_failure_returns_zeros():
    proc, minio, _ = make_processor()
    minio.get_object.side_effect = Exception("connection refused")
    bev_key, bev_mask_json, area, confidence = proc.process(
        "evt-2", "raw_images/evt-2.jpg", [[10.0, 10.0]]
    )
    assert bev_key is None
    assert bev_mask_json is None
    assert area == 0.0
    assert confidence == 0.0


def test_process_estimator_failure_returns_zeros():
    proc, minio, estimator = make_processor()
    estimator.compute_pothole_area.side_effect = Exception("homography failed")
    bev_key, bev_mask_json, area, confidence = proc.process(
        "evt-3", "raw_images/evt-3.jpg", [[10.0, 10.0]]
    )
    assert bev_key is None
    assert bev_mask_json is None
    assert area == 0.0
    assert confidence == 0.0
