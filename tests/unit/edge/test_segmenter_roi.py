import numpy as np
import pytest


@pytest.mark.unit
def test_pothole_in_trapezoid_filters_outside_masks(repo_root, load_module):
    module = load_module(
        "edge_segmenter_unit",
        repo_root / "edge" / "segmentation" / "pothole_segmenter.py",
        repo_root / "edge",
    )

    class DummySegmenter(module.PotholeSegmenter):
        def load_model(self):
            return object()

        def segment(self, frame_rgb):
            return []

    segmenter = DummySegmenter(
        model_path=None,
        trapezoid_coords=np.array(
            [[0.25, 0.25], [0.75, 0.25], [0.75, 0.75], [0.25, 0.75]],
            dtype=np.float32,
        ),
    )

    inside = np.array([[30, 30], [70, 30], [70, 70], [30, 70]], dtype=np.float32)
    outside = np.array([[10, 10], [30, 10], [30, 30], [10, 30]], dtype=np.float32)

    assert segmenter.pothole_in_trapezoid(inside, (100, 100, 3))
    assert not segmenter.pothole_in_trapezoid(outside, (100, 100, 3))
