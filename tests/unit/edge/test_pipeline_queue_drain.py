from datetime import datetime, timezone
import queue

import numpy as np
import pytest


@pytest.mark.unit
def test_upload_worker_drains_queue_after_inference_stops(repo_root, load_module):
    pytest.importorskip("confluent_kafka")
    module = load_module("edge_main_queue_unit", repo_root / "edge" / "main.py", repo_root / "edge")
    data_models = load_module(
        "edge_data_models_queue_unit",
        repo_root / "edge" / "data_models.py",
        repo_root / "edge",
    )

    class DummyUploader:
        is_online = True

        def __init__(self):
            self.stats = {"processed": 0, "uploaded": 0, "stored": 0, "failed": 0}
            self.uploaded = []

        def process_local_storage(self):
            pass

        def flush(self):
            pass

        def process_detection(self, detection):
            return detection.masks

        def upload_to_cloud(self, enriched):
            self.uploaded.append(enriched)
            return True

    pipeline = object.__new__(module.EdgePipeline)
    pipeline.running = False
    pipeline.detection_queue = queue.Queue()
    pipeline.uploader = DummyUploader()
    pipeline.detection_queue.put(
        data_models.DetectionData(
            frame_id="frame_000001",
            timestamp=datetime.now(timezone.utc),
            frame=np.zeros((8, 8, 3), dtype=np.uint8),
            masks=[data_models.DetectionMask(conf=0.9, coordinates=[[1, 1], [2, 2], [3, 1]])],
        )
    )

    pipeline.uploading_worker()

    assert pipeline.detection_queue.empty()
    assert len(pipeline.uploader.uploaded) == 1
