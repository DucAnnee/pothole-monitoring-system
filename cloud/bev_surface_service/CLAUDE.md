# BEV Surface Area Service

## Overview

Consumes `pothole.raw.events.v2` -> downloads raw image from MinIO -> runs
`PotholeAreaEstimator` (homography BEV transform) -> uploads BEV image to
MinIO -> publishes `pothole.surface.area.v2`.

## Entry Point

```bash
python cloud/bev_surface_service/bev_surface_service.py
```

## Key Gotchas

- `PotholeAreaEstimator` requires `camera_calibration.json`; path is set in
  `config.yaml` under `bev.calibration_file`.
- `bev_mask` in Avro schema is `"string"` (JSON-encoded `[[x,y],...]`), not bytes.
- On estimator failure: publishes `confidence=0.0`, `surface_area_cm2=0.0`,
  empty `bev_object_key`; depth service falls back to `raw_image_object_key`.
- Manual Kafka offset commit happens after the downstream produce flushes.
