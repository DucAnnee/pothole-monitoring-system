"""Canonical event samples used by unit, contract, and E2E assertions."""

RAW_EVENT_V2 = {
    "event_id": "evt-test-001",
    "vehicle_id": "vehicle-test",
    "timestamp": 1767225600000,
    "gps_lat": 10.775,
    "gps_lon": 106.701,
    "gps_accuracy": 7.5,
    "raw_image_object_key": "s3://warehouse/raw_images/evt-test-001.jpg",
    "original_mask": [[10.0, 10.0], [40.0, 10.0], [40.0, 35.0], [10.0, 35.0]],
    "detection_confidence": 0.91,
}

SURFACE_AREA_V2 = {
    "event_id": RAW_EVENT_V2["event_id"],
    "raw_image_object_key": RAW_EVENT_V2["raw_image_object_key"],
    "bev_object_key": "bev_images/evt-test-001.jpg",
    "bev_mask": "[[12, 12], [42, 12], [42, 37], [12, 37]]",
    "surface_area_cm2": 1234.5,
    "confidence": 1.0,
    "processed_at": 1767225601000,
}

DEPTH_V1 = {
    "event_id": RAW_EVENT_V2["event_id"],
    "depth_cm": 8.25,
    "confidence": 0.88,
    "surface_area_cm2": SURFACE_AREA_V2["surface_area_cm2"],
    "processed_at": 1767225602000,
}

SEVERITY_SCORE_V1 = {
    "event_id": RAW_EVENT_V2["event_id"],
    "depth_cm": DEPTH_V1["depth_cm"],
    "surface_area_cm2": DEPTH_V1["surface_area_cm2"],
    "severity_score": 4,
    "severity_level": "MODERATE",
    "calculated_at": 1767225603000,
}
