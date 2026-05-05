USE CATALOG lakehouse;

-- Deterministic quality flag JSON keeps the local thesis demo reproducible;
-- missing model and calibration lineage are explicit flags until registries emit IDs.

INSERT INTO silver.detections
SELECT
  event_id AS detection_id,
  event_id,
  device_id,
  vehicle_id,
  event_time,
  gps_lat,
  gps_lon,
  gps_accuracy_m,
  raw_image_object_key,
  original_mask_json,
  detection_confidence,
  CAST(NULL AS STRING) AS model_id,
  CAST(NULL AS STRING) AS calibration_id,
  CAST(NULL AS BIGINT) AS h3_cell,
  CASE
    WHEN gps_accuracy_m IS NULL THEN '["GPS_ACCURACY_MISSING","MODEL_LINEAGE_MISSING","CALIBRATION_LINEAGE_MISSING"]'
    ELSE '["MODEL_LINEAGE_MISSING","CALIBRATION_LINEAGE_MISSING"]'
  END AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events;

INSERT INTO silver.defect_evidence
SELECT
  r.event_id AS evidence_id,
  r.event_id,
  r.raw_image_object_key,
  s.bev_object_key,
  r.original_mask_json,
  s.bev_mask_json,
  CAST(NULL AS STRING) AS raw_image_sha256,
  CAST(NULL AS STRING) AS bev_image_sha256,
  CAST(NULL AS STRING) AS model_id,
  CAST(NULL AS STRING) AS calibration_id,
  CASE
    WHEN s.bev_object_key IS NULL OR s.bev_object_key = '' THEN '["BEV_MISSING","MODEL_LINEAGE_MISSING","CALIBRATION_LINEAGE_MISSING"]'
    ELSE '["MODEL_LINEAGE_MISSING","CALIBRATION_LINEAGE_MISSING"]'
  END AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events r
LEFT JOIN bronze.surface_area_events s ON r.event_id = s.event_id;

INSERT INTO silver.observations
SELECT
  r.event_id AS observation_id,
  CONCAT('defect-', r.event_id) AS defect_id,
  r.event_id,
  r.event_time AS observed_at,
  r.gps_lat,
  r.gps_lon,
  d.depth_cm,
  COALESCE(a.surface_area_cm2, d.surface_area_cm2) AS surface_area_cm2,
  sev.severity_score,
  sev.severity_level,
  'reported' AS status,
  r.event_id AS evidence_id,
  CASE
    WHEN d.depth_cm IS NULL AND sev.severity_score IS NULL THEN '["DEPTH_MISSING","SEVERITY_MISSING"]'
    WHEN d.depth_cm IS NULL THEN '["DEPTH_MISSING"]'
    WHEN sev.severity_score IS NULL THEN '["SEVERITY_MISSING"]'
    ELSE '[]'
  END AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.raw_detection_events r
LEFT JOIN bronze.surface_area_events a ON r.event_id = a.event_id
LEFT JOIN bronze.depth_estimation_events d ON r.event_id = d.event_id
LEFT JOIN bronze.severity_score_events sev ON r.event_id = sev.event_id;
