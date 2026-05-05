USE CATALOG lakehouse;

INSERT INTO gold.current_road_defects
SELECT
  o.defect_id,
  'POTHOLE' AS defect_type,
  MAX(o.status) AS status,
  MAX(o.severity_score) AS severity_score,
  MAX(o.severity_level) AS severity_level,
  MAX(d.detection_confidence) AS confidence,
  MAX(o.quality_flags_json) AS quality_flags_json,
  MAX(o.gps_lat) AS latitude,
  MAX(o.gps_lon) AS longitude,
  CONCAT('POINT (', CAST(MAX(o.gps_lon) AS STRING), ' ', CAST(MAX(o.gps_lat) AS STRING), ')') AS geometry_wkt,
  CAST(NULL AS BIGINT) AS h3_cell,
  CAST(NULL AS STRING) AS road_segment_id,
  CAST(NULL AS STRING) AS district,
  CAST(NULL AS STRING) AS ward,
  MIN(o.observed_at) AS first_seen_at,
  MAX(o.observed_at) AS last_seen_at,
  COUNT(*) AS observation_count,
  MAX(e.raw_image_object_key) AS latest_raw_image_object_key,
  MAX(e.bev_object_key) AS latest_bev_object_key,
  CURRENT_TIMESTAMP AS updated_at
FROM silver.observations o
LEFT JOIN silver.detections d ON o.event_id = d.event_id
LEFT JOIN silver.defect_evidence e ON o.evidence_id = e.evidence_id
GROUP BY o.defect_id;

INSERT INTO gold.defect_observation_history
SELECT
  defect_id,
  observation_id,
  event_id,
  observed_at,
  severity_score,
  severity_level,
  status,
  evidence_id
FROM silver.observations;
