SET 'execution.checkpointing.interval' = '30000';
SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';

USE CATALOG default_catalog;
DROP CATALOG IF EXISTS lakehouse;

CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://polaris:8181/api/catalog/',
  'warehouse' = 'warehouse',
  'credential' = 'root:s3cr3t',
  'oauth2-server-uri' = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
  'scope' = 'PRINCIPAL_ROLE:ALL',
  'header.Polaris-Realm' = 'POLARIS',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin'
);

USE CATALOG lakehouse;

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
FROM bronze.raw_detection_events /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */;

INSERT INTO silver.defect_evidence
SELECT
  event_id AS evidence_id,
  event_id,
  raw_image_object_key,
  bev_object_key,
  CAST(NULL AS STRING) AS original_mask_json,
  bev_mask_json,
  CAST(NULL AS STRING) AS raw_image_sha256,
  CAST(NULL AS STRING) AS bev_image_sha256,
  CAST(NULL AS STRING) AS model_id,
  CAST(NULL AS STRING) AS calibration_id,
  CASE
    WHEN bev_object_key IS NULL OR bev_object_key = '' THEN '["BEV_MISSING","MODEL_LINEAGE_MISSING","CALIBRATION_LINEAGE_MISSING"]'
    ELSE '["MODEL_LINEAGE_MISSING","CALIBRATION_LINEAGE_MISSING"]'
  END AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.surface_area_events /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */;

INSERT INTO silver.observations
SELECT
  event_id AS observation_id,
  CONCAT('defect-', event_id) AS defect_id,
  event_id,
  calculated_at AS observed_at,
  CAST(NULL AS DOUBLE) AS gps_lat,
  CAST(NULL AS DOUBLE) AS gps_lon,
  depth_cm,
  surface_area_cm2,
  severity_score,
  severity_level,
  'reported' AS status,
  event_id AS evidence_id,
  CASE
    WHEN depth_cm IS NULL AND severity_score IS NULL THEN '["DEPTH_MISSING","SEVERITY_MISSING"]'
    WHEN depth_cm IS NULL THEN '["DEPTH_MISSING"]'
    WHEN severity_score IS NULL THEN '["SEVERITY_MISSING"]'
    ELSE '[]'
  END AS quality_flags_json,
  CURRENT_TIMESTAMP AS created_at
FROM bronze.severity_score_events /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */;
