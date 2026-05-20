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

INSERT INTO gold.current_road_defects
SELECT
  o.defect_id,
  'POTHOLE' AS defect_type,
  o.status,
  o.severity_score,
  o.severity_level,
  CAST(NULL AS DOUBLE) AS confidence,
  o.quality_flags_json AS quality_flags_json,
  o.gps_lat AS latitude,
  o.gps_lon AS longitude,
  CONCAT('POINT (', CAST(o.gps_lon AS STRING), ' ', CAST(o.gps_lat AS STRING), ')') AS geometry_wkt,
  CAST(NULL AS BIGINT) AS h3_cell,
  CAST(NULL AS STRING) AS road_segment_id,
  CAST(NULL AS STRING) AS district,
  CAST(NULL AS STRING) AS ward,
  o.observed_at AS first_seen_at,
  o.observed_at AS last_seen_at,
  CAST(1 AS INT) AS observation_count,
  CAST(NULL AS STRING) AS latest_raw_image_object_key,
  CAST(NULL AS STRING) AS latest_bev_object_key,
  CURRENT_TIMESTAMP AS updated_at
FROM silver.observations /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */ o;

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
FROM silver.observations /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */;
