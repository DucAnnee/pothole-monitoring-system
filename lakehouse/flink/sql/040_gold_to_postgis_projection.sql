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

CREATE TEMPORARY TABLE serving_current_road_defects_jdbc (
  defect_id STRING,
  defect_type STRING,
  status STRING,
  severity_score DOUBLE,
  severity_level STRING,
  confidence DOUBLE,
  quality_flags STRING,
  longitude DOUBLE,
  latitude DOUBLE,
  road_segment_id STRING,
  district STRING,
  ward STRING,
  first_seen_at TIMESTAMP(3),
  last_seen_at TIMESTAMP(3),
  observation_count INT,
  latest_raw_image_object_key STRING,
  latest_bev_object_key STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (defect_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgis:5432/postgis_serving',
  'table-name' = 'serving.current_road_defects_projection_inbox',
  'username' = 'serving',
  'password' = 'servingpassword'
);

INSERT INTO serving_current_road_defects_jdbc
SELECT
  g.defect_id,
  g.defect_type,
  g.status,
  g.severity_score,
  g.severity_level,
  COALESCE(g.confidence, d.detection_confidence) AS confidence,
  g.quality_flags_json,
  d.gps_lon AS longitude,
  d.gps_lat AS latitude,
  COALESCE(ge.road_segment_id, g.road_segment_id) AS road_segment_id,
  COALESCE(ge.district, g.district) AS district,
  COALESCE(ge.ward, g.ward) AS ward,
  g.first_seen_at,
  g.last_seen_at,
  g.observation_count,
  COALESCE(g.latest_raw_image_object_key, d.raw_image_object_key) AS latest_raw_image_object_key,
  COALESCE(g.latest_bev_object_key, e.bev_object_key) AS latest_bev_object_key,
  g.updated_at
FROM gold.current_road_defects /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */ g
JOIN silver.detections /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */ d
  ON g.defect_id = CONCAT('defect-', d.event_id)
LEFT JOIN silver.defect_evidence /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */ e
  ON g.defect_id = CONCAT('defect-', e.event_id)
LEFT JOIN bronze.geo_enrichment_events /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */ ge
  ON ge.event_id = d.event_id;
