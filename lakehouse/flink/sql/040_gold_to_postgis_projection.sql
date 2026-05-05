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
  first_seen_at TIMESTAMP(6),
  last_seen_at TIMESTAMP(6),
  observation_count INT,
  latest_raw_image_object_key STRING,
  latest_bev_object_key STRING,
  updated_at TIMESTAMP(6),
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
  defect_id,
  defect_type,
  status,
  severity_score,
  severity_level,
  confidence,
  quality_flags_json,
  longitude,
  latitude,
  road_segment_id,
  district,
  ward,
  first_seen_at,
  last_seen_at,
  observation_count,
  latest_raw_image_object_key,
  latest_bev_object_key,
  updated_at
FROM gold.current_road_defects;
