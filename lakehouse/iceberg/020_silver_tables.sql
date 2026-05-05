CREATE TABLE IF NOT EXISTS iceberg.silver.detections (
  detection_id VARCHAR,
  event_id VARCHAR,
  device_id VARCHAR,
  vehicle_id VARCHAR,
  event_time TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  gps_accuracy_m DOUBLE,
  raw_image_object_key VARCHAR,
  original_mask_json VARCHAR,
  detection_confidence DOUBLE,
  model_id VARCHAR,
  calibration_id VARCHAR,
  h3_cell BIGINT,
  quality_flags_json VARCHAR,
  created_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(event_time)']);

CREATE TABLE IF NOT EXISTS iceberg.silver.observations (
  observation_id VARCHAR,
  defect_id VARCHAR,
  event_id VARCHAR,
  observed_at TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  depth_cm DOUBLE,
  surface_area_cm2 DOUBLE,
  severity_score DOUBLE,
  severity_level VARCHAR,
  status VARCHAR,
  evidence_id VARCHAR,
  quality_flags_json VARCHAR,
  created_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(observed_at)']);

CREATE TABLE IF NOT EXISTS iceberg.silver.defect_evidence (
  evidence_id VARCHAR,
  event_id VARCHAR,
  raw_image_object_key VARCHAR,
  bev_object_key VARCHAR,
  original_mask_json VARCHAR,
  bev_mask_json VARCHAR,
  raw_image_sha256 VARCHAR,
  bev_image_sha256 VARCHAR,
  model_id VARCHAR,
  calibration_id VARCHAR,
  quality_flags_json VARCHAR,
  created_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(created_at)']);

CREATE TABLE IF NOT EXISTS iceberg.silver.devices (
  device_id VARCHAR,
  vehicle_id VARCHAR,
  device_type VARCHAR,
  firmware_version VARCHAR,
  first_seen_at TIMESTAMP(6),
  last_seen_at TIMESTAMP(6),
  status VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.silver.vehicles (
  vehicle_id VARCHAR,
  fleet_id VARCHAR,
  route_id VARCHAR,
  plate_number VARCHAR,
  first_seen_at TIMESTAMP(6),
  last_seen_at TIMESTAMP(6),
  status VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.silver.models (
  model_id VARCHAR,
  model_name VARCHAR,
  model_version VARCHAR,
  task_type VARCHAR,
  artifact_uri VARCHAR,
  deployed_at TIMESTAMP(6),
  retired_at TIMESTAMP(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.silver.calibrations (
  calibration_id VARCHAR,
  device_id VARCHAR,
  calibration_type VARCHAR,
  parameters_json VARCHAR,
  effective_from TIMESTAMP(6),
  effective_to TIMESTAMP(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.silver.road_segments (
  road_segment_id VARCHAR,
  road_name VARCHAR,
  district VARCHAR,
  ward VARCHAR,
  geometry_wkt VARCHAR,
  source VARCHAR,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.silver.admin_areas (
  admin_area_id VARCHAR,
  area_type VARCHAR,
  name VARCHAR,
  parent_admin_area_id VARCHAR,
  geometry_wkt VARCHAR,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.silver.quality_flags (
  flag_id VARCHAR,
  event_id VARCHAR,
  defect_id VARCHAR,
  flag_code VARCHAR,
  severity VARCHAR,
  message VARCHAR,
  created_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(created_at)']);
