CREATE TABLE IF NOT EXISTS iceberg.bronze.raw_detection_events (
  event_id VARCHAR,
  vehicle_id VARCHAR,
  device_id VARCHAR,
  event_time TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  gps_accuracy_m DOUBLE,
  raw_image_object_key VARCHAR,
  original_mask_json VARCHAR,
  detection_confidence DOUBLE,
  kafka_topic VARCHAR,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(event_time)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.surface_area_events (
  event_id VARCHAR,
  raw_image_object_key VARCHAR,
  bev_object_key VARCHAR,
  bev_mask_json VARCHAR,
  surface_area_cm2 DOUBLE,
  confidence DOUBLE,
  processed_at TIMESTAMP(6),
  kafka_topic VARCHAR,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(processed_at)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.depth_estimation_events (
  event_id VARCHAR,
  depth_cm DOUBLE,
  confidence DOUBLE,
  surface_area_cm2 DOUBLE,
  processed_at TIMESTAMP(6),
  kafka_topic VARCHAR,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(processed_at)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.severity_score_events (
  event_id VARCHAR,
  depth_cm DOUBLE,
  surface_area_cm2 DOUBLE,
  severity_score DOUBLE,
  severity_level VARCHAR,
  calculated_at TIMESTAMP(6),
  kafka_topic VARCHAR,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(calculated_at)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.pipeline_latency_events (
  event_id VARCHAR,
  stage VARCHAR,
  service_name VARCHAR,
  latency_ms DOUBLE,
  measured_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(measured_at)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.device_telemetry_events (
  device_id VARCHAR,
  vehicle_id VARCHAR,
  telemetry_time TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  speed_kph DOUBLE,
  battery_percent DOUBLE,
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(telemetry_time)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.model_deployment_events (
  deployment_id VARCHAR,
  model_id VARCHAR,
  model_version VARCHAR,
  service_name VARCHAR,
  deployed_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(deployed_at)']);

CREATE TABLE IF NOT EXISTS iceberg.bronze.review_events (
  review_id VARCHAR,
  defect_id VARCHAR,
  event_id VARCHAR,
  reviewer_id VARCHAR,
  review_status VARCHAR,
  reviewed_at TIMESTAMP(6),
  payload_json VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(reviewed_at)']);
