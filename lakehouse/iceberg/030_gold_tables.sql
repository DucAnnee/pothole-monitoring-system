CREATE TABLE IF NOT EXISTS iceberg.gold.current_road_defects (
  defect_id VARCHAR,
  defect_type VARCHAR,
  status VARCHAR,
  severity_score DOUBLE,
  severity_level VARCHAR,
  confidence DOUBLE,
  quality_flags_json VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE,
  geometry_wkt VARCHAR,
  h3_cell BIGINT,
  road_segment_id VARCHAR,
  district VARCHAR,
  ward VARCHAR,
  first_seen_at TIMESTAMP(6),
  last_seen_at TIMESTAMP(6),
  observation_count INTEGER,
  latest_raw_image_object_key VARCHAR,
  latest_bev_object_key VARCHAR,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['month(first_seen_at)']);

CREATE TABLE IF NOT EXISTS iceberg.gold.defect_observation_history (
  defect_id VARCHAR,
  observation_id VARCHAR,
  event_id VARCHAR,
  observed_at TIMESTAMP(6),
  severity_score DOUBLE,
  severity_level VARCHAR,
  status VARCHAR,
  evidence_id VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(observed_at)']);

CREATE TABLE IF NOT EXISTS iceberg.gold.dashboard_summary_daily (
  summary_date DATE,
  active_defect_count BIGINT,
  new_defect_count BIGINT,
  fixed_defect_count BIGINT,
  average_severity DOUBLE,
  critical_defect_count BIGINT,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['summary_date']);

CREATE TABLE IF NOT EXISTS iceberg.gold.district_severity_daily (
  summary_date DATE,
  district VARCHAR,
  ward VARCHAR,
  defect_count BIGINT,
  average_severity DOUBLE,
  max_severity DOUBLE,
  critical_defect_count BIGINT,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['summary_date']);

CREATE TABLE IF NOT EXISTS iceberg.gold.model_quality_metrics (
  model_id VARCHAR,
  model_version VARCHAR,
  metric_date DATE,
  metric_name VARCHAR,
  metric_value DOUBLE,
  sample_count BIGINT,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['metric_date']);

CREATE TABLE IF NOT EXISTS iceberg.gold.device_health_latest (
  device_id VARCHAR,
  vehicle_id VARCHAR,
  last_seen_at TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  speed_kph DOUBLE,
  battery_percent DOUBLE,
  status VARCHAR,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.gold.pipeline_latency_summary (
  summary_date DATE,
  stage VARCHAR,
  service_name VARCHAR,
  avg_latency_ms DOUBLE,
  p95_latency_ms DOUBLE,
  p99_latency_ms DOUBLE,
  event_count BIGINT,
  updated_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['summary_date']);
