CREATE TABLE IF NOT EXISTS iceberg.ml.training_dataset_items (
  dataset_item_id VARCHAR,
  event_id VARCHAR,
  evidence_id VARCHAR,
  raw_image_object_key VARCHAR,
  bev_object_key VARCHAR,
  label_json VARCHAR,
  split_name VARCHAR,
  dataset_version_id VARCHAR,
  created_at TIMESTAMP(6)
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(created_at)']);

CREATE TABLE IF NOT EXISTS iceberg.ml.dataset_versions (
  dataset_version_id VARCHAR,
  dataset_name VARCHAR,
  version_label VARCHAR,
  item_count BIGINT,
  source_snapshot_id VARCHAR,
  created_at TIMESTAMP(6),
  metadata_json VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg.ml.annotation_versions (
  annotation_version_id VARCHAR,
  dataset_item_id VARCHAR,
  annotator_id VARCHAR,
  annotation_json VARCHAR,
  created_at TIMESTAMP(6),
  supersedes_annotation_version_id VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(created_at)']);

CREATE TABLE IF NOT EXISTS iceberg.ml.model_validation_runs (
  validation_run_id VARCHAR,
  model_id VARCHAR,
  model_version VARCHAR,
  dataset_version_id VARCHAR,
  started_at TIMESTAMP(6),
  completed_at TIMESTAMP(6),
  metrics_json VARCHAR,
  status VARCHAR
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(started_at)']);

CREATE TABLE IF NOT EXISTS iceberg.ml.model_lineage (
  model_id VARCHAR,
  model_version VARCHAR,
  parent_model_id VARCHAR,
  parent_model_version VARCHAR,
  dataset_version_id VARCHAR,
  training_run_id VARCHAR,
  artifact_uri VARCHAR,
  created_at TIMESTAMP(6)
)
WITH (format = 'PARQUET');
