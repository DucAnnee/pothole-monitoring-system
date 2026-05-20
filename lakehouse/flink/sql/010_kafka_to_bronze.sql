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

CREATE TEMPORARY TABLE kafka_raw_events (
  event_id STRING,
  vehicle_id STRING,
  `timestamp` TIMESTAMP(3),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  gps_accuracy DOUBLE,
  raw_image_object_key STRING,
  original_mask ARRAY<ARRAY<DOUBLE>>,
  detection_confidence DOUBLE,
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.raw.events.v2',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-raw-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_surface_area_events (
  event_id STRING,
  raw_image_object_key STRING,
  bev_object_key STRING,
  bev_mask STRING,
  surface_area_cm2 DOUBLE,
  confidence DOUBLE,
  processed_at TIMESTAMP(3),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.surface.area.v2',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-surface-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_depth_events (
  event_id STRING,
  depth_cm DOUBLE,
  confidence DOUBLE,
  surface_area_cm2 DOUBLE,
  processed_at TIMESTAMP(3),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.depth.v1',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-depth-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_severity_events (
  event_id STRING,
  depth_cm DOUBLE,
  surface_area_cm2 DOUBLE,
  severity_score INT,
  severity_level STRING,
  calculated_at TIMESTAMP(3),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.severity.score.v1',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-severity-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_geo_events (
  event_id STRING,
  city STRING,
  district STRING,
  ward STRING,
  road_segment_id STRING,
  geocoded_at TIMESTAMP(3),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.geo.enriched.v1',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-geo-v2',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

INSERT INTO bronze.raw_detection_events (
  event_id, vehicle_id, device_id, event_time, gps_lat, gps_lon,
  gps_accuracy_m, raw_image_object_key, original_mask_json,
  detection_confidence, kafka_topic, kafka_partition, kafka_offset,
  ingested_at, payload_json
)
SELECT
  event_id, vehicle_id, CAST(NULL AS STRING) AS device_id,
  `timestamp` AS event_time, gps_lat, gps_lon, gps_accuracy AS gps_accuracy_m,
  raw_image_object_key, CAST(original_mask AS STRING) AS original_mask_json,
  detection_confidence, kafka_topic, kafka_partition, kafka_offset,
  CURRENT_TIMESTAMP AS ingested_at, CAST(NULL AS STRING) AS payload_json
FROM kafka_raw_events;

INSERT INTO bronze.surface_area_events (
  event_id, raw_image_object_key, bev_object_key, bev_mask_json,
  surface_area_cm2, confidence, processed_at, kafka_topic, kafka_partition,
  kafka_offset, ingested_at, payload_json
)
SELECT
  event_id, raw_image_object_key, bev_object_key, bev_mask AS bev_mask_json,
  surface_area_cm2, confidence, processed_at, kafka_topic, kafka_partition,
  kafka_offset, CURRENT_TIMESTAMP AS ingested_at, CAST(NULL AS STRING) AS payload_json
FROM kafka_surface_area_events;

INSERT INTO bronze.depth_estimation_events (
  event_id, depth_cm, confidence, surface_area_cm2, processed_at,
  kafka_topic, kafka_partition, kafka_offset, ingested_at, payload_json
)
SELECT
  event_id, depth_cm, confidence, surface_area_cm2, processed_at,
  kafka_topic, kafka_partition, kafka_offset,
  CURRENT_TIMESTAMP AS ingested_at, CAST(NULL AS STRING) AS payload_json
FROM kafka_depth_events;

INSERT INTO bronze.severity_score_events (
  event_id, depth_cm, surface_area_cm2, severity_score, severity_level,
  calculated_at, kafka_topic, kafka_partition, kafka_offset, ingested_at, payload_json
)
SELECT
  event_id, depth_cm, surface_area_cm2,
  CAST(severity_score AS DOUBLE) AS severity_score, severity_level, calculated_at,
  kafka_topic, kafka_partition, kafka_offset,
  CURRENT_TIMESTAMP AS ingested_at, CAST(NULL AS STRING) AS payload_json
FROM kafka_severity_events;

INSERT INTO bronze.geo_enrichment_events (
  event_id, city, district, ward, road_segment_id, geocoded_at,
  kafka_topic, kafka_partition, kafka_offset, ingested_at, payload_json
)
SELECT
  event_id, city, district, ward, road_segment_id, geocoded_at,
  kafka_topic, kafka_partition, kafka_offset,
  CURRENT_TIMESTAMP AS ingested_at, CAST(NULL AS STRING) AS payload_json
FROM kafka_geo_events;
