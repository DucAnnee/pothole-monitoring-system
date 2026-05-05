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
  device_id STRING,
  event_time TIMESTAMP(6),
  gps_lat DOUBLE,
  gps_lon DOUBLE,
  gps_accuracy_m DOUBLE,
  raw_image_object_key STRING,
  original_mask_json STRING,
  detection_confidence DOUBLE,
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.raw.events.v2',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-raw-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_surface_area_events (
  event_id STRING,
  raw_image_object_key STRING,
  bev_object_key STRING,
  bev_mask_json STRING,
  surface_area_cm2 DOUBLE,
  confidence DOUBLE,
  processed_at TIMESTAMP(6),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.surface.area.v2',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-surface-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_depth_events (
  event_id STRING,
  depth_cm DOUBLE,
  confidence DOUBLE,
  surface_area_cm2 DOUBLE,
  processed_at TIMESTAMP(6),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.depth.v1',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-depth-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_severity_events (
  event_id STRING,
  depth_cm DOUBLE,
  surface_area_cm2 DOUBLE,
  severity_score DOUBLE,
  severity_level STRING,
  calculated_at TIMESTAMP(6),
  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'pothole.severity.score.v1',
  'properties.bootstrap.servers' = 'kafka-1:9094,kafka-2:9094,kafka-3:9094',
  'properties.group.id' = 'lakehouse-bronze-severity-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

INSERT INTO bronze.raw_detection_events
SELECT *, CURRENT_TIMESTAMP, CAST(NULL AS STRING) FROM kafka_raw_events;

INSERT INTO bronze.surface_area_events
SELECT *, CURRENT_TIMESTAMP, CAST(NULL AS STRING) FROM kafka_surface_area_events;

INSERT INTO bronze.depth_estimation_events
SELECT *, CURRENT_TIMESTAMP, CAST(NULL AS STRING) FROM kafka_depth_events;

INSERT INTO bronze.severity_score_events
SELECT *, CURRENT_TIMESTAMP, CAST(NULL AS STRING) FROM kafka_severity_events;
