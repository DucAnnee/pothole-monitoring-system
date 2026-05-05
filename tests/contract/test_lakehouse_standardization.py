from pathlib import Path


EXPECTED_NAMESPACES = ["bronze", "silver", "gold", "ml"]

EXPECTED_BRONZE_TABLES = [
    "raw_detection_events",
    "surface_area_events",
    "depth_estimation_events",
    "severity_score_events",
    "pipeline_latency_events",
    "device_telemetry_events",
    "model_deployment_events",
    "review_events",
]

EXPECTED_SILVER_TABLES = [
    "detections",
    "observations",
    "defect_evidence",
    "devices",
    "vehicles",
    "models",
    "calibrations",
    "road_segments",
    "admin_areas",
    "quality_flags",
]

EXPECTED_GOLD_TABLES = [
    "current_road_defects",
    "defect_observation_history",
    "dashboard_summary_daily",
    "district_severity_daily",
    "model_quality_metrics",
    "device_health_latest",
    "pipeline_latency_summary",
]

EXPECTED_ML_TABLES = [
    "training_dataset_items",
    "dataset_versions",
    "annotation_versions",
    "model_validation_runs",
    "model_lineage",
]

CURRENT_TOPICS = [
    "pothole.raw.events.v2",
    "pothole.surface.area.v2",
    "pothole.depth.v1",
    "pothole.severity.score.v1",
]


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def assert_tables(sql: str, namespace: str, tables: list[str]) -> None:
    lowered = sql.lower()
    for table in tables:
      assert f"create table if not exists iceberg.{namespace}.{table}" in lowered


def test_medallion_iceberg_ddl_declares_standard_namespaces_and_tables(repo_root: Path):
    lakehouse = repo_root / "lakehouse" / "iceberg"
    namespace_sql = read(lakehouse / "001_medallion_namespaces.sql").lower()
    for namespace in EXPECTED_NAMESPACES:
        assert f"create schema if not exists iceberg.{namespace}" in namespace_sql

    assert_tables(read(lakehouse / "010_bronze_tables.sql"), "bronze", EXPECTED_BRONZE_TABLES)
    assert_tables(read(lakehouse / "020_silver_tables.sql"), "silver", EXPECTED_SILVER_TABLES)
    assert_tables(read(lakehouse / "030_gold_tables.sql"), "gold", EXPECTED_GOLD_TABLES)
    assert_tables(read(lakehouse / "040_ml_tables.sql"), "ml", EXPECTED_ML_TABLES)


def test_postgis_serving_schema_is_projection_first(repo_root: Path):
    sql = read(repo_root / "lakehouse" / "postgis" / "001_serving_schema.sql").lower()
    for table in [
        "serving.current_road_defects",
        "serving.defect_evidence_index",
        "serving.review_tasks",
        "serving.annotations",
        "serving.device_registry",
        "serving.audit_log",
    ]:
        assert f"create table if not exists {table}" in sql

    for column in [
        "defect_id",
        "defect_type",
        "status",
        "severity_score",
        "severity_level",
        "confidence",
        "quality_flags",
        "geometry geometry(point, 4326)",
        "road_segment_id",
        "district",
        "ward",
        "first_seen_at",
        "last_seen_at",
        "observation_count",
        "latest_raw_image_object_key",
        "latest_bev_object_key",
    ]:
        assert column in sql

    assert "using gist (geometry)" in sql
    assert "current_road_defects_projection_inbox" in sql
    assert "st_setsrid(st_makepoint" in sql


def test_flink_sql_pins_current_topics_and_medallion_flow(repo_root: Path):
    flink_dir = repo_root / "lakehouse" / "flink" / "sql"
    kafka_to_bronze = read(flink_dir / "010_kafka_to_bronze.sql").lower()
    for topic in CURRENT_TOPICS:
        assert topic in kafka_to_bronze
    for bronze_table in [
        "bronze.raw_detection_events",
        "bronze.surface_area_events",
        "bronze.depth_estimation_events",
        "bronze.severity_score_events",
    ]:
        assert f"insert into {bronze_table}" in kafka_to_bronze

    silver = read(flink_dir / "020_silver_materialization.sql").lower()
    assert "insert into silver.detections" in silver
    assert "insert into silver.observations" in silver
    assert "insert into silver.defect_evidence" in silver

    gold = read(flink_dir / "030_gold_materialization.sql").lower()
    assert "insert into gold.current_road_defects" in gold
    assert "insert into gold.defect_observation_history" in gold

    projection = read(flink_dir / "040_gold_to_postgis_projection.sql").lower()
    assert "jdbc:postgresql://postgis:5432/postgis_serving" in projection
    assert "serving.current_road_defects_projection_inbox" in projection
    assert "from gold.current_road_defects" in projection


def test_flink_silver_declares_quality_flag_semantics(repo_root: Path):
    flink_dir = repo_root / "lakehouse" / "flink" / "sql"
    silver = read(flink_dir / "020_silver_materialization.sql").lower()
    gold = read(flink_dir / "030_gold_materialization.sql").lower()

    for flag in [
        "gps_accuracy_missing",
        "model_lineage_missing",
        "calibration_lineage_missing",
        "bev_missing",
        "depth_missing",
        "severity_missing",
    ]:
        assert flag in silver

    assert "max(o.quality_flags_json) as quality_flags_json" in gold
    assert "'[]' as quality_flags_json" not in silver


def test_flink_kafka_sources_alias_active_avro_fields(repo_root: Path):
    sql = read(repo_root / "lakehouse" / "flink" / "sql" / "010_kafka_to_bronze.sql").lower()
    compact_sql = " ".join(sql.split())

    assert "`timestamp` as event_time" in sql
    assert "gps_accuracy as gps_accuracy_m" in sql
    assert "cast(null as string) as device_id" in sql
    assert "bev_mask as bev_mask_json" in sql
    assert "cast(original_mask as string) as original_mask_json" in sql
    assert "cast(severity_score as double) as severity_score" in sql
    assert "select *" not in sql
    raw_source = sql.split("create temporary table kafka_raw_events", 1)[1].split(") with", 1)[0]
    assert "`timestamp` timestamp(6)" in raw_source
    assert "timestamp timestamp(6)" not in raw_source.replace("`timestamp`", "")
    assert "original_mask array<array<double>>" in raw_source
    assert "original_mask_json string" not in raw_source
    assert "device_id string" not in raw_source
    assert "event_time timestamp" not in raw_source
    surface_source = sql.split("create temporary table kafka_surface_area_events", 1)[1].split(") with", 1)[0]
    assert "bev_mask string" in surface_source
    assert "bev_mask_json string" not in surface_source
    severity_source = sql.split("create temporary table kafka_severity_events", 1)[1].split(") with", 1)[0]
    assert "severity_score int" in severity_source
    assert (
        "insert into bronze.raw_detection_events ( event_id, vehicle_id, device_id, event_time, gps_lat, "
        "gps_lon, gps_accuracy_m, raw_image_object_key, original_mask_json, detection_confidence, "
        "kafka_topic, kafka_partition, kafka_offset, ingested_at, payload_json ) select"
    ) in compact_sql
    assert (
        "insert into bronze.surface_area_events ( event_id, raw_image_object_key, bev_object_key, "
        "bev_mask_json, surface_area_cm2, confidence, processed_at, kafka_topic, kafka_partition, "
        "kafka_offset, ingested_at, payload_json ) select"
    ) in compact_sql
    assert (
        "insert into bronze.depth_estimation_events ( event_id, depth_cm, confidence, surface_area_cm2, "
        "processed_at, kafka_topic, kafka_partition, kafka_offset, ingested_at, payload_json ) select"
    ) in compact_sql
    assert (
        "insert into bronze.severity_score_events ( event_id, depth_cm, surface_area_cm2, severity_score, "
        "severity_level, calculated_at, kafka_topic, kafka_partition, kafka_offset, ingested_at, payload_json ) select"
    ) in compact_sql


def test_compose_declares_streamhouse_and_serving_services(repo_root: Path):
    compose = read(repo_root / "docker-compose.yml").lower()
    for service in [
        "postgis:",
        "flink-jobmanager:",
        "flink-taskmanager:",
    ]:
        assert service in compose
    assert "postgis/postgis" in compose
    assert "flink:1.19" in compose
    assert "./lakehouse/postgis/001_serving_schema.sql" in compose
    assert "./lakehouse/flink/sql:/opt/pothole-lakehouse/sql:ro" in compose


def test_lakehouse_bootstrap_script_submits_standard_jobs(repo_root: Path):
    script = read(repo_root / "scripts" / "start-lakehouse-jobs.ps1").lower()

    for service in [
        "flink-jobmanager",
        "flink-taskmanager",
        "trino",
        "postgis-serving",
        "polaris",
        "minio",
    ]:
        assert service in script

    for ddl_file in [
        "001_medallion_namespaces.sql",
        "010_bronze_tables.sql",
        "020_silver_tables.sql",
        "030_gold_tables.sql",
        "040_ml_tables.sql",
    ]:
        assert ddl_file in script

    previous_index = -1
    for flink_job in [
        "010_kafka_to_bronze.sql",
        "020_silver_materialization.sql",
        "030_gold_materialization.sql",
        "040_gold_to_postgis_projection.sql",
    ]:
        current_index = script.index(flink_job)
        assert current_index > previous_index
        previous_index = current_index

    assert "docker exec -d flink-jobmanager" in script
    for expected_bootstrap_guard in [
        "allowduplicatejobs",
        "jobs/overview",
        "polaris-setup",
        "wait-containerhealthy",
        "wait-containercompleted",
        "wait-flinkjobcountincrease",
        "use catalog lakehouse",
        "/tmp/pothole-lakehouse-",
        'if ($filename -ne "010_kafka_to_bronze.sql")',
    ]:
        assert expected_bootstrap_guard in script
