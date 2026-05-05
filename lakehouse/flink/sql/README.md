# Flink Lakehouse Jobs

These SQL files define the first Streamhouse milestone:

1. Kafka topics are ingested into `iceberg.bronze.*`.
2. Bronze events are conformed into `iceberg.silver.*`.
3. Silver tables are materialized into `iceberg.gold.*`.
4. `iceberg.gold.current_road_defects` is projected into PostGIS serving tables.

The Docker Compose Flink services mount this folder at
`/opt/pothole-lakehouse/sql`.

## Local Job Bootstrap

Start the local lakehouse stack first:

```powershell
docker compose up -d
```

Then bootstrap the lakehouse DDL and streaming jobs:

```powershell
scripts/start-lakehouse-jobs.ps1
```

The bootstrap script waits for the Compose lakehouse services, applies the
Iceberg medallion DDL files through Trino, and starts the streaming Flink SQL
files in this order:

1. `010_kafka_to_bronze.sql`
2. `020_silver_materialization.sql`
3. `030_gold_materialization.sql`
4. `040_gold_to_postgis_projection.sql`

Airflow is not part of this milestone. Flink owns the continuous
transformations; a scheduler can be added later for batch operations.

## Connector Packaging

The local Docker Compose setup mounts connector/runtime JARs from
`container-conf/flink/lib/` into `/opt/flink/lib/` on both `flink-jobmanager`
and `flink-taskmanager`. Keep these mounted as individual files so the mount
does not hide Flink's built-in libraries.

Verified JAR set:

| JAR | Purpose |
|---|---|
| `iceberg-flink-runtime-1.19-1.10.1.jar` | Iceberg catalog, Flink table integration, and `S3FileIO`. |
| `iceberg-aws-bundle-1.10.1.jar` | AWS SDK dependencies used by Iceberg S3/MinIO access. |
| `flink-sql-connector-kafka-3.3.0-1.19.jar` | Kafka SQL source/sink connector. |
| `flink-avro-1.19.3.jar` | Flink Avro format support. |
| `flink-sql-avro-confluent-registry-1.19.3.jar` | Confluent Schema Registry Avro format support. |
| `flink-connector-jdbc-3.3.0-1.19.jar` | JDBC sink connector for the PostGIS projection inbox. |
| `postgresql-42.7.11.jar` | PostgreSQL JDBC driver. |
| `hadoop-client-api-3.3.6.jar` | Hadoop API classes required by Iceberg's Flink catalog path. |
| `hadoop-client-runtime-3.3.6.jar` | Hadoop runtime classes required by Iceberg's Flink catalog path. |

The connector layer was verified with Flink SQL probes:

- Kafka connector and raw format temporary table creation passed.
- Kafka connector with `avro-confluent` format passed.
- JDBC temporary table creation against the PostGIS projection table passed.
- Iceberg REST catalog creation against live Polaris + MinIO passed.

The Iceberg catalog needs the Polaris OAuth settings in
`010_kafka_to_bronze.sql`: `credential`, `oauth2-server-uri`, `scope`, and
`header.Polaris-Realm`.

Remaining work: run the full streaming insert jobs after Kafka, Schema
Registry, Polaris, MinIO, PostGIS, the Iceberg namespaces/tables, and sample
source events are all running together.
