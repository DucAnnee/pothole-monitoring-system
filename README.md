# Pothole Monitoring System

Hybrid edge-cloud platform for detecting potholes from bus-mounted cameras,
estimating physical severity, deduplicating detections geospatially, and
displaying the resulting road-defect state in a web dashboard.

## Standardized Pipeline

```text
edge
  -> pothole.raw.events.v2
  -> cloud/bev_surface_service
  -> pothole.surface.area.v2
  -> cloud/depth_estimation_model via Triton
  -> pothole.depth.v1
  -> cloud/severity_calculation_service
  -> pothole.severity.score.v1
  -> Flink
  -> iceberg.bronze.*
  -> iceberg.silver.*
  -> iceberg.gold.*
  -> PostGIS serving projection
  -> OGC API Features + dashboard
```

`graphify-out/GRAPH_REPORT.md` identifies the core graph hubs as
`ConfigLoader`, `EdgePipeline`, `ModelRegistry`, `Uploader`, `log_event()`, and
the cloud microservice pipeline.

The Python ETL/final-enrichment Iceberg writes are legacy table owners during
migration. New storage ownership belongs to Flink jobs in
`lakehouse/flink/sql/`.

## Repository Map

| Path | Purpose |
|---|---|
| `edge/` | Edge camera pipeline, segmentation, MinIO/Kafka upload, offline replay, MLOps model updater. |
| `cloud/bev_surface_service/` | Cloud-side BEV transform, mask projection, surface area calculation. |
| `cloud/depth_estimation_model/` | Depth service; batches image tensors and calls Triton gRPC. |
| `cloud/triton_inference_server/` | Triton model repository and deployment notes for Depth-Anything-V2 ONNX. |
| `cloud/severity_calculation_service/` | Stateless depth+area severity scoring. |
| `cloud/final_enrichment_service/` | Raw+severity aggregation, H3 dedup, OSM geocoding, Iceberg writes. |
| `cloud/etl_service/` | Kafka-to-Iceberg ingestion for raw, surface area, and severity topics. |
| `lakehouse/iceberg/` | Medallion Iceberg DDL for Bronze, Silver, Gold, and ML tables. |
| `lakehouse/flink/sql/` | Kafka-to-Bronze, Silver, Gold, and PostGIS projection Flink SQL jobs. |
| `lakehouse/postgis/` | Serving projection DDL for OGC/API/GIS workflows. |
| `web/` | Current React Router 7 + Vite web application and server-side API routes. |
| `dashboard/`, `dashboard-backend/` | Legacy/reference dashboard surfaces. |
| `tests/` | Contract and E2E tests for schemas, topics, services, and pipeline behavior. |
| `graphify-out/` | Knowledge graph report, graph JSON, and HTML visualization. |

## Data Contracts

Authoritative docs:

- `PIPELINE.md` - active event flow
- `KAFKA-CONF.md` - Avro schemas and Kafka topic configuration
- `SCHEMA.md` - Iceberg table schemas
- `lakehouse/iceberg/*.sql` - standardized Lakehouse DDL
- `lakehouse/postgis/*.sql` - serving projection DDL
- `container-conf/flink/lib/*.jar` - Flink connector/runtime JARs mounted into `/opt/flink/lib`
- `tests/contract/test_schema_contracts.py` - schema compatibility checks
- `tests/contract/test_topic_contracts.py` - topic creation checks

The current Kafka chain is:

| Topic | Role |
|---|---|
| `pothole.raw.events.v2` | Raw edge event with raw image key and original mask. |
| `pothole.surface.area.v2` | BEV image/mask and surface area from cloud BEV service. |
| `pothole.depth.v1` | Depth estimate plus surface area passthrough. |
| `pothole.severity.score.v1` | Final per-event severity score and level. |

## Quick Start

Start infrastructure:

```bash
docker compose up -d
```

Important service ports:

| Service | Port |
|---|---:|
| Trino | `8081` |
| Flink UI | `8084` |
| PostGIS serving | `5437` |
| MinIO API / console | `9000` / `9090` |

Flink connector/runtime JARs are expected in `container-conf/flink/lib/` and
are mounted individually into both Flink services. The verified local set is:
Iceberg Flink runtime `1.10.1` for Flink `1.19`, Iceberg AWS bundle `1.10.1`,
Flink Kafka `3.3.0-1.19`, Flink JDBC `3.3.0-1.19`, Flink Avro `1.19.3`,
Flink Confluent Avro `1.19.3`, PostgreSQL JDBC `42.7.11`, and Hadoop client
API/runtime `3.3.6`.

Run legacy cloud services while migrating storage ownership:

```bash
python cloud/etl_service/etl_microservice.py
python cloud/bev_surface_service/bev_surface_service.py
python cloud/depth_estimation_model/cloud_pipeline.py
python cloud/severity_calculation_service/severity_aggregator.py
python cloud/final_enrichment_service/final_enrichment_service.py
```

Prepare and run Triton:

```bash
python cloud/depth_estimation_model/export_to_onnx.py
docker compose up triton-inference-server -d
curl http://localhost:8000/v2/health/ready
```

Run the edge pipeline:

```bash
python edge/main.py
python edge/main.py --video path/to/video.mp4
```

Run the current web app:

```bash
cd web
npm install
npm run dev
```

OGC API Features-style endpoints:

```text
GET /api/v1/collections
GET /api/v1/collections/road-defects/items
GET /api/v1/collections/road-defects/items/{defect_id}
```

## Verification

```bash
pytest tests/contract
pytest tests/e2e
```

After code changes, keep the knowledge graph current:

```bash
graphify update .
```

## Current Production Gaps

- GPS is simulated and `vehicle_id` is generated at process startup.
- BEV/depth failures need DLQs and quality flags instead of being treated as ordinary low-severity observations.
- Final pothole records retain raw image evidence but not enough BEV/model/calibration evidence.
- Flink connector packaging is verified, but the full streaming jobs still need end-to-end fixture/replay validation.
- Web auth is demo-grade and API routes need production authentication/RBAC.
- Production deployment assets, secrets, TLS, Kafka ACLs, observability, and backup/restore controls are still future work.
