# Pipeline Standardization — Gap Analysis and Design Spec

> Derived from cross-referencing `refinement-plan.draft.md` against all context
> files, Avro schemas, Flink SQL, Iceberg DDL, and PostGIS schema.
> Date: 2026-05-05

---

## Summary

Nine gap areas remain before the pipeline and system are fully standardized.
They are ordered by dependency: data contract changes come first, then storage
pipeline completion, then consumers and operations layers.

---

## Gap 1 — Avro v3 Schema and Edge Identity

**What is broken / missing:**

- `pothole.raw.events.v2` Avro schema has no `device_id` field. Bronze
  `raw_detection_events` declares `device_id VARCHAR` and populates it `NULL`
  because the Kafka record has none.
- `vehicle_id` is generated at `EdgePipeline.__init__()` as
  `f"vehicle-{uuid4().hex[:8]}"`. It changes every process restart — no stable
  bus identity.
- GPS is simulated (`_generate_random_gps()`). No GPS provider abstraction exists.
- `severity_score` in `pothole.severity.score.v1` Avro is `int`; medallion
  bronze/silver/gold tables declare `DOUBLE`. Type mismatch survives silently
  today but will break typed consumers.
- `KAFKA-CONF.md` explicitly deferred v3 for "stable device identity, real GPS,
  model lineage, and normalized quality flags."

**What needs to happen:**

- Define `pothole.raw.events.v3` Avro schema adding: `device_id STRING`,
  `model_id STRING` (nullable), `quality_flags ARRAY<STRING>` (nullable/default
  `[]`). Keep all v2 fields unchanged.
- Fix `severity_score` type: change Avro `int` → `double` in severity schema
  (matches medallion DDL). Version as `pothole.severity.score.v2` or add as
  compatible evolution if Schema Registry allows.
- Add `GpsProvider` abstraction to edge (`interface + SimulatedGpsProvider +
  SerialNmeaGpsProvider` stub). Config selects provider. Simulated provider keeps
  current behavior.
- Add `device_id` to edge config (static string, e.g. `edge-device-001`). Read
  from env var `EDGE_DEVICE_ID`. Populate `device_id` in published Avro events.
- Make `vehicle_id` persistent: read from a local state file at
  `edge/local_storage/.device_state.json`; generate once, persist, reload on
  restart.
- Flink job `010_kafka_to_bronze.sql` Kafka source DDL must be updated to read
  `device_id` and `quality_flags` from v3 schema.

**Files to change:**

- Edge: `main.py`, `uploader.py`, `data_models.py`, `config.yaml`,
  `config_loader.py` (new GPS provider abstraction)
- Avro schemas registered in Schema Registry (new subjects or evolved schemas)
- `lakehouse/flink/sql/010_kafka_to_bronze.sql` (update source DDL)
- `KAFKA-CONF.md` (document v3)

---

## Gap 2 — Flink Jobs End-to-End Validation

**What is broken / missing:**

- Per `PIPELINE.md` and `lakehouse/flink/sql/README.md`: "Full streaming insert
  jobs still need end-to-end run with Kafka topics, registered schemas, Iceberg
  DDL, and sample events."
- Connector probes passed (Kafka table creation, JDBC, Iceberg REST catalog) but
  actual streaming INSERTs have never run against a real event flow.
- No integration tests exercise the full Flink path.
- Iceberg namespaces and tables must be pre-created via Trino DDL before Flink
  jobs start; no automated bootstrapping exists.
- `010_kafka_to_bronze.sql` uses `scan.startup.mode = earliest-offset` — fine
  for first run, but means replayed events on job restart. No checkpointing
  config present.

**What needs to happen:**

- Write a bootstrap script (or `docker-compose` init service) that runs Iceberg
  DDL via Trino before Flink jobs start:
  `lakehouse/iceberg/001–040` → Trino → confirm namespaces + tables exist.
- Add a Flink SQL runner script at `lakehouse/flink/run_jobs.sh` (or `run_jobs.py`)
  that submits all four SQL files to the Flink REST API in order.
- Add contract test `tests/contract/test_flink_pipeline.py` with a fixture that:
  1. Publishes sample events to each Kafka topic via producer.
  2. Waits for records to appear in corresponding bronze Iceberg tables via Trino.
  3. Asserts correct column values and partition.
- Add Flink checkpointing config (filesystem backend, checkpoint interval 60s)
  to both `flink-jobmanager` and `flink-taskmanager` in `docker-compose.yml`.
  Mount a `flink-checkpoints` volume; set env
  `execution.checkpointing.interval: 60000` and `state.backend: filesystem`.
- Update `lakehouse/flink/sql/README.md` with step-by-step run instructions.

**Files to change / create:**

- `lakehouse/flink/bootstrap_iceberg.sql` or script
- `lakehouse/flink/run_jobs.sh`
- `tests/contract/test_flink_pipeline.py`
- `docker-compose.yml` (Flink checkpointing env vars)

---

## Gap 3 — Silver H3 Deduplication and Enrichment

**What is broken / missing:**

- `silver.observations.defect_id = CONCAT('defect-', event_id)` — synthetic,
  every event gets its own "defect." No H3 cell dedup.
- `silver.detections.h3_cell` and `silver.observations` `gps_lat`/`gps_lon`
  exist but `h3_cell` is always NULL from Flink.
- `gold.current_road_defects` `h3_cell`, `road_segment_id`, `district`, `ward`
  all NULL.
- Real H3 dedup logic exists in `cloud/final_enrichment_service` (Python,
  writing to legacy `iceberg.city.potholes`) but has no Flink equivalent.
- Gold GROUP BY is on synthetic `defect_id`, so "aggregation" is 1:1 with events —
  no actual deduplication.

**What needs to happen:**

- Implement H3 dedup in the Flink path. Two options:
  - **Option A (recommended):** Flink Stateful DataStream job (Java/Python) that
    maintains H3 cell → defect_id mapping in keyed state. For each observation,
    look up existing defect for same H3 cell (res 12); if found, re-use defect_id;
    else generate new UUID. Emit enriched observation to silver.
  - **Option B:** Keep dedup in a separate Python enrichment microservice that
    reads `bronze.raw_detection_events` from Iceberg and writes to silver via
    Flink SQL. Higher latency, simpler.
  
  Option A is preferred: native Flink stateful dedup, low latency, no extra service.

- Add H3 UDF or use Flink UDTF to compute `h3_cell` from `gps_lat`, `gps_lon`.
  Register as Flink catalog function.

- Add OSM reverse-geocode lookup in the enrichment path to populate `district`,
  `ward` on silver/gold records. Can reuse Redis cache strategy from Final Enrichment
  service (key: `osm:h3:<h3_hex>`, TTL 30 days). Implement as a Flink async I/O
  function.

- `030_gold_materialization.sql` GROUP BY `defect_id` already correct — once
  silver dedup assigns real defect_ids, gold correctly aggregates.

**Files to change / create:**

- New Flink DataStream job: `lakehouse/flink/sql/021_silver_h3_dedup.sql` (if
  UDF-based) or `lakehouse/flink/jobs/SilverEnrichmentJob.java/py`
- `lakehouse/iceberg/020_silver_tables.sql` (no DDL change needed, h3_cell already there)
- Possibly a Flink UDF JAR for H3 computation

---

## Gap 4 — Failure Handling: DLQs and Quality Flags

**What is broken / missing:**

- DLQ topics (`pothole.*.dlq.v1`) are created by `kafka-init` but **none of the
  cloud services publish to them**. All failures are logged and swallowed.
- BEV failure produces a valid-looking event with `confidence=0.0`,
  `surface_area_cm2=0.0`, empty `bev_object_key`. Depth service falls back to
  raw image silently. Severity computes minimum score. No downstream signal.
- `quality_flags_json` in bronze/silver/gold is always `'[]'` — nothing sets flags.
- `bronze.pipeline_latency_events`, `bronze.device_telemetry_events`,
  `bronze.model_deployment_events`, `bronze.review_events` — tables exist, nothing writes them.
- Final Enrichment aggregation timeout (300s): if severity never arrives (Triton
  down), raw event is silently dropped. No DLQ, no quality flag.

**What needs to happen:**

- **BEV Service:** On failure, produce a surface area event with
  `confidence=0.0`, `bev_object_key=""`, `surface_area_cm2=0.0` AND also publish
  the original raw event to `pothole.raw.events.dlq.v1` with header
  `quality_flag = "BEV_FAILURE"`. Keep producing the zero-confidence surface area
  event so the depth service still receives the signal and can fall back to the
  raw image — removing the event entirely would stall the depth service's
  aggregation.
- **Depth Service:** On Triton failure or degenerate depth map, publish depth
  event to `pothole.depth.dlq.v1`. Add `quality_flag = "DEPTH_FAILURE"`.
- **Final Enrichment:** On aggregation timeout, publish raw event to
  `pothole.raw.events.dlq.v1` with `quality_flag = "SEVERITY_TIMEOUT"`.
- **ETL Service:** On Iceberg write failure, publish to corresponding DLQ topic.
- **Quality flags propagation:** Flink `010_kafka_to_bronze.sql` should carry
  `quality_flags` from v3 Avro events into bronze. Silver/gold materialize from
  bronze quality flags.
- **Latency events:** `cloud/shared/latency_tracker.py` writes to Redis. Extend
  to also publish to `bronze.pipeline_latency_events` via Kafka topic
  `pothole.pipeline.latency.v1` consumed by Flink.
- **Review routing:** Gold/PostGIS projection should include a `needs_review`
  flag (true when `quality_flags` not empty or confidence below threshold).
  `serving.review_tasks` auto-populated for flagged defects.

**Files to change:**

- `cloud/bev_surface_service/bev_surface_service.py`
- `cloud/depth_estimation_model/cloud_pipeline.py`
- `cloud/final_enrichment_service/final_enrichment_service.py`
- `cloud/etl_service/etl_microservice.py`
- `cloud/shared/latency_tracker.py`
- `lakehouse/flink/sql/010_kafka_to_bronze.sql` (add quality_flags column to sources)
- `lakehouse/flink/sql/020_silver_materialization.sql` (propagate quality flags)
- `lakehouse/flink/sql/030_gold_materialization.sql` (aggregate quality flags)
- `lakehouse/flink/sql/040_gold_to_postgis_projection.sql` (add needs_review)
- `lakehouse/postgis/001_serving_schema.sql` (add needs_review column, auto-review_tasks trigger)

---

## Gap 5 — Evidence Persistence (Model Lineage and Image Evidence)

**What is broken / missing:**

- `silver.defect_evidence`: `raw_image_sha256`, `bev_image_sha256`, `model_id`,
  `calibration_id` all NULL. These are important for audit and re-validation.
- `silver.detections.model_id` and `calibration_id` NULL — no per-event model
  lineage.
- `silver.models` and `silver.calibrations` tables exist but nothing writes them.
- `ml.*` tables (training items, model validation, lineage) exist but are empty.
- Edge MLOps tracks model registry locally (`edge/mlops/model_registry.json`) but
  does not propagate `model_id` to published Avro events.
- BEV calibration version (`edge/.conf/camera_calibration.json`) not versioned
  or propagated to events.
- `gold.current_road_defects.latest_bev_object_key` will be populated once
  Flink silver/gold is running, but `potholes` in legacy `iceberg.city` still
  lacks `bev_object_key` (web still shows old table).

**What needs to happen:**

- Add `model_id` and `calibration_id` to Avro v3 raw schema (nullable strings).
  Edge writes its active `RuntimeModel.model_id` and calibration file hash into
  each event.
- Compute `raw_image_sha256` in ETL service (download image from MinIO + hash)
  or in BEV service before upload. Store in `pothole.surface.area.v3` or as a
  separate metadata field.
- Flink 020 silver job: populate `silver.detections.model_id` from bronze field;
  populate `silver.defect_evidence.raw_image_sha256` and `bev_image_sha256`.
- Write `silver.models` record on `bronze.model_deployment_events` (new Flink
  insert into silver from model_deployment topic).
- Write `silver.calibrations` from a static calibration bootstrap or from events
  when calibration changes.
- Wire `ml.training_dataset_items`: when an annotation is created in
  `serving.annotations`, create an ML training item record.

**Files to change:**

- `edge/main.py`, `edge/uploader.py`, `edge/data_models.py` (propagate model_id)
- Avro v3 schema (add model_id, calibration_id)
- `cloud/bev_surface_service/bev_processor.py` (compute + attach sha256)
- `lakehouse/flink/sql/020_silver_materialization.sql`
- New bootstrap: `lakehouse/iceberg/bootstrap_calibrations.sql` or service

---

## Gap 6 — Web Data Source Migration (PostGIS-first + Gold Tables)

**What is broken / missing:**

- `web/app/lib/trino.server.ts` queries `iceberg.city` schema (legacy). Env var
  `TRINO_SCHEMA=city` is the default.
- `/api/map-data` queries Trino for map markers; should query PostGIS for spatial
  reads (indexed geometry, lower latency for bbox queries).
- `web/app/lib/trino.server.ts` exposes `raw_image_path` and `bev_image_path`
  in `PotholeDetail` type — these map to old legacy column names.
- No `postgis.server.ts` helper exists in web (referenced in `web/CLAUDE.md`
  but not yet created).
- OGC API Features routes (`/api/v1/collections/road-defects/items`) are defined
  in `PIPELINE.md` and `README.md` but may not exist in web app routes.
- Analytics routes (overview stats, district severity) should read from
  `gold.dashboard_summary_daily` and `gold.district_severity_daily` via Trino,
  not from Trino queries that scan `iceberg.city`.

**What needs to happen:**

- Create `web/app/lib/postgis.server.ts`: PostGIS connection via `pg` client,
  `queryDefects(bbox, filters)`, `getDefectById(id)`, spatial GeoJSON serializer.
- Migrate `/api/map-data` from Trino `iceberg.city.potholes` to PostGIS
  `serving.current_road_defects` with bbox filter on geometry index.
- Migrate `/api/pothole/:id` from Trino to PostGIS `serving.current_road_defects`
  JOIN `serving.defect_evidence_index`.
- Implement OGC API Features routes:
  - `GET /api/v1/collections` → list of available collections
  - `GET /api/v1/collections/road-defects/items` → GeoJSON FeatureCollection
    from PostGIS with bbox/limit/offset params
  - `GET /api/v1/collections/road-defects/items/:id` → single Feature
- Update Trino queries for analytics pages (overview, health) to use
  `gold.dashboard_summary_daily`, `gold.pipeline_latency_summary` instead of
  raw `iceberg.city` scans.
- Update `PotholeDetail` TypeScript type: replace `raw_image_path`/`bev_image_path`
  with `raw_image_object_key`/`bev_object_key`.

**Files to change / create:**

- `web/app/lib/postgis.server.ts` (new)
- `web/app/routes/api.map-data.tsx`
- `web/app/routes/api.pothole.$id.tsx`
- `web/app/routes/api.v1.collections.tsx` (new)
- `web/app/routes/api.v1.collections.road-defects.items.tsx` (new)
- `web/app/routes/api.v1.collections.road-defects.items.$id.tsx` (new)
- `web/app/lib/trino.server.ts` (update queries + types)
- `web/app/routes/_app.overview.tsx` (update data loader)

---

## Gap 7 — Human Review and Annotation Wiring

**What is broken / missing:**

- `serving.review_tasks`, `serving.annotations`, `serving.defect_evidence_index`
  tables exist in PostGIS but nothing writes to them.
- `bronze.review_events` table exists but nothing writes to it.
- Web pages `/dataset`, `/lowconf`, `/annotation` are concept/scaffold pages
  with no real data backend.
- Low-confidence detections are not routed to a review queue. No `needs_review`
  flag exists on defect records.
- `ml.training_dataset_items` and `ml.annotation_versions` exist but nothing
  populates them.

**What needs to happen:**

- Add `needs_review BOOLEAN` column to `serving.current_road_defects` and
  `gold.current_road_defects`. Set true when `confidence < threshold` or
  `quality_flags` is non-empty.
- Add API routes for review workflow:
  - `POST /api/v1/review-tasks` → create review task for a defect
  - `PUT /api/v1/review-tasks/:id` → update status (pending → in_review → resolved)
  - `POST /api/v1/annotations` → save annotation for a defect + evidence
  - `GET /api/v1/collections/road-defects/items?needs_review=true` → filter for
    defects needing review
- Wire `/lowconf` page to query `serving.current_road_defects WHERE needs_review = true`.
- Wire `/annotation` page to POST to `/api/v1/annotations`.
- On annotation creation: write `bronze.review_events` via a Kafka topic
  `pothole.review.events.v1` consumed by Flink → `bronze.review_events` and
  create `ml.training_dataset_items` entry.
- Auto-create review tasks in PostGIS trigger when `needs_review` becomes true
  on insert/update.

**Files to change / create:**

- `lakehouse/postgis/001_serving_schema.sql` (add needs_review, auto-review trigger)
- `lakehouse/iceberg/030_gold_tables.sql` (add needs_review)
- `lakehouse/flink/sql/030_gold_materialization.sql` (populate needs_review)
- `web/app/routes/_app.lowconf.tsx`
- `web/app/routes/_app.annotation.tsx`
- New API routes for review/annotation
- New Kafka topic `pothole.review.events.v1` (in `kafka-init` entrypoint)
- New Flink source in `010_kafka_to_bronze.sql` or new file `015_review_events.sql`

---

## Gap 8 — Auth, RBAC, and Audit Logging

**What is broken / missing:**

- Web auth is demo-grade: `_auth.login.tsx` exists but no real session validation
  protects API routes.
- `serving.audit_log` table exists in PostGIS but nothing writes to it.
- API routes return data without authentication checks.
- No role definitions (read-only viewer vs. reviewer vs. admin).
- Image proxy (`/api/image/proxy`) exposes MinIO images without auth.

**What needs to happen:**

- Implement session-based auth using a secret-signed cookie. Replace demo login
  with a real credential check against a config-driven user list or
  environment-provided credentials (no external IdP required for Phase 1).
- Add auth middleware applied to all `/api/*` routes and all app routes except
  `/login`.
- Define three roles: `viewer` (read-only), `reviewer` (can create review tasks
  and annotations), `admin` (can manage settings and devices).
- Write to `serving.audit_log` on: review task creation/update, annotation
  creation, image proxy access, login/logout.
- Protect image proxy: verify session before proxying MinIO object.

**Note:** Full production identity/SSO is out of scope here. The target is
"not demo-grade" — no anonymous data access and a proper audit trail.

**Files to change / create:**

- `web/app/lib/auth.server.ts` (session helpers, role check)
- `web/app/routes/_auth.login.tsx` (real credential validation)
- `web/app/middleware.ts` or route-level auth guards
- `web/app/routes/api.image.proxy.tsx` (add auth check)
- `web/app/lib/postgis.server.ts` (add `writeAuditLog()` helper)

---

## Gap 9 — Legacy Service Retirement Criteria

**What is broken / missing:**

- `cloud/etl_service` and `cloud/final_enrichment_service` still write
  `iceberg.city.*` tables.
- Flink jobs are intended to own new storage but are not end-to-end validated.
- No defined criteria for when the legacy Python writers can be shut down.
- Web app still queries `iceberg.city` via Trino.
- `iceberg.city.*` tables will diverge from `iceberg.bronze/silver/gold.*` during
  migration; no reconciliation mechanism.

**What needs to happen:**

- Define retirement criteria (all must be true to retire legacy services):
  1. Flink jobs 010–040 validated end-to-end (Gap 2 complete).
  2. Web app data sources migrated to PostGIS/gold (Gap 6 complete).
  3. Silver H3 dedup producing equivalent dedup quality to final enrichment (Gap 3).
  4. No consumer group active on `iceberg.city.*` writes (verify via Trino queries).
- Add a deprecation flag to ETL and final enrichment: `LEGACY_WRITE_ENABLED=true`
  env var (default true). When false, services stop writing to `iceberg.city.*`
  but continue consuming Kafka (for DLQ passthrough during cutover).
- Add migration validation test `tests/contract/test_migration_parity.py`:
  queries equivalent records from `iceberg.city.potholes` and
  `gold.current_road_defects` and diffs counts + field values.

**Files to change:**

- `cloud/etl_service/etl_microservice.py` (LEGACY_WRITE_ENABLED gate)
- `cloud/final_enrichment_service/final_enrichment_service.py` (same)
- `tests/contract/test_migration_parity.py` (new)
- Documentation: `PIPELINE.md`, `README.md` (add retirement criteria section)

---

## Implementation Order (Recommended)

Dependencies flow in this order. Later gaps depend on earlier ones.

```
Gap 1 (Avro v3 + identity)
  └─> Gap 4 (quality flags can't propagate without v3 schema)
        └─> Gap 3 (enrichment quality depends on quality flags)
              └─> Gap 2 (Flink end-to-end validation unblocks all storage consumers)
                    └─> Gap 5 (evidence fields need Flink path working)
                          └─> Gap 6 (web migration needs Gold tables populated)
                                └─> Gap 7 (review wiring needs web migration)
                                      └─> Gap 8 (auth wraps everything above)
                                            └─> Gap 9 (retirement after everything works)
```

Gaps 1, 2, 4 can be started in parallel in separate branches since they
affect different layers. Gap 3 (Flink H3 dedup) is the highest complexity item.

---

## Out of Scope for This Spec

- Kubernetes / Helm deployment assets
- TLS, Kafka ACLs, MinIO bucket policies (infrastructure hardening)
- External IdP / SSO integration
- Road-segment geometry matching (ITS interoperability)
- GeoJSON/CSV/Parquet export endpoints
- GPU/model serving scaling (Triton multi-instance, load balancing)
