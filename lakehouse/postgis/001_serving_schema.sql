CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.current_road_defects (
  defect_id TEXT PRIMARY KEY,
  defect_type TEXT NOT NULL DEFAULT 'POTHOLE',
  status TEXT NOT NULL,
  severity_score DOUBLE PRECISION,
  severity_level TEXT,
  confidence DOUBLE PRECISION,
  quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
  geometry GEOMETRY(Point, 4326) NOT NULL,
  road_segment_id TEXT,
  district TEXT,
  ward TEXT,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  observation_count INTEGER NOT NULL DEFAULT 0,
  latest_raw_image_object_key TEXT,
  latest_bev_object_key TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS current_road_defects_geometry_gix
  ON serving.current_road_defects USING GIST (geometry);
CREATE INDEX IF NOT EXISTS current_road_defects_district_idx
  ON serving.current_road_defects (district);
CREATE INDEX IF NOT EXISTS current_road_defects_ward_idx
  ON serving.current_road_defects (ward);
CREATE INDEX IF NOT EXISTS current_road_defects_road_segment_idx
  ON serving.current_road_defects (road_segment_id);
CREATE INDEX IF NOT EXISTS current_road_defects_severity_idx
  ON serving.current_road_defects (severity_level);
CREATE INDEX IF NOT EXISTS current_road_defects_status_idx
  ON serving.current_road_defects (status);
CREATE INDEX IF NOT EXISTS current_road_defects_last_seen_idx
  ON serving.current_road_defects (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS serving.current_road_defects_projection_inbox (
  defect_id TEXT PRIMARY KEY,
  defect_type TEXT NOT NULL DEFAULT 'POTHOLE',
  status TEXT NOT NULL,
  severity_score DOUBLE PRECISION,
  severity_level TEXT,
  confidence DOUBLE PRECISION,
  quality_flags TEXT NOT NULL DEFAULT '[]',
  longitude DOUBLE PRECISION NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  road_segment_id TEXT,
  district TEXT,
  ward TEXT,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  observation_count INTEGER NOT NULL DEFAULT 0,
  latest_raw_image_object_key TEXT,
  latest_bev_object_key TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION serving.apply_current_road_defect_projection()
RETURNS TRIGGER AS $$
DECLARE
  existing_id   TEXT;
  incoming_geom GEOMETRY(Point, 4326);
BEGIN
  incoming_geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);

  -- Merge into nearest existing defect within 15m; insert new row otherwise.
  SELECT defect_id INTO existing_id
  FROM serving.current_road_defects
  WHERE ST_DWithin(geometry::geography, incoming_geom::geography, 15.0)
  ORDER BY ST_Distance(geometry::geography, incoming_geom::geography)
  LIMIT 1;

  IF existing_id IS NOT NULL THEN
    UPDATE serving.current_road_defects SET
      observation_count           = observation_count + 1,
      last_seen_at                = GREATEST(last_seen_at, NEW.last_seen_at),
      severity_score              = NEW.severity_score,
      severity_level              = NEW.severity_level,
      confidence                  = NEW.confidence,
      latest_raw_image_object_key = COALESCE(NEW.latest_raw_image_object_key, latest_raw_image_object_key),
      latest_bev_object_key       = COALESCE(NEW.latest_bev_object_key, latest_bev_object_key),
      updated_at                  = now()
    WHERE defect_id = existing_id;
  ELSE
    INSERT INTO serving.current_road_defects (
      defect_id, defect_type, status,
      severity_score, severity_level, confidence, quality_flags,
      geometry, road_segment_id, district, ward,
      first_seen_at, last_seen_at, observation_count,
      latest_raw_image_object_key, latest_bev_object_key, updated_at
    ) VALUES (
      NEW.defect_id, NEW.defect_type, NEW.status,
      NEW.severity_score, NEW.severity_level, NEW.confidence, NEW.quality_flags::jsonb,
      incoming_geom, NEW.road_segment_id, NEW.district, NEW.ward,
      NEW.first_seen_at, NEW.last_seen_at, NEW.observation_count,
      NEW.latest_raw_image_object_key, NEW.latest_bev_object_key, NEW.updated_at
    );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS current_road_defects_projection_apply
  ON serving.current_road_defects_projection_inbox;
-- INSERT-only: avoids double-counting observation_count on Flink retract messages.
CREATE TRIGGER current_road_defects_projection_apply
AFTER INSERT ON serving.current_road_defects_projection_inbox
FOR EACH ROW EXECUTE FUNCTION serving.apply_current_road_defect_projection();

CREATE TABLE IF NOT EXISTS serving.defect_evidence_index (
  evidence_id TEXT PRIMARY KEY,
  defect_id TEXT NOT NULL REFERENCES serving.current_road_defects(defect_id) ON DELETE CASCADE,
  event_id TEXT NOT NULL,
  raw_image_object_key TEXT,
  bev_object_key TEXT,
  captured_at TIMESTAMPTZ,
  quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS defect_evidence_index_defect_idx
  ON serving.defect_evidence_index (defect_id);

CREATE TABLE IF NOT EXISTS serving.review_tasks (
  review_task_id TEXT PRIMARY KEY,
  defect_id TEXT NOT NULL REFERENCES serving.current_road_defects(defect_id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  priority TEXT,
  assigned_to TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS serving.annotations (
  annotation_id TEXT PRIMARY KEY,
  defect_id TEXT NOT NULL REFERENCES serving.current_road_defects(defect_id) ON DELETE CASCADE,
  evidence_id TEXT REFERENCES serving.defect_evidence_index(evidence_id) ON DELETE SET NULL,
  label_json JSONB NOT NULL,
  annotator_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS serving.device_registry (
  device_id TEXT PRIMARY KEY,
  vehicle_id TEXT,
  device_type TEXT,
  firmware_version TEXT,
  status TEXT,
  last_seen_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS serving.audit_log (
  audit_id BIGSERIAL PRIMARY KEY,
  actor_id TEXT,
  action TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
