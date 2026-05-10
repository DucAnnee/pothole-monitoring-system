-- Migration: replace per-event INSERT trigger with spatial dedup trigger.
-- Merges incoming detections within 15m of an existing defect instead of
-- always inserting a new row. Switches to INSERT-only to avoid double-counting
-- observation_count on Flink retract/update messages.

CREATE OR REPLACE FUNCTION serving.apply_current_road_defect_projection()
RETURNS TRIGGER AS $$
DECLARE
  existing_id   TEXT;
  incoming_geom GEOMETRY(Point, 4326);
BEGIN
  incoming_geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);

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

CREATE TRIGGER current_road_defects_projection_apply
AFTER INSERT ON serving.current_road_defects_projection_inbox
FOR EACH ROW EXECUTE FUNCTION serving.apply_current_road_defect_projection();
