import { randomUUID } from "node:crypto";
import pg from "pg";
import type { AnnotationLabel } from "~/lib/annotation-contract";
import {
  buildReviewQueueSql,
  mapReviewQueueRow,
  readLowConfidenceThreshold,
  type ReviewQueueItem,
  type ReviewQueueRow,
  type ReviewTaskStatus,
} from "~/lib/review-contract";
import type { PotholeDetail } from "~/lib/trino.server";

const { Pool } = pg;

export interface PotholeMarker {
  pothole_id: string;
  gps_lat: number;
  gps_lon: number;
  severity_level: string;
  status: string;
  district?: string;
  reported_at?: string;
}

export interface SummaryData {
  activePotholes: {
    count: number;
    trend: {
      today: { count: number; comparison: string };
      thisWeek: { count: number; comparison: string };
    };
  };
  averageSeverity: number;
  inProgress: number;
  activePotholesLast30Days: Array<{ date: string; count: number }>;
  severityDistribution: Record<string, number>;
  statusChanges: {
    reportedToInProgress: { thisWeek: number; comparison: string };
    inProgressToFixed: { thisWeek: number; comparison: string };
  };
  recentCritical: PotholeMarker[];
  topDistricts: Array<{ district: string; count: number }>;
}

export interface ReviewTaskDetail extends ReviewQueueItem {
  imageProxyUrl: string | null;
  originalPolygon: Array<[number, number]>;
  latestAnnotation: AnnotationLabel | null;
}

export interface InsertAnnotationInput {
  defectId: string;
  evidenceId: string | null;
  label: AnnotationLabel;
  annotatorId: string | null;
}

export interface AuditLogInput {
  actorId: string | null;
  action: string;
  entityType: string;
  entityId: string;
  payload: Record<string, unknown>;
}

export function emptySummaryData(): SummaryData {
  const now = new Date();
  return {
    activePotholes: {
      count: 0,
      trend: {
        today: { count: 0, comparison: "+0 vs yesterday" },
        thisWeek: { count: 0, comparison: "+0 last week" },
      },
    },
    averageSeverity: 0,
    inProgress: 0,
    activePotholesLast30Days: Array.from({ length: 30 }, (_, i) => {
      const d = new Date(now.getTime() - (29 - i) * 86400000);
      return { date: d.toISOString().split("T")[0], count: 0 };
    }),
    severityDistribution: { MINOR: 0, MODERATE: 0, HIGH: 0, CRITICAL: 0 },
    statusChanges: {
      reportedToInProgress: { thisWeek: 0, comparison: "+0 last week" },
      inProgressToFixed: { thisWeek: 0, comparison: "+0 last week" },
    },
    recentCritical: [],
    topDistricts: [],
  };
}

type PointGeometry = {
  type: "Point";
  coordinates: [number, number];
};

const POSTGIS_HOST = process.env.POSTGIS_HOST ?? "localhost";
const POSTGIS_PORT = Number(process.env.POSTGIS_PORT ?? "5437");
const POSTGIS_USER = process.env.POSTGIS_USER ?? "serving";
const POSTGIS_PASSWORD = process.env.POSTGIS_PASSWORD ?? "servingpassword";
const POSTGIS_DATABASE = process.env.POSTGIS_DATABASE ?? "postgis_serving";

let pool: pg.Pool | null = null;

function getPool() {
  pool ??= new Pool({
    host: POSTGIS_HOST,
    port: POSTGIS_PORT,
    user: POSTGIS_USER,
    password: POSTGIS_PASSWORD,
    database: POSTGIS_DATABASE,
    max: 10,
  });
  return pool;
}

async function queryPostgis<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  const result = await getPool().query(sql, params);
  return result.rows as T[];
}

export interface RoadDefectFilters {
  bbox?: string | null;
  datetime?: string | null;
  district?: string | null;
  ward?: string | null;
  roadSegmentId?: string | null;
  severityLevel?: string | null;
  status?: string | null;
  limit?: string | null;
  offset?: string | null;
}

export interface RoadDefectFeatureRow {
  defect_id: string;
  defect_type: string;
  status: string;
  severity_score: number | null;
  severity_level: string | null;
  confidence: number | null;
  quality_flags: unknown;
  geometry: PointGeometry;
  road_segment_id: string | null;
  district: string | null;
  ward: string | null;
  first_seen_at: string | null;
  last_seen_at: string | null;
  observation_count: number;
  latest_raw_image_object_key: string | null;
  latest_bev_object_key: string | null;
  longitude: number;
  latitude: number;
}

type GeoJsonFeature = {
  type: "Feature";
  id: string;
  geometry: PointGeometry;
  properties: Record<string, unknown>;
};

function parsePositiveInt(value: string | null | undefined, fallback: number, max: number) {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return Math.min(parsed, max);
}

function appendWhere(
  where: string[],
  params: unknown[],
  condition: string,
  ...values: unknown[]
) {
  const offset = params.length;
  params.push(...values);
  where.push(condition.replace(/\$(\d+)/g, (_, index) => `$${offset + Number(index)}`));
}

function buildRoadDefectWhere(filters: RoadDefectFilters) {
  const where: string[] = [];
  const params: unknown[] = [];

  if (filters.bbox) {
    const coords = filters.bbox.split(",").map((v) => Number.parseFloat(v.trim()));
    if (coords.length !== 4 || coords.some((v) => !Number.isFinite(v))) {
      throw new Response("Invalid bbox. Expected minLon,minLat,maxLon,maxLat.", { status: 400 });
    }
    appendWhere(
      where,
      params,
      "geometry && ST_MakeEnvelope($1, $2, $3, $4, 4326)",
      coords[0],
      coords[1],
      coords[2],
      coords[3]
    );
  }

  if (filters.datetime) {
    const [start, end] = filters.datetime.split("/");
    if (end) {
      appendWhere(where, params, "last_seen_at >= $1::timestamptz AND last_seen_at <= $2::timestamptz", start, end);
    } else {
      appendWhere(where, params, "last_seen_at >= $1::timestamptz", start);
    }
  }

  if (filters.district) appendWhere(where, params, "district = $1", filters.district);
  if (filters.ward) appendWhere(where, params, "ward = $1", filters.ward);
  if (filters.roadSegmentId) appendWhere(where, params, "road_segment_id = $1", filters.roadSegmentId);
  if (filters.severityLevel) appendWhere(where, params, "severity_level = $1", filters.severityLevel);
  if (filters.status) appendWhere(where, params, "status = $1", filters.status);

  return { clause: where.length ? `WHERE ${where.join(" AND ")}` : "", params };
}

const ROAD_DEFECT_SELECT = `
  SELECT
    defect_id,
    defect_type,
    status,
    severity_score,
    severity_level,
    confidence,
    quality_flags,
    ST_AsGeoJSON(geometry)::json AS geometry,
    road_segment_id,
    district,
    ward,
    first_seen_at,
    last_seen_at,
    observation_count,
    latest_raw_image_object_key,
    latest_bev_object_key,
    ST_X(geometry) AS longitude,
    ST_Y(geometry) AS latitude
  FROM serving.current_road_defects
`;

export async function queryRoadDefectItems(filters: RoadDefectFilters = {}) {
  const limit = parsePositiveInt(filters.limit, 100, 500);
  const offset = parsePositiveInt(filters.offset, 0, 100000);
  const { clause, params } = buildRoadDefectWhere(filters);
  params.push(limit, offset);
  return queryPostgis<RoadDefectFeatureRow>(
    `${ROAD_DEFECT_SELECT}
     ${clause}
     ORDER BY last_seen_at DESC NULLS LAST, defect_id
     LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params
  );
}

export async function queryRoadDefectById(defectId: string) {
  const rows = await queryPostgis<RoadDefectFeatureRow>(
    `${ROAD_DEFECT_SELECT} WHERE defect_id = $1 LIMIT 1`,
    [defectId]
  );
  return rows[0] ?? null;
}

export function toRoadDefectFeature(row: RoadDefectFeatureRow): GeoJsonFeature {
  const {
    defect_id,
    geometry,
    longitude: _longitude,
    latitude: _latitude,
    ...properties
  } = row;

  return {
    type: "Feature",
    id: defect_id,
    geometry,
    properties: {
      ...properties,
      defect_id,
    },
  };
}

export async function queryMapPotholes(
  lat?: number,
  lon?: number,
  radiusKm = 1
): Promise<PotholeMarker[]> {
  const filters: RoadDefectFilters = { limit: "30" };
  let bbox: string | undefined;

  if (lat !== undefined && lon !== undefined) {
    const deg = radiusKm * 0.009;
    const cosLat = Math.max(Math.cos((lat * Math.PI) / 180), 0.1);
    bbox = `${lon - deg / cosLat},${lat - deg},${lon + deg / cosLat},${lat + deg}`;
  }

  const rows = await queryRoadDefectItems({ ...filters, bbox });
  return rows.map((row) => ({
    pothole_id: row.defect_id,
    gps_lat: row.latitude,
    gps_lon: row.longitude,
    severity_level: row.severity_level ?? "UNKNOWN",
    status: row.status,
    district: row.district ?? undefined,
    reported_at: row.first_seen_at ?? undefined,
  }));
}

export async function queryPotholeDetail(id: string): Promise<PotholeDetail | null> {
  const row = await queryRoadDefectById(id);
  if (!row) return null;

  return {
    pothole_id: row.defect_id,
    first_event_id: "",
    gps_lat: row.latitude,
    gps_lon: row.longitude,
    city: "",
    ward: row.ward ?? "",
    district: row.district ?? "",
    street_name: "",
    road_id: row.road_segment_id ?? "",
    depth_cm: 0,
    surface_area_cm2: 0,
    severity_score: row.severity_score ?? 0,
    severity_level: row.severity_level ?? "UNKNOWN",
    status: row.status,
    detected_at: row.first_seen_at ?? "",
    in_progress_at: null,
    fixed_at: null,
    last_updated: row.last_seen_at ?? "",
    observation_count: row.observation_count,
    raw_image_path: row.latest_raw_image_object_key,
    bev_image_path: row.latest_bev_object_key,
  };
}

export async function querySummary(): Promise<SummaryData> {
  const [summaryRows, severityRows, topDistrictRows] = await Promise.all([
    queryPostgis<{
      active_count: string;
      average_severity: string | null;
      in_progress_count: string;
      new_today: string;
    }>(
      `
      SELECT
        COUNT(*) FILTER (WHERE status IN ('reported', 'in_progress')) AS active_count,
        AVG(severity_score) FILTER (WHERE status IN ('reported', 'in_progress')) AS average_severity,
        COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress_count,
        COUNT(*) FILTER (WHERE first_seen_at >= date_trunc('day', now())) AS new_today
      FROM serving.current_road_defects
      `
    ),
    queryPostgis<{ severity_level: string; count: string }>(
      `
      SELECT severity_level, COUNT(*) AS count
      FROM serving.current_road_defects
      WHERE status IN ('reported', 'in_progress') AND severity_level IS NOT NULL
      GROUP BY severity_level
      `
    ),
    queryPostgis<{ district: string | null; count: string }>(
      `
      SELECT COALESCE(district, 'Unknown') AS district, COUNT(*) AS count
      FROM serving.current_road_defects
      WHERE status IN ('reported', 'in_progress')
      GROUP BY COALESCE(district, 'Unknown')
      ORDER BY COUNT(*) DESC
      LIMIT 5
      `
    ),
  ]);

  const row = summaryRows[0];
  // Build severity_distribution from active-only counts (status IN ('reported', 'in_progress'))
  const severityDistribution = {
    MINOR: 0,
    MODERATE: 0,
    HIGH: 0,
    CRITICAL: 0,
  };
  for (const r of severityRows) {
    const key = r.severity_level.toUpperCase();
    if (key in severityDistribution) {
      severityDistribution[key as keyof typeof severityDistribution] = Number(r.count);
    }
  }

  const recentCritical = await queryMapPotholes();

  return {
    activePotholes: {
      count: Number(row?.active_count ?? 0),
      trend: {
        today: { count: Number(row?.new_today ?? 0), comparison: "+0 vs yesterday" },
        thisWeek: { count: 0, comparison: "+0 last week" },
      },
    },
    averageSeverity: Math.round(Number(row?.average_severity ?? 0) * 100) / 100,
    inProgress: Number(row?.in_progress_count ?? 0),
    activePotholesLast30Days: emptySummaryData().activePotholesLast30Days,
    severityDistribution,
    statusChanges: emptySummaryData().statusChanges,
    recentCritical: recentCritical.filter((p) => p.severity_level === "CRITICAL").slice(0, 5),
    topDistricts: topDistrictRows.map((r) => ({ district: r.district ?? "Unknown", count: Number(r.count) })),
  };
}

export async function queryReviewQueue(
  threshold = readLowConfidenceThreshold(process.env.LOW_CONFIDENCE_THRESHOLD),
): Promise<ReviewQueueItem[]> {
  const query = buildReviewQueueSql(threshold);
  const rows = await queryPostgis<ReviewQueueRow>(query.sql, query.params);
  return rows.map(mapReviewQueueRow);
}

export async function queryReviewTaskDetail(
  reviewTaskId: string,
): Promise<ReviewTaskDetail | null> {
  const rows = await queryReviewQueue();
  const item = rows.find((row) => row.reviewTaskId === reviewTaskId);
  if (!item) return null;

  const annotationRows = await queryPostgis<{ label_json: AnnotationLabel }>(
    `
      SELECT label_json
      FROM serving.annotations
      WHERE defect_id = $1
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [item.defectId],
  );

  return {
    ...item,
    imageProxyUrl: item.rawImageObjectKey
      ? `/api/image/proxy?path=${encodeURIComponent(item.rawImageObjectKey)}`
      : null,
    originalPolygon: [[200, 120], [350, 100], [380, 230], [220, 260]],
    latestAnnotation: annotationRows[0]?.label_json ?? null,
  };
}

export async function insertAnnotation(
  input: InsertAnnotationInput,
): Promise<string> {
  const annotationId = randomUUID();
  await queryPostgis(
    `
      INSERT INTO serving.annotations (
        annotation_id,
        defect_id,
        evidence_id,
        label_json,
        annotator_id
      )
      VALUES ($1, $2, $3, $4::jsonb, $5)
    `,
    [
      annotationId,
      input.defectId,
      input.evidenceId,
      JSON.stringify(input.label),
      input.annotatorId,
    ],
  );
  return annotationId;
}

export async function updateReviewTaskStatus(
  reviewTaskId: string,
  status: ReviewTaskStatus,
): Promise<void> {
  await queryPostgis(
    `
      UPDATE serving.review_tasks
      SET status = $2, updated_at = now()
      WHERE review_task_id = $1
    `,
    [reviewTaskId, status],
  );
}

export async function insertAuditLog(input: AuditLogInput): Promise<void> {
  await queryPostgis(
    `
      INSERT INTO serving.audit_log (
        actor_id,
        action,
        entity_type,
        entity_id,
        payload
      )
      VALUES ($1, $2, $3, $4, $5::jsonb)
    `,
    [
      input.actorId,
      input.action,
      input.entityType,
      input.entityId,
      JSON.stringify(input.payload),
    ],
  );
}
