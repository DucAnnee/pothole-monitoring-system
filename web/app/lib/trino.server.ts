const TRINO_HOST = process.env.TRINO_HOST ?? "localhost";
const TRINO_PORT = process.env.TRINO_PORT ?? "8081";
const TRINO_USER = process.env.TRINO_USER ?? "admin";
const TRINO_CATALOG = process.env.TRINO_CATALOG ?? "iceberg";
const TRINO_SCHEMA = process.env.TRINO_SCHEMA ?? "city";
const BASE_URL = `http://${TRINO_HOST}:${TRINO_PORT}`;

const TRINO_HEADERS = {
  "X-Trino-User": TRINO_USER,
  "X-Trino-Catalog": TRINO_CATALOG,
  "X-Trino-Schema": TRINO_SCHEMA,
  "X-Trino-Source": "pothole-web",
  "Content-Type": "text/plain",
};

interface TrinoResponse {
  id: string;
  nextUri?: string;
  columns?: Array<{ name: string; type: string }>;
  data?: unknown[][];
  stats: { state: string };
  error?: { message: string };
}

async function trinoFetch(sql: string): Promise<Record<string, unknown>[]> {
  const initRes = await fetch(`${BASE_URL}/v1/statement`, {
    method: "POST",
    headers: TRINO_HEADERS,
    body: sql,
  });

  if (!initRes.ok) {
    throw new Error(`Trino POST failed: ${initRes.status}`);
  }

  let state = (await initRes.json()) as TrinoResponse;
  const columns: string[] = [];
  const rows: Record<string, unknown>[] = [];

  if (state.columns) {
    columns.push(...state.columns.map((c) => c.name));
  }
  if (state.data) {
    for (const row of state.data) {
      rows.push(Object.fromEntries(columns.map((c, i) => [c, row[i]])));
    }
  }

  while (state.nextUri) {
    await new Promise((r) => setTimeout(r, 50));
    const res = await fetch(state.nextUri, { headers: TRINO_HEADERS });
    state = (await res.json()) as TrinoResponse;

    if (state.error) throw new Error(state.error.message);
    if (state.columns && columns.length === 0) {
      columns.push(...state.columns.map((c) => c.name));
    }
    if (state.data) {
      for (const row of state.data) {
        rows.push(Object.fromEntries(columns.map((c, i) => [c, row[i]])));
      }
    }
  }

  if (state.stats.state === "FAILED") {
    throw new Error(`Trino query failed: ${state.error?.message}`);
  }

  return rows;
}

function formatTs(date: Date) {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

function getWeekBounds(date: Date) {
  const d = new Date(date);
  const day = d.getUTCDay();
  const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1);
  const start = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), diff));
  const end = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), diff + 6, 23, 59, 59, 999));
  return { start, end };
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

export interface PotholeMarker {
  pothole_id: string;
  gps_lat: number;
  gps_lon: number;
  severity_level: string;
  status: string;
  district?: string;
  reported_at?: string;
}

export interface PotholeDetail {
  pothole_id: string;
  first_event_id: string;
  gps_lat: number;
  gps_lon: number;
  city: string;
  ward: string;
  district: string;
  street_name: string;
  road_id: string;
  depth_cm: number;
  surface_area_cm2: number;
  severity_score: number;
  severity_level: string;
  status: string;
  detected_at: string;
  in_progress_at: string | null;
  fixed_at: string | null;
  last_updated: string;
  observation_count: number;
  raw_image_path: string | null;
  bev_image_path: string | null;
}

export interface LatencyStats {
  stages: Record<string, { avg_ms: number; p50_ms: number; p95_ms: number; p99_ms: number; min_ms: number; max_ms: number; count: number }>;
  microservices: Array<{ name: string; key: string; latency_ms: number; p95_ms: number; status: string }>;
  total_pipeline: { avg_ms: number; p50_ms: number; p95_ms: number; p99_ms: number; min_ms: number; max_ms: number };
}

export async function querySummary(): Promise<SummaryData> {
  const now = new Date();
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
  const todayStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const todayEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 23, 59, 59, 999));
  const yesterdayStart = new Date(todayStart.getTime() - 86400000);
  const yesterdayEnd = new Date(todayEnd.getTime() - 86400000);
  const thisWeek = getWeekBounds(now);
  const lastWeek = {
    start: new Date(thisWeek.start.getTime() - 7 * 86400000),
    end: new Date(thisWeek.end.getTime() - 7 * 86400000),
  };

  const rows = await trinoFetch(`
    SELECT pothole_id, status, severity_score, severity_level, reported_at,
           in_progress_at, fixed_at, district
    FROM iceberg.city.potholes
    WHERE reported_at >= TIMESTAMP '${formatTs(thirtyDaysAgo)}'
       OR status IN ('reported', 'in_progress')
  `);

  let activeCount = 0, activeSeveritySum = 0, inProgressCount = 0;
  let newToday = 0, newYesterday = 0, newThisWeek = 0, newLastWeek = 0;
  let rtipThis = 0, rtipLast = 0, ipfThis = 0, ipfLast = 0;
  const severityDist: Record<string, number> = { MINOR: 0, MODERATE: 0, HIGH: 0, CRITICAL: 0 };
  const dailyCounts = new Map<string, number>();
  const districtCounts = new Map<string, number>();

  for (const p of rows) {
    const reportedAt = new Date(p.reported_at as string);
    const inProgressAt = p.in_progress_at ? new Date(p.in_progress_at as string) : null;
    const fixedAt = p.fixed_at ? new Date(p.fixed_at as string) : null;
    const district = (p.district as string) ?? "Unknown";

    if (p.status === "reported") {
      activeCount++;
      activeSeveritySum += parseFloat(String(p.severity_score)) || 0;
      const lv = String(p.severity_level).toUpperCase();
      if (lv in severityDist) severityDist[lv]++;
      const ds = reportedAt.toISOString().split("T")[0];
      dailyCounts.set(ds, (dailyCounts.get(ds) ?? 0) + 1);
      districtCounts.set(district, (districtCounts.get(district) ?? 0) + 1);
    }
    if (p.status === "in_progress") inProgressCount++;
    if (reportedAt >= todayStart && reportedAt <= todayEnd) newToday++;
    if (reportedAt >= yesterdayStart && reportedAt <= yesterdayEnd) newYesterday++;
    if (reportedAt >= thisWeek.start && reportedAt <= thisWeek.end) newThisWeek++;
    if (reportedAt >= lastWeek.start && reportedAt <= lastWeek.end) newLastWeek++;
    if (p.status === "in_progress" && inProgressAt) {
      if (inProgressAt >= thisWeek.start && inProgressAt <= thisWeek.end) rtipThis++;
      if (inProgressAt >= lastWeek.start && inProgressAt <= lastWeek.end) rtipLast++;
    }
    if (p.status === "fixed" && fixedAt) {
      if (fixedAt >= thisWeek.start && fixedAt <= thisWeek.end) ipfThis++;
      if (fixedAt >= lastWeek.start && fixedAt <= lastWeek.end) ipfLast++;
    }
  }

  const last30 = Array.from({ length: 30 }, (_, i) => {
    const d = new Date(now.getTime() - (29 - i) * 86400000);
    const ds = d.toISOString().split("T")[0];
    return { date: ds, count: dailyCounts.get(ds) ?? 0 };
  });

  const topDistricts = [...districtCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([district, count]) => ({ district, count }));

  const sign = (n: number) => (n >= 0 ? `+${n}` : `${n}`);

  const recentCritical = await trinoFetch(`
    SELECT pothole_id, gps_lat, gps_lon, severity_level, status, district, reported_at
    FROM iceberg.city.potholes
    WHERE severity_level = 'CRITICAL' AND status = 'reported'
    ORDER BY reported_at DESC LIMIT 5
  `);

  return {
    activePotholes: {
      count: activeCount,
      trend: {
        today: { count: newToday, comparison: `${sign(newToday - newYesterday)} vs yesterday` },
        thisWeek: { count: newThisWeek, comparison: `${sign(newThisWeek - newLastWeek)} last week` },
      },
    },
    averageSeverity: activeCount > 0 ? Math.round((activeSeveritySum / activeCount) * 100) / 100 : 0,
    inProgress: inProgressCount,
    activePotholesLast30Days: last30,
    severityDistribution: severityDist,
    statusChanges: {
      reportedToInProgress: { thisWeek: rtipThis, comparison: `${sign(rtipThis - rtipLast)} last week` },
      inProgressToFixed: { thisWeek: ipfThis, comparison: `${sign(ipfThis - ipfLast)} last week` },
    },
    recentCritical: recentCritical as unknown as PotholeMarker[],
    topDistricts,
  };
}

export async function queryMapPotholes(
  lat?: number,
  lon?: number,
  radiusKm = 1
): Promise<PotholeMarker[]> {
  let sql: string;

  if (lat !== undefined && lon !== undefined) {
    const deg = radiusKm * 0.009;
    const cosLat = Math.cos((lat * Math.PI) / 180);
    sql = `
      SELECT pothole_id, gps_lat, gps_lon, severity_level, status, district, reported_at
      FROM iceberg.city.potholes
      WHERE gps_lat BETWEEN ${lat - deg} AND ${lat + deg}
        AND gps_lon BETWEEN ${lon - deg / cosLat} AND ${lon + deg / cosLat}
        AND status IN ('reported', 'in_progress')
      ORDER BY severity_score DESC LIMIT 30
    `;
  } else {
    sql = `
      SELECT pothole_id, gps_lat, gps_lon, severity_level, status, district, reported_at
      FROM iceberg.city.potholes
      WHERE status IN ('reported', 'in_progress')
      ORDER BY reported_at DESC LIMIT 30
    `;
  }

  const rows = await trinoFetch(sql);
  return rows as unknown as PotholeMarker[];
}

export async function queryPotholeDetail(id: string): Promise<PotholeDetail | null> {
  const safeId = id.replace(/'/g, "''");
  const rows = await trinoFetch(`
    SELECT pothole_id, first_event_id, reported_at, gps_lat, gps_lon,
           city, ward, district, street_name, road_id,
           depth_cm, surface_area_cm2, severity_score, severity_level,
           status, in_progress_at, fixed_at, last_updated_at,
           observation_count, raw_image_path, bev_image_path
    FROM iceberg.city.potholes
    WHERE pothole_id = '${safeId}'
  `);
  if (!rows.length) return null;
  const p = rows[0];
  return {
    pothole_id: p.pothole_id as string,
    first_event_id: p.first_event_id as string,
    gps_lat: p.gps_lat as number,
    gps_lon: p.gps_lon as number,
    city: p.city as string,
    ward: p.ward as string,
    district: p.district as string,
    street_name: p.street_name as string,
    road_id: p.road_id as string,
    depth_cm: p.depth_cm as number,
    surface_area_cm2: p.surface_area_cm2 as number,
    severity_score: p.severity_score as number,
    severity_level: p.severity_level as string,
    status: p.status as string,
    detected_at: p.reported_at as string,
    in_progress_at: p.in_progress_at as string | null,
    fixed_at: p.fixed_at as string | null,
    last_updated: p.last_updated_at as string,
    observation_count: p.observation_count as number,
    raw_image_path: p.raw_image_path as string | null,
    bev_image_path: p.bev_image_path as string | null,
  };
}
