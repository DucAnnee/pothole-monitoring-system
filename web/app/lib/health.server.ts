import { getLatencyMetrics, pingRedis } from "./redis.server";
import { promQuery } from "./prometheus.server";
import pg from "pg";

export interface ServiceStatus {
  name: string;
  icon: string;
  status: "healthy" | "warning" | "unknown";
  uptime: number | null;
  latency_ms: number;
  last_check: string;
}

export interface EdgeDevice {
  vehicle_id: string;
  device_id: string;
  gps_ok: boolean | null;
  camera_ok: boolean | null;
  model_version: string;
  last_upload: string;
  battery_pct: number | null;
  storage_pct: number | null;
  connection: "online" | "offline" | "degraded";
  pending_count: number | null;
  health: "healthy" | "warning" | "critical";
}

export interface HealthData {
  kafka: {
    brokers: Array<{ name: string; ip: string; last_heartbeat: string; active: boolean }>;
  };
  minio: {
    used_gb: number | null;
    total_gb: number | null;
    type_dist: Record<string, number>;
  };
  polaris: {
    catalogs: number | null;
    tables: number | null;
    queries_per_min: number | null;
  };
  microservices: ServiceStatus[];
  edge_devices: EdgeDevice[];
  latency: Awaited<ReturnType<typeof getLatencyMetrics>>;
  redis_ok: boolean;
}

function pgClient() {
  return new pg.Client({
    host: process.env.POSTGIS_HOST ?? "localhost",
    port: Number(process.env.POSTGIS_PORT ?? "5437"),
    user: process.env.POSTGIS_USER ?? "serving",
    password: process.env.POSTGIS_PASSWORD ?? "servingpassword",
    database: process.env.POSTGIS_DATABASE ?? "postgis_serving",
  });
}

async function probePolaris(): Promise<{ catalogs: number | null; tables: number | null }> {
  try {
    const base = process.env.POLARIS_URL ?? "http://localhost:8181";
    const realm = "POLARIS";
    const tok = await fetch(`${base}/api/catalog/v1/oauth/tokens`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded", "Polaris-Realm": realm },
      body: "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL",
      signal: AbortSignal.timeout(2500),
    })
      .then((r) => r.json())
      .then((j) => j.access_token)
      .catch(() => null);
    if (!tok) return { catalogs: null, tables: null };
    const catalogs = await fetch(`${base}/api/management/v1/catalogs`, {
      headers: { Authorization: `Bearer ${tok}`, "Polaris-Realm": realm },
      signal: AbortSignal.timeout(2500),
    })
      .then((r) => r.json())
      .then((j) => (Array.isArray(j.catalogs) ? j.catalogs.length : null))
      .catch(() => null);
    return { catalogs, tables: null };
  } catch {
    return { catalogs: null, tables: null };
  }
}

async function fetchEdgeDevices(): Promise<EdgeDevice[]> {
  const client = pgClient();
  try {
    await client.connect();
    const { rows } = await client.query(
      `SELECT device_id, vehicle_id, status, last_seen_at
       FROM serving.device_registry ORDER BY last_seen_at DESC NULLS LAST LIMIT 20`
    );
    return rows.map((r) => {
      const recent = r.last_seen_at && Date.now() - new Date(r.last_seen_at).getTime() < 15 * 60_000;
      return {
        vehicle_id: r.vehicle_id ?? "—",
        device_id: r.device_id,
        gps_ok: null,
        camera_ok: null,
        model_version: "—",
        last_upload: r.last_seen_at ? new Date(r.last_seen_at).toISOString() : "",
        battery_pct: null,
        storage_pct: null,
        connection: recent ? "online" : "offline",
        pending_count: null,
        health: r.status === "active" ? "healthy" : "warning",
      } as EdgeDevice;
    });
  } catch {
    return [];
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function fetchHealthData(): Promise<HealthData> {
  const [latency, redis_ok, brokerCount, minioUsed, minioTotal, postgisUp, polaris, edge_devices] =
    await Promise.all([
      getLatencyMetrics().catch(() => null),
      pingRedis(),
      promQuery("kafka_brokers"),
      promQuery("minio_cluster_usage_total_bytes"),
      promQuery("minio_cluster_capacity_raw_total_bytes"),
      promQuery('pg_up{job="postgis"}'),
      probePolaris(),
      fetchEdgeDevices(),
    ]);

  const now = new Date().toISOString();

  const microservices: ServiceStatus[] = (latency?.microservices ?? []).map((m) => ({
    name: m.name,
    icon: m.key,
    status: m.status === "healthy" ? "healthy" : m.status === "unknown" ? "unknown" : "warning",
    uptime: null,
    latency_ms: m.latency_ms,
    last_check: now,
  }));

  microservices.push({
    name: "PostGIS Projection",
    icon: "postgis",
    status: postgisUp === 1 ? "healthy" : "unknown",
    uptime: null,
    latency_ms: 0,
    last_check: now,
  });

  const brokers = Array.from({ length: brokerCount ?? 0 }, (_, i) => ({
    name: `kafka-${i + 1}`,
    ip: "",
    last_heartbeat: now,
    active: true,
  }));

  return {
    kafka: { brokers },
    minio: {
      used_gb: minioUsed != null ? minioUsed / 1e9 : null,
      total_gb: minioTotal != null ? minioTotal / 1e9 : null,
      type_dist: {},
    },
    polaris: { catalogs: polaris.catalogs, tables: polaris.tables, queries_per_min: null },
    microservices,
    edge_devices,
    latency: latency ?? {
      stages: {},
      recentEvents: [],
      microservices: [],
      total_pipeline: { avg_ms: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0, min_ms: 0, max_ms: 0 },
    },
    redis_ok,
  };
}
