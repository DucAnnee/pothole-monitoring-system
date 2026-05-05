import { getLatencyMetrics, pingRedis } from "./redis.server";
import pg from "pg";
import * as Minio from "minio";

export interface ServiceStatus {
  name: string;
  icon: string;
  status: "healthy" | "warning" | "unknown";
  uptime: number;
  latency_ms: number;
  last_check: string;
}

export interface EdgeDevice {
  vehicle_id: string;
  device_id: string;
  gps_ok: boolean;
  camera_ok: boolean;
  model_version: string;
  last_upload: string;
  battery_pct: number;
  storage_pct: number;
  connection: "online" | "offline" | "degraded";
  pending_count: number;
  health: "healthy" | "warning" | "critical";
}

export interface HealthData {
  kafka: {
    brokers: Array<{ name: string; ip: string; last_heartbeat: string; active: boolean }>;
  };
  minio: {
    used_gb: number;
    total_gb: number;
    type_dist: Record<string, number>;
  };
  polaris: {
    catalogs: number;
    tables: number;
    queries_per_min: number;
  };
  microservices: ServiceStatus[];
  edge_devices: EdgeDevice[];
  latency: Awaited<ReturnType<typeof getLatencyMetrics>>;
  redis_ok: boolean;
}

async function probeKafka() {
  const url = process.env.KAFKA_UI_URL ?? "http://localhost:8080";
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(2000) });
    return { status: res.ok ? ("healthy" as const) : ("warning" as const), detail: `${res.status}` };
  } catch {
    return { status: "unknown" as const, detail: "unreachable" };
  }
}

async function probeMinio() {
  try {
    const client = new Minio.Client({
      endPoint: process.env.MINIO_HOST ?? "localhost",
      port: Number(process.env.MINIO_PORT ?? "9000"),
      useSSL: false,
      accessKey: process.env.MINIO_ACCESS_KEY ?? "minioadmin",
      secretKey: process.env.MINIO_SECRET_KEY ?? "minioadmin",
    });
    await client.bucketExists(process.env.MINIO_BUCKET ?? "warehouse");
    return { status: "healthy" as const };
  } catch {
    return { status: "unknown" as const };
  }
}

async function probePostgis() {
  const client = new pg.Client({
    host: process.env.POSTGIS_HOST ?? "localhost",
    port: Number(process.env.POSTGIS_PORT ?? "5437"),
    user: process.env.POSTGIS_USER ?? "serving",
    password: process.env.POSTGIS_PASSWORD ?? "servingpassword",
    database: process.env.POSTGIS_DATABASE ?? "postgis_serving",
  });
  try {
    await client.connect();
    await client.query("SELECT 1 FROM serving.current_road_defects LIMIT 1");
    return { status: "healthy" as const };
  } catch {
    return { status: "unknown" as const };
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function fetchHealthData(): Promise<HealthData> {
  const [latency, redis_ok, kafkaProbe, _minioProbe, postgisProbe] = await Promise.all([
    getLatencyMetrics().catch(() => null),
    pingRedis(),
    probeKafka(),
    probeMinio(),
    probePostgis(),
  ]);

  const now = new Date().toISOString();

  const microservices: ServiceStatus[] = (latency?.microservices ?? []).map((m) => ({
    name: m.name,
    icon: m.key,
    status: m.status === "healthy" ? "healthy" : m.status === "unknown" ? "unknown" : "warning",
    uptime: 99.8,
    latency_ms: m.latency_ms,
    last_check: now,
  }));

  if (!microservices.length) {
    const fallbacks = [
      "Edge to Kafka", "Kafka to Storage", "Depth Estimation",
      "Final Enrichment", "ETL Service", "Severity Calculator",
    ];
    fallbacks.forEach((name) =>
      microservices.push({ name, icon: name, status: "unknown", uptime: 0, latency_ms: 0, last_check: now })
    );
  }

  microservices.push({
    name: "PostGIS Projection",
    icon: "postgis",
    status: postgisProbe.status,
    uptime: 0,
    latency_ms: 0,
    last_check: now,
  });

  return {
    kafka: {
      brokers: [
        { name: "kafka-ui", ip: kafkaProbe.detail ?? "", last_heartbeat: now, active: kafkaProbe.status === "healthy" },
      ],
    },
    minio: {
      used_gb: 42.3,
      total_gb: 200,
      type_dist: { raw_images: 60, bev_images: 35, other: 5 },
    },
    polaris: {
      catalogs: 2,
      tables: 4,
      queries_per_min: 12,
    },
    microservices,
    edge_devices: [],
    latency: latency ?? {
      stages: {},
      recentEvents: [],
      microservices: [],
      total_pipeline: { avg_ms: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0, min_ms: 0, max_ms: 0 },
    },
    redis_ok,
  };
}
