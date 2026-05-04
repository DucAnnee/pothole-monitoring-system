import { getLatencyMetrics, pingRedis } from "./redis.server";

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

export async function fetchHealthData(): Promise<HealthData> {
  const [latency, redis_ok] = await Promise.all([
    getLatencyMetrics().catch(() => null),
    pingRedis(),
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

  return {
    kafka: {
      brokers: [
        { name: "broker-1", ip: "10.0.0.1", last_heartbeat: now, active: true },
        { name: "broker-2", ip: "10.0.0.2", last_heartbeat: now, active: true },
        { name: "broker-3", ip: "10.0.0.3", last_heartbeat: now, active: true },
        { name: "controller-1", ip: "10.0.0.4", last_heartbeat: now, active: true },
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
    edge_devices: [
      {
        vehicle_id: "BUS-001",
        device_id: "RPi-4B-A1",
        gps_ok: true,
        camera_ok: true,
        model_version: "YOLOv8-seg-v2.1",
        last_upload: now,
        battery_pct: 87,
        storage_pct: 42,
        connection: "online",
        pending_count: 0,
        health: "healthy",
      },
      {
        vehicle_id: "BUS-002",
        device_id: "RPi-4B-A2",
        gps_ok: true,
        camera_ok: false,
        model_version: "YOLOv8-seg-v2.1",
        last_upload: new Date(Date.now() - 300000).toISOString(),
        battery_pct: 62,
        storage_pct: 71,
        connection: "degraded",
        pending_count: 12,
        health: "warning",
      },
      {
        vehicle_id: "BUS-003",
        device_id: "RPi-4B-A3",
        gps_ok: false,
        camera_ok: false,
        model_version: "YOLOv8-seg-v2.0",
        last_upload: new Date(Date.now() - 3600000).toISOString(),
        battery_pct: 18,
        storage_pct: 91,
        connection: "offline",
        pending_count: 87,
        health: "critical",
      },
    ],
    latency: latency ?? {
      stages: {},
      recentEvents: [],
      microservices: [],
      total_pipeline: { avg_ms: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0, min_ms: 0, max_ms: 0 },
    },
    redis_ok,
  };
}
