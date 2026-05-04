import Redis from "ioredis";

let client: Redis | null = null;

function getClient(): Redis {
  if (!client) {
    client = new Redis({
      host: process.env.REDIS_HOST ?? "localhost",
      port: parseInt(process.env.REDIS_PORT ?? "6379"),
      connectTimeout: 500,
      commandTimeout: 1000,
      maxRetriesPerRequest: 1,
      retryStrategy: (times) => (times > 1 ? null : 200),
      lazyConnect: true,
    });
    client.on("error", (err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      console.warn("[Redis] unavailable:", message);
    });
  }
  return client;
}

export async function cached<T>(
  key: string,
  ttlSec: number,
  fn: () => Promise<T>
): Promise<T> {
  try {
    const redis = getClient();
    const hit = await redis.get(key);
    if (hit) return JSON.parse(hit) as T;
    const value = await fn();
    await redis.set(key, JSON.stringify(value), "EX", ttlSec);
    return value;
  } catch {
    return fn();
  }
}

const RECENT_EVENTS_KEY = "latency:events:recent";
const STAGE_STATS_PREFIX = "latency:stage:";

function percentile(arr: number[], p: number) {
  if (!arr.length) return 0;
  const idx = Math.ceil(arr.length * p) - 1;
  return arr[Math.max(0, Math.min(idx, arr.length - 1))];
}

async function getStageStats(redis: Redis, name: string) {
  const vals = await redis.zrange(`${STAGE_STATS_PREFIX}${name}`, 0, -1, "WITHSCORES");
  if (!vals.length) return null;
  const latencies: number[] = [];
  for (let i = 1; i < vals.length; i += 2) latencies.push(parseFloat(vals[i]));
  latencies.sort((a, b) => a - b);
  const sum = latencies.reduce((a, b) => a + b, 0);
  const n = latencies.length;
  return {
    count: n,
    avg_ms: Math.round((sum / n) * 100) / 100,
    min_ms: Math.round(latencies[0] * 100) / 100,
    max_ms: Math.round(latencies[n - 1] * 100) / 100,
    p50_ms: Math.round(percentile(latencies, 0.5) * 100) / 100,
    p95_ms: Math.round(percentile(latencies, 0.95) * 100) / 100,
    p99_ms: Math.round(percentile(latencies, 0.99) * 100) / 100,
  };
}

export async function getLatencyMetrics() {
  const redis = getClient();
  const stageNames = ["edge_to_kafka", "kafka_to_storage", "depth_estimation", "enrichment", "total"];
  const stages: Record<string, ReturnType<typeof getStageStats> extends Promise<infer T> ? T : never> = {};

  for (const name of stageNames) {
    const s = await getStageStats(redis, name);
    if (s) stages[name] = s;
  }

  const recentJson = await redis.lrange(RECENT_EVENTS_KEY, 0, 20);
  const recentEvents = recentJson.map((e) => {
    try { return JSON.parse(e); } catch { return null; }
  }).filter(Boolean);

  const total = stages["total"];
  return {
    stages,
    recentEvents,
    microservices: [
      { name: "Edge to Kafka", key: "edge_to_kafka", latency_ms: stages["edge_to_kafka"]?.avg_ms ?? 0, p95_ms: stages["edge_to_kafka"]?.p95_ms ?? 0, status: stages["edge_to_kafka"] ? "healthy" : "unknown" },
      { name: "Kafka to Storage", key: "kafka_to_storage", latency_ms: stages["kafka_to_storage"]?.avg_ms ?? 0, p95_ms: stages["kafka_to_storage"]?.p95_ms ?? 0, status: stages["kafka_to_storage"] ? "healthy" : "unknown" },
      { name: "Depth Estimation", key: "depth_estimation", latency_ms: stages["depth_estimation"]?.avg_ms ?? 0, p95_ms: stages["depth_estimation"]?.p95_ms ?? 0, status: stages["depth_estimation"] ? "healthy" : "unknown" },
      { name: "Final Enrichment", key: "enrichment", latency_ms: stages["enrichment"]?.avg_ms ?? 0, p95_ms: stages["enrichment"]?.p95_ms ?? 0, status: stages["enrichment"] ? "healthy" : "unknown" },
    ],
    total_pipeline: {
      avg_ms: total?.avg_ms ?? 0,
      p50_ms: total?.p50_ms ?? 0,
      p95_ms: total?.p95_ms ?? 0,
      p99_ms: total?.p99_ms ?? 0,
      min_ms: total?.min_ms ?? 0,
      max_ms: total?.max_ms ?? 0,
    },
  };
}

export async function pingRedis(): Promise<boolean> {
  try {
    const res = await getClient().ping();
    return res === "PONG";
  } catch {
    return false;
  }
}
