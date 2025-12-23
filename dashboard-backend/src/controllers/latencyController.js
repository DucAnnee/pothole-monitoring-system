/**
 * Latency Controller
 * 
 * Provides API endpoints for pipeline latency metrics.
 * Reads latency data from Redis that was recorded by the enrichment service.
 */

import Redis from 'ioredis';

// Redis configuration
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379');

// Redis keys (must match latency_tracker.py)
const RECENT_EVENTS_KEY = "latency:events:recent";
const STATS_KEY = "latency:stats";
const STAGE_STATS_PREFIX = "latency:stage:";

let redisClient = null;

/**
 * Get or create Redis client
 */
function getRedisClient() {
    if (redisClient) {
        return redisClient;
    }
    
    redisClient = new Redis({
        host: REDIS_HOST,
        port: REDIS_PORT,
        retryStrategy: (times) => {
            if (times > 3) return null;
            return Math.min(times * 200, 2000);
        }
    });
    
    redisClient.on('error', (err) => {
        console.error('[LATENCY] Redis error:', err.message);
    });
    
    return redisClient;
}

/**
 * Calculate percentile from sorted array
 */
function percentile(arr, p) {
    if (arr.length === 0) return 0;
    const index = Math.ceil(arr.length * p) - 1;
    return arr[Math.max(0, Math.min(index, arr.length - 1))];
}

/**
 * Get latency statistics for a specific stage
 */
async function getStageStats(client, stageName) {
    const stageKey = `${STAGE_STATS_PREFIX}${stageName}`;
    
    // Get all values from sorted set with scores
    const values = await client.zrange(stageKey, 0, -1, 'WITHSCORES');
    
    if (!values || values.length === 0) {
        return null;
    }
    
    // ioredis returns flat array [member, score, member, score, ...]
    const latencies = [];
    for (let i = 1; i < values.length; i += 2) {
        latencies.push(parseFloat(values[i]));
    }
    latencies.sort((a, b) => a - b);
    
    const n = latencies.length;
    if (n === 0) return null;
    
    const sum = latencies.reduce((a, b) => a + b, 0);
    
    return {
        count: n,
        avg_ms: Math.round((sum / n) * 100) / 100,
        min_ms: Math.round(Math.min(...latencies) * 100) / 100,
        max_ms: Math.round(Math.max(...latencies) * 100) / 100,
        p50_ms: Math.round(percentile(latencies, 0.5) * 100) / 100,
        p95_ms: Math.round(percentile(latencies, 0.95) * 100) / 100,
        p99_ms: Math.round(percentile(latencies, 0.99) * 100) / 100,
    };
}

/**
 * GET /api/v1/latency
 * 
 * Returns pipeline latency statistics including:
 * - Per-stage latency metrics (avg, p50, p95, p99)
 * - Recent latency events
 * - Summary statistics
 */
export async function getLatencyStats(req, res) {
    try {
        const client = getRedisClient();
        
        const result = {
            timestamp: new Date().toISOString(),
            stages: {},
            recent_events: [],
            summary: {},
        };
        
        // Get stage statistics
        const stageNames = [
            "edge_to_kafka",
            "kafka_to_storage", 
            "depth_estimation",
            "enrichment",
            "total"
        ];
        
        for (const stageName of stageNames) {
            const stats = await getStageStats(client, stageName);
            if (stats) {
                result.stages[stageName] = stats;
            }
        }
        
        // Get recent events
        const recentJson = await client.lrange(RECENT_EVENTS_KEY, 0, 20);
        result.recent_events = recentJson.map(e => {
            try {
                return JSON.parse(e);
            } catch {
                return null;
            }
        }).filter(e => e !== null);
        
        // Get summary stats
        const summaryData = await client.hgetall(STATS_KEY);
        if (summaryData && Object.keys(summaryData).length > 0) {
            result.summary = {
                last_updated: summaryData.last_updated,
                last_event_id: summaryData.last_event_id,
                last_total_ms: parseInt(summaryData.last_total_ms || '0'),
                source: 'redis',
            };
        }
        
        // Calculate derived metrics for frontend
        if (result.stages.total) {
            result.summary.avg_total_ms = result.stages.total.avg_ms;
            result.summary.p95_total_ms = result.stages.total.p95_ms;
        }
        
        // Map to frontend-friendly format
        result.microservices = [
            {
                name: "Edge to Kafka",
                key: "edge_to_kafka",
                latency_ms: result.stages.edge_to_kafka?.avg_ms || 0,
                p95_ms: result.stages.edge_to_kafka?.p95_ms || 0,
                status: result.stages.edge_to_kafka ? "healthy" : "unknown",
            },
            {
                name: "Kafka to Storage",
                key: "kafka_to_storage",
                latency_ms: result.stages.kafka_to_storage?.avg_ms || 0,
                p95_ms: result.stages.kafka_to_storage?.p95_ms || 0,
                status: result.stages.kafka_to_storage ? "healthy" : "unknown",
            },
            {
                name: "Depth Estimation",
                key: "depth_estimation",
                latency_ms: result.stages.depth_estimation?.avg_ms || 0,
                p95_ms: result.stages.depth_estimation?.p95_ms || 0,
                status: result.stages.depth_estimation ? "healthy" : "unknown",
            },
            {
                name: "Final Enrichment",
                key: "enrichment",
                latency_ms: result.stages.enrichment?.avg_ms || 0,
                p95_ms: result.stages.enrichment?.p95_ms || 0,
                status: result.stages.enrichment ? "healthy" : "unknown",
            },
        ];
        
        result.total_pipeline = {
            avg_ms: result.stages.total?.avg_ms || 0,
            p50_ms: result.stages.total?.p50_ms || 0,
            p95_ms: result.stages.total?.p95_ms || 0,
            p99_ms: result.stages.total?.p99_ms || 0,
            min_ms: result.stages.total?.min_ms || 0,
            max_ms: result.stages.total?.max_ms || 0,
        };
        
        res.json(result);
        
    } catch (error) {
        console.error('[LATENCY] Error fetching stats:', error);
        
        // Return empty response with error flag
        res.status(200).json({
            timestamp: new Date().toISOString(),
            error: error.message,
            stages: {},
            recent_events: [],
            summary: { source: 'error' },
            microservices: [
                { name: "Edge to Kafka", key: "edge_to_kafka", latency_ms: 0, status: "unknown" },
                { name: "Kafka to Storage", key: "kafka_to_storage", latency_ms: 0, status: "unknown" },
                { name: "Depth Estimation", key: "depth_estimation", latency_ms: 0, status: "unknown" },
                { name: "Final Enrichment", key: "enrichment", latency_ms: 0, status: "unknown" },
            ],
            total_pipeline: { avg_ms: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0 },
        });
    }
}

/**
 * GET /api/v1/latency/recent
 * 
 * Returns only the most recent latency events (for real-time monitoring)
 */
export async function getRecentLatency(req, res) {
    try {
        const count = Math.min(parseInt(req.query.count || '10'), 100);
        const client = getRedisClient();
        
        const recentJson = await client.lrange(RECENT_EVENTS_KEY, 0, count - 1);
        const events = recentJson.map(e => {
            try {
                return JSON.parse(e);
            } catch {
                return null;
            }
        }).filter(e => e !== null);
        
        res.json({
            timestamp: new Date().toISOString(),
            count: events.length,
            events: events,
        });
        
    } catch (error) {
        console.error('[LATENCY] Error fetching recent:', error);
        res.status(200).json({
            timestamp: new Date().toISOString(),
            count: 0,
            events: [],
            error: error.message,
        });
    }
}
