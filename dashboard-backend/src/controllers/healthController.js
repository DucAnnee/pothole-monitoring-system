import trino from '../db/trino.js';
import redis from '../db/redis.js';
import config from '../config/index.js';

const CACHE_KEY = 'api:health';

/**
 * GET /api/v1/health
 * Returns service health status
 */
export async function getHealth(req, res) {
  try {
    // Check cache (to avoid hammering dependencies on frequent health checks)
    const cached = await redis.get(CACHE_KEY);
    if (cached) {
      return res.status(cached.status === 'healthy' ? 200 : 503).json(cached);
    }

    const trinoHealthy = await trino.healthCheck();
    const redisHealthy = await redis.healthCheck();

    const allHealthy = trinoHealthy && redisHealthy;

    const response = {
      status: allHealthy ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
      services: {
        trino: {
          status: trinoHealthy ? 'healthy' : 'unhealthy',
          ...trino.getStatus(),
        },
        redis: {
          status: redisHealthy ? 'healthy' : 'unhealthy',
          ...redis.getStatus(),
        },
      },
    };

    // Cache health response briefly
    await redis.set(CACHE_KEY, response, config.cacheTTL.health);

    return res.status(allHealthy ? 200 : 503).json(response);
  } catch (error) {
    console.error('[Health] Error:', error);
    return res.status(503).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      error: error.message,
    });
  }
}
