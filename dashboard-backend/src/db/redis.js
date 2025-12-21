import Redis from 'ioredis';
import config from '../config/index.js';

/**
 * Redis client singleton for caching
 */
class RedisClient {
  constructor() {
    this.client = null;
    this.isConnected = false;
  }

  /**
   * Initialize Redis connection
   */
  connect() {
    if (this.client) {
      return this.client;
    }

    this.client = new Redis({
      host: config.redis.host,
      port: config.redis.port,
      retryStrategy: (times) => {
        const delay = Math.min(times * 50, 2000);
        return delay;
      },
      maxRetriesPerRequest: 3,
    });

    this.client.on('connect', () => {
      this.isConnected = true;
      console.log(`[Redis] Connected to ${config.redis.host}:${config.redis.port}`);
    });

    this.client.on('error', (err) => {
      console.error('[Redis] Connection error:', err.message);
      this.isConnected = false;
    });

    this.client.on('close', () => {
      console.log('[Redis] Connection closed');
      this.isConnected = false;
    });

    return this.client;
  }

  /**
   * Get value from cache
   * @param {string} key - Cache key
   * @returns {Promise<any|null>} - Cached value or null
   */
  async get(key) {
    if (!this.client) {
      return null;
    }
    try {
      const value = await this.client.get(key);
      if (value) {
        return JSON.parse(value);
      }
      return null;
    } catch (error) {
      console.error('[Redis] Get error:', error.message);
      return null;
    }
  }

  /**
   * Set value in cache with TTL
   * @param {string} key - Cache key
   * @param {any} value - Value to cache
   * @param {number} ttl - Time to live in seconds
   */
  async set(key, value, ttl) {
    if (!this.client) {
      return;
    }
    try {
      await this.client.setex(key, ttl, JSON.stringify(value));
    } catch (error) {
      console.error('[Redis] Set error:', error.message);
    }
  }

  /**
   * Delete a key from cache
   * @param {string} key - Cache key
   */
  async del(key) {
    if (!this.client) {
      return;
    }
    try {
      await this.client.del(key);
    } catch (error) {
      console.error('[Redis] Delete error:', error.message);
    }
  }

  /**
   * Delete keys matching a pattern
   * @param {string} pattern - Key pattern (e.g., "mapview:*")
   */
  async delPattern(pattern) {
    if (!this.client) {
      return;
    }
    try {
      const keys = await this.client.keys(pattern);
      if (keys.length > 0) {
        await this.client.del(...keys);
      }
    } catch (error) {
      console.error('[Redis] Delete pattern error:', error.message);
    }
  }

  /**
   * Check if Redis is healthy
   * @returns {Promise<boolean>}
   */
  async healthCheck() {
    if (!this.client) {
      return false;
    }
    try {
      const pong = await this.client.ping();
      return pong === 'PONG';
    } catch (error) {
      console.error('[Redis] Health check failed:', error.message);
      return false;
    }
  }

  /**
   * Get connection status
   */
  getStatus() {
    return {
      connected: this.isConnected,
      host: config.redis.host,
      port: config.redis.port,
    };
  }

  /**
   * Close Redis connection
   */
  async close() {
    if (this.client) {
      await this.client.quit();
      this.client = null;
      this.isConnected = false;
    }
  }
}

// Export singleton instance
const redisClient = new RedisClient();
export default redisClient;
