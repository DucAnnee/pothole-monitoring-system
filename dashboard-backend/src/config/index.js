import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

const config = {
  // Server
  port: parseInt(process.env.PORT || '3002', 10),
  nodeEnv: process.env.NODE_ENV || 'development',

  // Trino (for querying Iceberg tables)
  trino: {
    host: process.env.TRINO_HOST || 'localhost',
    port: parseInt(process.env.TRINO_PORT || '8081', 10),
    user: process.env.TRINO_USER || 'trino',
    catalog: process.env.TRINO_CATALOG || 'iceberg',
    schema: process.env.TRINO_SCHEMA || 'city',
  },

  // Redis
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
  },

  // CORS
  cors: {
    origin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  },

  // Cache TTLs (in seconds)
  cacheTTL: {
    summary: 60,        // 1 minute - dashboard summary updates frequently
    mapView: 30,        // 30 seconds - location-based queries
    potholeDetail: 120, // 2 minutes - individual pothole details
    health: 10,         // 10 seconds - health check
  },
};

export default config;
