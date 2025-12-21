import express from 'express';
import cors from 'cors';
import morgan from 'morgan';

import config from './config/index.js';
import routes from './routes/index.js';
import { errorHandler, notFoundHandler } from './middleware/errorHandler.js';
import trino from './db/trino.js';
import redis from './db/redis.js';

const app = express();

// =============================================================================
// Middleware
// =============================================================================

// CORS configuration
app.use(cors({
  origin: config.cors.origin,
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// Request logging
app.use(morgan('combined'));

// Parse JSON bodies
app.use(express.json());

// =============================================================================
// Routes
// =============================================================================

// API routes
app.use('/api/v1', routes);

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    name: 'Pothole Monitoring Dashboard API',
    version: '1.0.0',
    endpoints: {
      health: '/api/v1/health',
      summary: '/api/v1/summary',
      mapView: '/api/v1/map-view?lat={lat}&lon={lon}',
      potholeDetail: '/api/v1/pothole/{pothole_id}',
    },
  });
});

// =============================================================================
// Error Handling
// =============================================================================

// 404 handler
app.use(notFoundHandler);

// Global error handler
app.use(errorHandler);

// =============================================================================
// Server Startup
// =============================================================================

async function startServer() {
  try {
    // Initialize database connections
    console.log('Initializing connections...');
    
    // Initialize Trino client
    trino.connect();
    
    // Initialize Redis client
    redis.connect();

    // Start HTTP server
    app.listen(config.port, () => {
      console.log('='.repeat(60));
      console.log('POTHOLE MONITORING DASHBOARD BACKEND');
      console.log('='.repeat(60));
      console.log(`Environment: ${config.nodeEnv}`);
      console.log(`Server running on http://localhost:${config.port}`);
      console.log(`CORS enabled for: ${config.cors.origin}`);
      console.log('='.repeat(60));
      console.log('Available endpoints:');
      console.log(`  GET /api/v1/health`);
      console.log(`  GET /api/v1/summary`);
      console.log(`  GET /api/v1/map-view?lat={lat}&lon={lon}`);
      console.log(`  GET /api/v1/pothole/{pothole_id}`);
      console.log('='.repeat(60));
    });

    // Graceful shutdown handler
    const shutdown = async (signal) => {
      console.log(`\n[${signal}] Shutting down gracefully...`);
      
      try {
        await redis.close();
        console.log('[Shutdown] Redis connection closed');
      } catch (err) {
        console.error('[Shutdown] Error closing Redis:', err);
      }

      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
