import { Router } from 'express';
import { getSummary } from '../controllers/summaryController.js';
import { getMapView } from '../controllers/mapViewController.js';
import { getPotholeById } from '../controllers/potholeController.js';
import { getHealth } from '../controllers/healthController.js';
import { getPresignedUrl, proxyImage } from '../controllers/imageController.js';
import { getLatencyStats, getRecentLatency } from '../controllers/latencyController.js';

const router = Router();

// Health check endpoint
router.get('/health', getHealth);

// Dashboard summary
router.get('/summary', getSummary);

// Map view - potholes near a location
router.get('/map-view', getMapView);

// Individual pothole details
router.get('/pothole/:pothole_id', getPotholeById);

// Image endpoints
router.get('/image/presigned', getPresignedUrl);
router.get('/image/proxy', proxyImage);

// Latency metrics endpoints
router.get('/latency', getLatencyStats);
router.get('/latency/recent', getRecentLatency);

export default router;
