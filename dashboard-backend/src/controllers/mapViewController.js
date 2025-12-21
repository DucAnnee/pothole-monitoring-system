import trino from '../db/trino.js';
import redis from '../db/redis.js';
import config from '../config/index.js';

/**
 * Calculate bounding box for simple lat/lon filtering
 * 1 degree of latitude ≈ 111 km
 * 1 degree of longitude ≈ 111 km * cos(latitude)
 * For 1km radius, we need approximately 0.009 degrees
 */
const KM_TO_DEGREE = 0.9; // Approximate conversion for 1km

/**
 * Validate latitude
 * @param {number} lat 
 * @returns {boolean}
 */
function isValidLat(lat) {
  return typeof lat === 'number' && lat >= -90 && lat <= 90;
}

/**
 * Validate longitude
 * @param {number} lon 
 * @returns {boolean}
 */
function isValidLon(lon) {
  return typeof lon === 'number' && lon >= -180 && lon <= 180;
}

/**
 * Generate cache key for map view
 * @param {number} lat 
 * @param {number} lon 
 * @returns {string}
 */
function getCacheKey(lat, lon) {
  // Round to 3 decimal places for cache key (about 100m precision)
  const roundedLat = Math.round(lat * 1000) / 1000;
  const roundedLon = Math.round(lon * 1000) / 1000;
  return `api:mapview:${roundedLat}:${roundedLon}`;
}

/**
 * GET /api/v1/map-view
 * Returns potholes near a specific location, or top 30 most recent if no coordinates provided
 * 
 * Query Parameters:
 * - lat (optional): Latitude coordinate
 * - lon (optional): Longitude coordinate
 */
export async function getMapView(req, res) {
  try {
    const { lat, lon } = req.query;

    // Parse coordinates
    const latitude = parseFloat(lat);
    const longitude = parseFloat(lon);

    // If no coordinates provided, return top 30 most recent active potholes
    if (isNaN(latitude) || isNaN(longitude)) {
      const recentCacheKey = 'api:mapview:recent';
      const cached = await redis.get(recentCacheKey);
      if (cached) {
        return res.json(cached);
      }

      const recentQuery = `
        SELECT 
          pothole_id,
          gps_lat,
          gps_lon,
          severity_level,
          status
        FROM iceberg.city.potholes
        WHERE status IN ('reported', 'in_progress')
        ORDER BY reported_at DESC
        LIMIT 30
      `;

      const results = await trino.query(recentQuery);

      const response = {
        potholes: results.map(row => ({
          pothole_id: row.pothole_id,
          gps_lat: row.gps_lat,
          gps_lon: row.gps_lon,
          severity_level: row.severity_level,
          status: row.status,
        })),
      };

      await redis.set(recentCacheKey, response, config.cacheTTL.mapView);
      return res.json(response);
    }

    // Validate coordinates if provided
    if (!isValidLat(latitude)) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'lat must be between -90 and 90',
      });
    }

    if (!isValidLon(longitude)) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'lon must be between -180 and 180',
      });
    }

    // Check cache first
    const cacheKey = getCacheKey(latitude, longitude);
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Calculate bounding box (1km radius)
    const latDelta = KM_TO_DEGREE;
    const lonDelta = KM_TO_DEGREE / Math.cos(latitude * Math.PI / 180);

    const minLat = latitude - latDelta;
    const maxLat = latitude + latDelta;
    const minLon = longitude - lonDelta;
    const maxLon = longitude + lonDelta;

    // Query potholes within bounding box
    const query = `
      SELECT 
        pothole_id,
        gps_lat,
        gps_lon,
        severity_level,
        status
      FROM iceberg.city.potholes
      WHERE gps_lat >= ${minLat}
        AND gps_lat <= ${maxLat}
        AND gps_lon >= ${minLon}
        AND gps_lon <= ${maxLon}
        AND status IN ('reported', 'in_progress')
      ORDER BY severity_score DESC
      LIMIT 30
    `;

    const results = await trino.query(query);

    const response = {
      potholes: results.map(row => ({
        pothole_id: row.pothole_id,
        gps_lat: row.gps_lat,
        gps_lon: row.gps_lon,
        severity_level: row.severity_level,
        status: row.status,
      })),
    };

    // Cache the response
    await redis.set(cacheKey, response, config.cacheTTL.mapView);

    return res.json(response);
  } catch (error) {
    console.error('[MapView] Error:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to fetch potholes for map view',
      details: config.nodeEnv === 'development' ? error.message : undefined,
    });
  }
}
