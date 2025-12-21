import trino from '../db/trino.js';
import redis from '../db/redis.js';
import config from '../config/index.js';

/**
 * Generate cache key for pothole detail
 * @param {string} potholeId 
 * @returns {string}
 */
function getCacheKey(potholeId) {
  return `api:pothole:${potholeId}`;
}

/**
 * GET /api/v1/pothole/:pothole_id
 * Returns complete information for a specific pothole
 */
export async function getPotholeById(req, res) {
  try {
    const { pothole_id } = req.params;

    if (!pothole_id) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'pothole_id parameter is required',
      });
    }

    // Check cache first
    const cacheKey = getCacheKey(pothole_id);
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Query pothole details
    const query = `
      SELECT 
        pothole_id,
        first_event_id,
        reported_at,
        gps_lat,
        gps_lon,
        geom_h3,
        city,
        ward,
        district,
        street_name,
        road_id,
        depth_cm,
        surface_area_cm2,
        severity_score,
        severity_level,
        pothole_polygon,
        status,
        in_progress_at,
        fixed_at,
        last_updated_at,
        observation_count
      FROM iceberg.city.potholes
      WHERE pothole_id = '${pothole_id.replace(/'/g, "''")}'
    `;

    const results = await trino.query(query);

    if (results.length === 0) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Pothole with id '${pothole_id}' not found`,
      });
    }

    const pothole = results[0];

    // Format response
    const response = {
      pothole_id: pothole.pothole_id,
      first_event_id: pothole.first_event_id,
      gps_lat: pothole.gps_lat,
      gps_lon: pothole.gps_lon,
      geom_h3: pothole.geom_h3,
      city: pothole.city,
      ward: pothole.ward,
      district: pothole.district,
      street_name: pothole.street_name,
      road_id: pothole.road_id,
      depth_cm: pothole.depth_cm,
      surface_area_cm2: pothole.surface_area_cm2,
      severity_score: pothole.severity_score,
      severity_level: pothole.severity_level,
      pothole_polygon: pothole.pothole_polygon,
      status: pothole.status,
      detected_at: pothole.reported_at,
      in_progress_at: pothole.in_progress_at,
      fixed_at: pothole.fixed_at,
      last_updated: pothole.last_updated_at,
      observation_count: pothole.observation_count,
    };

    // Cache the response
    await redis.set(cacheKey, response, config.cacheTTL.potholeDetail);

    return res.json(response);
  } catch (error) {
    console.error('[PotholeDetail] Error:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to fetch pothole details',
      details: config.nodeEnv === 'development' ? error.message : undefined,
    });
  }
}
