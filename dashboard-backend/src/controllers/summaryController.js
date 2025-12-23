import trino from '../db/trino.js';
import redis from '../db/redis.js';
import config from '../config/index.js';

const CACHE_KEY = 'api:summary:v2';
const CACHE_TTL_SECONDS = 300; // 5 minutes

/**
 * Get the start and end of a calendar week (Monday-Sunday) for a given date
 * @param {Date} date - Reference date
 * @returns {{ start: Date, end: Date }}
 */
function getWeekBounds(date) {
  const d = new Date(date);
  const day = d.getUTCDay();
  const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1); // Adjust for Monday start
  
  const start = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), diff, 0, 0, 0, 0));
  const end = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), diff + 6, 23, 59, 59, 999));
  
  return { start, end };
}

/**
 * Format date as YYYY-MM-DD
 * @param {Date} date 
 * @returns {string}
 */
function formatDate(date) {
  return date.toISOString().split('T')[0];
}

/**
 * Format timestamp for Trino SQL (YYYY-MM-DD HH:MM:SS.sss)
 * Trino doesn't accept ISO 8601 format with 'T' and 'Z'
 * @param {Date} date 
 * @returns {string}
 */
function formatTrinoTimestamp(date) {
  return date.toISOString().replace('T', ' ').replace('Z', '');
}

/**
 * Fetch all pothole data needed for summary calculations in ONE query.
 * This reduces 12+ round trips to Trino/MinIO down to just 1.
 * @param {Date} thirtyDaysAgo - Cutoff for historical data
 * @returns {Promise<Array>} - All pothole records needed for aggregation
 */
async function fetchAllPotholesForSummary(thirtyDaysAgo) {
  const query = `
    SELECT 
      pothole_id,
      status,
      severity_score,
      severity_level,
      reported_at,
      in_progress_at,
      fixed_at
    FROM iceberg.city.potholes
    WHERE reported_at >= TIMESTAMP '${formatTrinoTimestamp(thirtyDaysAgo)}'
       OR status IN ('reported', 'in_progress')
  `;
  
  return await trino.query(query);
}

/**
 * Calculate all summary metrics from in-memory data.
 * Performs all aggregations in JavaScript instead of multiple SQL queries.
 * @param {Array} potholes - All pothole records
 * @param {Object} dateRanges - Pre-calculated date ranges
 * @returns {Object} - All calculated summary metrics
 */
function calculateSummaryMetrics(potholes, dateRanges) {
  const { 
    now, todayStart, todayEnd, yesterdayStart, yesterdayEnd,
    thisWeek, lastWeek 
  } = dateRanges;

  // Initialize counters
  let activePotholesCount = 0;
  let activeSeveritySum = 0;
  let newToday = 0;
  let newYesterday = 0;
  let newThisWeek = 0;
  let newLastWeek = 0;
  let reportedToInProgressThisWeek = 0;
  let reportedToInProgressLastWeek = 0;
  let inProgressToFixedThisWeek = 0;
  let inProgressToFixedLastWeek = 0;

  const severityDistribution = {
    MINOR: 0,
    MODERATE: 0,
    HIGH: 0,
    CRITICAL: 0,
  };

  // Daily counts for last 30 days (map: dateStr -> count)
  const dailyCounts = new Map();

  // Process each pothole once
  for (const p of potholes) {
    const reportedAt = new Date(p.reported_at);
    const inProgressAt = p.in_progress_at ? new Date(p.in_progress_at) : null;
    const fixedAt = p.fixed_at ? new Date(p.fixed_at) : null;

    // 1. Active potholes count & severity (status = 'reported')
    if (p.status === 'reported') {
      activePotholesCount++;
      activeSeveritySum += parseFloat(p.severity_score) || 0;

      // Severity distribution (active only)
      if (p.severity_level in severityDistribution) {
        severityDistribution[p.severity_level]++;
      }

      // Daily counts for active potholes in last 30 days
      const dateStr = formatDate(reportedAt);
      dailyCounts.set(dateStr, (dailyCounts.get(dateStr) || 0) + 1);
    }

    // 2. New potholes today
    if (reportedAt >= todayStart && reportedAt <= todayEnd) {
      newToday++;
    }

    // 3. New potholes yesterday
    if (reportedAt >= yesterdayStart && reportedAt <= yesterdayEnd) {
      newYesterday++;
    }

    // 4. New potholes this week
    if (reportedAt >= thisWeek.start && reportedAt <= thisWeek.end) {
      newThisWeek++;
    }

    // 5. New potholes last week
    if (reportedAt >= lastWeek.start && reportedAt <= lastWeek.end) {
      newLastWeek++;
    }

    // 6. Status changes: reported -> in_progress (this week)
    if (p.status === 'in_progress' && inProgressAt) {
      if (inProgressAt >= thisWeek.start && inProgressAt <= thisWeek.end) {
        reportedToInProgressThisWeek++;
      }
      if (inProgressAt >= lastWeek.start && inProgressAt <= lastWeek.end) {
        reportedToInProgressLastWeek++;
      }
    }

    // 7. Status changes: in_progress -> fixed (this/last week)
    if (p.status === 'fixed' && fixedAt) {
      if (fixedAt >= thisWeek.start && fixedAt <= thisWeek.end) {
        inProgressToFixedThisWeek++;
      }
      if (fixedAt >= lastWeek.start && fixedAt <= lastWeek.end) {
        inProgressToFixedLastWeek++;
      }
    }
  }

  // Calculate average severity
  const averageSeverity = activePotholesCount > 0 
    ? (activeSeveritySum / activePotholesCount).toFixed(2)
    : '0.00';

  // Build last 30 days array with 0-filled gaps
  const activePotholesLast30Days = [];
  for (let i = 29; i >= 0; i--) {
    const d = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
    const dateStr = formatDate(d);
    activePotholesLast30Days.push({
      date: dateStr,
      count: dailyCounts.get(dateStr) || 0,
    });
  }

  return {
    activePotholesCount,
    averageSeverity: parseFloat(averageSeverity),
    newToday,
    newYesterday,
    newThisWeek,
    newLastWeek,
    reportedToInProgressThisWeek,
    reportedToInProgressLastWeek,
    inProgressToFixedThisWeek,
    inProgressToFixedLastWeek,
    severityDistribution,
    activePotholesLast30Days,
  };
}

/**
 * GET /api/v1/summary
 * Returns dashboard summary metrics
 * 
 * OPTIMIZED: Single Trino query + in-memory aggregation + Redis caching (5 min TTL)
 */
export async function getSummary(req, res) {
  const startTime = Date.now();
  
  try {
    // Check cache first
    const cached = await redis.get(CACHE_KEY);
    if (cached) {
      console.log(`[Summary] Cache HIT - served in ${Date.now() - startTime}ms`);
      return res.json(cached);
    }
    console.log('[Summary] Cache MISS - fetching from Trino...');

    // Calculate all date ranges needed
    const now = new Date();
    const todayStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
    const todayEnd = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 23, 59, 59, 999));
    const yesterdayStart = new Date(todayStart.getTime() - 24 * 60 * 60 * 1000);
    const yesterdayEnd = new Date(todayEnd.getTime() - 24 * 60 * 60 * 1000);
    const thisWeek = getWeekBounds(now);
    const lastWeek = {
      start: new Date(thisWeek.start.getTime() - 7 * 24 * 60 * 60 * 1000),
      end: new Date(thisWeek.end.getTime() - 7 * 24 * 60 * 60 * 1000),
    };
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

    const dateRanges = {
      now, todayStart, todayEnd, yesterdayStart, yesterdayEnd,
      thisWeek, lastWeek,
    };

    // SINGLE QUERY to fetch all data needed
    const queryStartTime = Date.now();
    const potholes = await fetchAllPotholesForSummary(thirtyDaysAgo);
    console.log(`[Summary] Trino query returned ${potholes.length} rows in ${Date.now() - queryStartTime}ms`);

    // Calculate all metrics in-memory
    const metrics = calculateSummaryMetrics(potholes, dateRanges);

    // Build comparison strings
    const todayComparison = metrics.newToday - metrics.newYesterday;
    const weekComparison = metrics.newThisWeek - metrics.newLastWeek;
    const rtipComparison = metrics.reportedToInProgressThisWeek - metrics.reportedToInProgressLastWeek;
    const ipfComparison = metrics.inProgressToFixedThisWeek - metrics.inProgressToFixedLastWeek;

    const formatComparison = (diff, unit = 'vs') => {
      const sign = diff >= 0 ? '+' : '';
      return `${sign}${diff} ${unit}`;
    };

    const response = {
      activePotholes: {
        count: metrics.activePotholesCount,
        trend: {
          today: {
            count: metrics.newToday,
            comparison: `${formatComparison(todayComparison)} yesterday`,
          },
          thisWeek: {
            count: metrics.newThisWeek,
            comparison: `${formatComparison(weekComparison)} last week`,
          },
        },
      },
      averageSeverity: metrics.averageSeverity,
      activePotholesLast30Days: metrics.activePotholesLast30Days,
      severityDistribution: metrics.severityDistribution,
      statusChanges: {
        reportedToInProgress: {
          thisWeek: metrics.reportedToInProgressThisWeek,
          comparison: `${formatComparison(rtipComparison)} last week`,
        },
        inProgressToFixed: {
          thisWeek: metrics.inProgressToFixedThisWeek,
          comparison: `${formatComparison(ipfComparison)} last week`,
        },
      },
    };

    // Cache the response with 5-minute TTL
    await redis.set(CACHE_KEY, response, CACHE_TTL_SECONDS);
    console.log(`[Summary] Response cached (TTL=${CACHE_TTL_SECONDS}s). Total time: ${Date.now() - startTime}ms`);

    return res.json(response);
  } catch (error) {
    console.error('[Summary] Error:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to fetch dashboard summary',
      details: config.nodeEnv === 'development' ? error.message : undefined,
    });
  }
}
