import trino from '../db/trino.js';
import redis from '../db/redis.js';
import config from '../config/index.js';

const CACHE_KEY = 'api:summary';

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
 * GET /api/v1/summary
 * Returns dashboard summary metrics
 */
export async function getSummary(req, res) {
  try {
    // Check cache first
    const cached = await redis.get(CACHE_KEY);
    if (cached) {
      return res.json(cached);
    }

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

    // 1. Active potholes count (status = 'reported')
    const activeCountQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE status = 'reported'
    `;
    const activeResult = await trino.query(activeCountQuery);
    const activePotholesCount = parseInt(activeResult[0]?.count || 0);

    // 2. New potholes today
    const newTodayQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE reported_at >= TIMESTAMP '${formatTrinoTimestamp(todayStart)}'
        AND reported_at <= TIMESTAMP '${formatTrinoTimestamp(todayEnd)}'
    `;
    const newTodayResult = await trino.query(newTodayQuery);
    const newToday = parseInt(newTodayResult[0]?.count || 0);

    // 3. New potholes yesterday (for comparison)
    const newYesterdayQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE reported_at >= TIMESTAMP '${formatTrinoTimestamp(yesterdayStart)}'
        AND reported_at <= TIMESTAMP '${formatTrinoTimestamp(yesterdayEnd)}'
    `;
    const newYesterdayResult = await trino.query(newYesterdayQuery);
    const newYesterday = parseInt(newYesterdayResult[0]?.count || 0);

    // 4. New potholes this week
    const newThisWeekQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE reported_at >= TIMESTAMP '${formatTrinoTimestamp(thisWeek.start)}'
        AND reported_at <= TIMESTAMP '${formatTrinoTimestamp(thisWeek.end)}'
    `;
    const newThisWeekResult = await trino.query(newThisWeekQuery);
    const newThisWeek = parseInt(newThisWeekResult[0]?.count || 0);

    // 5. New potholes last week (for comparison)
    const newLastWeekQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE reported_at >= TIMESTAMP '${formatTrinoTimestamp(lastWeek.start)}'
        AND reported_at <= TIMESTAMP '${formatTrinoTimestamp(lastWeek.end)}'
    `;
    const newLastWeekResult = await trino.query(newLastWeekQuery);
    const newLastWeek = parseInt(newLastWeekResult[0]?.count || 0);

    // 6. Average severity score of active potholes
    const avgSeverityQuery = `
      SELECT AVG(severity_score) as avg_severity
      FROM iceberg.city.potholes
      WHERE status = 'reported'
    `;
    const avgSeverityResult = await trino.query(avgSeverityQuery);
    const averageSeverity = parseFloat(avgSeverityResult[0]?.avg_severity || 0).toFixed(2);

    // 7. Active potholes last 30 days (daily counts)
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const activeLast30DaysQuery = `
      SELECT 
        CAST(reported_at AS DATE) as date,
        COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE reported_at >= TIMESTAMP '${formatTrinoTimestamp(thirtyDaysAgo)}'
        AND status = 'reported'
      GROUP BY CAST(reported_at AS DATE)
      ORDER BY date ASC
    `;
    const activeLast30DaysResult = await trino.query(activeLast30DaysQuery);
    
    // Fill in missing days with 0
    const activePotholesLast30Days = [];
    for (let i = 29; i >= 0; i--) {
      const d = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
      const dateStr = formatDate(d);
      const found = activeLast30DaysResult.find(r => formatDate(new Date(r.date)) === dateStr);
      activePotholesLast30Days.push({
        date: dateStr,
        count: found ? parseInt(found.count) : 0,
      });
    }

    // 8. Severity distribution (active potholes only)
    const severityDistQuery = `
      SELECT 
        severity_level,
        COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE status = 'reported'
      GROUP BY severity_level
    `;
    const severityDistResult = await trino.query(severityDistQuery);
    const severityDistribution = {
      MINOR: 0,
      MODERATE: 0,
      HIGH: 0,
      CRITICAL: 0,
    };
    severityDistResult.forEach(row => {
      if (row.severity_level in severityDistribution) {
        severityDistribution[row.severity_level] = parseInt(row.count);
      }
    });

    // 9. Status changes this week: reported -> in_progress
    const reportedToInProgressThisWeekQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE status = 'in_progress'
        AND in_progress_at >= TIMESTAMP '${formatTrinoTimestamp(thisWeek.start)}'
        AND in_progress_at <= TIMESTAMP '${formatTrinoTimestamp(thisWeek.end)}'
    `;
    const rtipThisWeekResult = await trino.query(reportedToInProgressThisWeekQuery);
    const reportedToInProgressThisWeek = parseInt(rtipThisWeekResult[0]?.count || 0);

    // 10. Status changes last week: reported -> in_progress
    const reportedToInProgressLastWeekQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE status = 'in_progress'
        AND in_progress_at >= TIMESTAMP '${formatTrinoTimestamp(lastWeek.start)}'
        AND in_progress_at <= TIMESTAMP '${formatTrinoTimestamp(lastWeek.end)}'
    `;
    const rtipLastWeekResult = await trino.query(reportedToInProgressLastWeekQuery);
    const reportedToInProgressLastWeek = parseInt(rtipLastWeekResult[0]?.count || 0);

    // 11. Status changes this week: in_progress -> fixed
    const inProgressToFixedThisWeekQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE status = 'fixed'
        AND fixed_at >= TIMESTAMP '${formatTrinoTimestamp(thisWeek.start)}'
        AND fixed_at <= TIMESTAMP '${formatTrinoTimestamp(thisWeek.end)}'
    `;
    const ipfThisWeekResult = await trino.query(inProgressToFixedThisWeekQuery);
    const inProgressToFixedThisWeek = parseInt(ipfThisWeekResult[0]?.count || 0);

    // 12. Status changes last week: in_progress -> fixed
    const inProgressToFixedLastWeekQuery = `
      SELECT COUNT(*) as count
      FROM iceberg.city.potholes
      WHERE status = 'fixed'
        AND fixed_at >= TIMESTAMP '${formatTrinoTimestamp(lastWeek.start)}'
        AND fixed_at <= TIMESTAMP '${formatTrinoTimestamp(lastWeek.end)}'
    `;
    const ipfLastWeekResult = await trino.query(inProgressToFixedLastWeekQuery);
    const inProgressToFixedLastWeek = parseInt(ipfLastWeekResult[0]?.count || 0);

    // Build comparison strings
    const todayComparison = newToday - newYesterday;
    const weekComparison = newThisWeek - newLastWeek;
    const rtipComparison = reportedToInProgressThisWeek - reportedToInProgressLastWeek;
    const ipfComparison = inProgressToFixedThisWeek - inProgressToFixedLastWeek;

    const formatComparison = (diff, unit = 'vs') => {
      const sign = diff >= 0 ? '+' : '';
      return `${sign}${diff} ${unit}`;
    };

    const response = {
      activePotholes: {
        count: activePotholesCount,
        trend: {
          today: {
            count: newToday,
            comparison: `${formatComparison(todayComparison)} yesterday`,
          },
          thisWeek: {
            count: newThisWeek,
            comparison: `${formatComparison(weekComparison)} last week`,
          },
        },
      },
      averageSeverity: parseFloat(averageSeverity),
      activePotholesLast30Days,
      severityDistribution,
      statusChanges: {
        reportedToInProgress: {
          thisWeek: reportedToInProgressThisWeek,
          comparison: `${formatComparison(rtipComparison)} last week`,
        },
        inProgressToFixed: {
          thisWeek: inProgressToFixedThisWeek,
          comparison: `${formatComparison(ipfComparison)} last week`,
        },
      },
    };

    // Cache the response
    await redis.set(CACHE_KEY, response, config.cacheTTL.summary);

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
