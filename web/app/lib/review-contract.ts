export type ReviewTaskStatus = "pending" | "in_progress" | "completed";

export interface ReviewQueueRow {
  review_task_id: string | null;
  defect_id: string;
  evidence_id: string | null;
  confidence: number | null;
  severity_level: string | null;
  status: string;
  task_status: string | null;
  priority: string | null;
  assigned_to: string | null;
  latest_raw_image_object_key: string | null;
  latest_bev_object_key: string | null;
  last_seen_at: string | null;
  district: string | null;
  ward: string | null;
}

export interface ReviewQueueItem {
  reviewTaskId: string;
  defectId: string;
  evidenceId: string | null;
  confidence: number | null;
  confidencePercent: number;
  severity: "critical" | "high" | "moderate" | "minor";
  defectStatus: string;
  taskStatus: ReviewTaskStatus;
  priority: string | null;
  assignedTo: string | null;
  rawImageObjectKey: string | null;
  bevImageObjectKey: string | null;
  lastSeenAt: string | null;
  location: string;
  reason: string;
}

export function readLowConfidenceThreshold(value: string | undefined): number {
  const parsed = Number.parseFloat(value ?? "");
  if (!Number.isFinite(parsed) || parsed <= 0 || parsed >= 1) return 0.6;
  return parsed;
}

export function deriveReviewTaskId(defectId: string): string {
  return `review-${defectId}`;
}

export function normalizeSeverity(
  value: string | null,
): ReviewQueueItem["severity"] {
  const normalized = String(value ?? "minor").toLowerCase();
  if (
    normalized === "critical" ||
    normalized === "high" ||
    normalized === "moderate"
  ) {
    return normalized;
  }
  return "minor";
}

export function normalizeTaskStatus(value: string | null): ReviewTaskStatus {
  if (value === "in_progress" || value === "completed") return value;
  return "pending";
}

export function buildReviewQueueSql(
  threshold: number,
): { sql: string; params: unknown[] } {
  return {
    sql: `
      SELECT
        rt.review_task_id,
        d.defect_id,
        e.evidence_id,
        d.confidence,
        d.severity_level,
        d.status,
        COALESCE(rt.status, 'pending') AS task_status,
        rt.priority,
        rt.assigned_to,
        COALESCE(e.raw_image_object_key, d.latest_raw_image_object_key) AS latest_raw_image_object_key,
        COALESCE(e.bev_object_key, d.latest_bev_object_key) AS latest_bev_object_key,
        d.last_seen_at,
        d.district,
        d.ward
      FROM serving.current_road_defects d
      LEFT JOIN serving.review_tasks rt
        ON rt.defect_id = d.defect_id
      LEFT JOIN serving.defect_evidence_index e
        ON e.defect_id = d.defect_id
      WHERE COALESCE(d.confidence, 1.0) < $1
        AND COALESCE(rt.status, 'pending') <> 'completed'
      ORDER BY d.confidence ASC NULLS FIRST, d.last_seen_at DESC NULLS LAST
      LIMIT 100
    `,
    params: [threshold],
  };
}

export function buildReviewTaskStatusSql(
  reviewTaskId: string,
  defectId: string,
  status: ReviewTaskStatus,
): { sql: string; params: unknown[] } {
  return {
    sql: `
      INSERT INTO serving.review_tasks (
        review_task_id,
        defect_id,
        status,
        updated_at
      )
      VALUES ($1, $2, $3, now())
      ON CONFLICT (review_task_id) DO UPDATE SET
        status = EXCLUDED.status,
        updated_at = now()
    `,
    params: [reviewTaskId, defectId, status],
  };
}

export function mapReviewQueueRow(row: ReviewQueueRow): ReviewQueueItem {
  const confidencePercent = Math.round((row.confidence ?? 0) * 100);
  return {
    reviewTaskId: row.review_task_id ?? deriveReviewTaskId(row.defect_id),
    defectId: row.defect_id,
    evidenceId: row.evidence_id,
    confidence: row.confidence,
    confidencePercent,
    severity: normalizeSeverity(row.severity_level),
    defectStatus: row.status,
    taskStatus: normalizeTaskStatus(row.task_status),
    priority: row.priority,
    assignedTo: row.assigned_to,
    rawImageObjectKey: row.latest_raw_image_object_key,
    bevImageObjectKey: row.latest_bev_object_key,
    lastSeenAt: row.last_seen_at != null ? String(row.last_seen_at) : null,
    location:
      row.ward && row.district
        ? `${row.ward}, ${row.district}`
        : row.district ?? row.ward ?? "Unknown location",
    reason: "Model confidence below review threshold",
  };
}
