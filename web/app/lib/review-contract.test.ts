import { describe, expect, test } from "vitest";
import {
  buildReviewQueueSql,
  deriveReviewTaskId,
  mapReviewQueueRow,
  readLowConfidenceThreshold,
} from "./review-contract";

describe("readLowConfidenceThreshold", () => {
  test("defaults to 0.6", () => {
    expect(readLowConfidenceThreshold(undefined)).toBe(0.6);
  });

  test("uses a valid configured value", () => {
    expect(readLowConfidenceThreshold("0.42")).toBe(0.42);
  });
});

describe("deriveReviewTaskId", () => {
  test("creates stable task ids for defects", () => {
    expect(deriveReviewTaskId("defect-event-1")).toBe("review-defect-event-1");
  });
});

describe("buildReviewQueueSql", () => {
  test("filters pending tasks and low confidence candidates", () => {
    const query = buildReviewQueueSql(0.6);
    expect(query.sql).toContain("serving.current_road_defects");
    expect(query.sql).toContain("serving.review_tasks");
    expect(query.sql).toContain("COALESCE(rt.status, 'pending')");
    expect(query.params).toEqual([0.6]);
  });
});

describe("mapReviewQueueRow", () => {
  test("normalizes nullable SQL rows for the UI", () => {
    expect(
      mapReviewQueueRow({
        review_task_id: null,
        defect_id: "defect-1",
        evidence_id: "event-1",
        confidence: 0.42,
        severity_level: "HIGH",
        status: "reported",
        task_status: null,
        priority: null,
        assigned_to: null,
        latest_raw_image_object_key: "s3://warehouse/raw_images/event-1.jpg",
        latest_bev_object_key: null,
        last_seen_at: "2026-05-08T12:00:00.000Z",
        district: "District 1",
        ward: null,
      })
    ).toMatchObject({
      reviewTaskId: "review-defect-1",
      defectId: "defect-1",
      evidenceId: "event-1",
      confidencePercent: 42,
      severity: "high",
      taskStatus: "pending",
      location: "District 1",
      reason: "Model confidence below review threshold",
    });
  });
});
