import { describe, expect, test } from "vitest";
import {
  buildAnnotationLabel,
  buildSam3AssistResponse,
  parseAnnotationActionPayload,
  validatePolygon,
} from "./annotation-contract";

describe("validatePolygon", () => {
  test("accepts a polygon with at least three finite points", () => {
    expect(validatePolygon([[1, 2], [3, 4], [5, 6]])).toEqual({
      ok: true,
      polygon: [[1, 2], [3, 4], [5, 6]],
    });
  });

  test("rejects polygons with fewer than three points", () => {
    expect(validatePolygon([[1, 2], [3, 4]])).toEqual({
      ok: false,
      error: "Annotation polygon requires at least three points.",
    });
  });

  test("rejects non-finite coordinates", () => {
    expect(validatePolygon([[1, 2], [Number.NaN, 4], [5, 6]])).toEqual({
      ok: false,
      error: "Annotation polygon contains invalid coordinates.",
    });
  });
});

describe("parseAnnotationActionPayload", () => {
  test("parses save intent and polygon JSON", () => {
    const formData = new FormData();
    formData.set("intent", "save");
    formData.set("review_task_id", "task-1");
    formData.set("defect_id", "defect-1");
    formData.set("evidence_id", "event-1");
    formData.set("polygon", JSON.stringify([[1, 2], [3, 4], [5, 6]]));
    formData.set("source", "manual");
    formData.set("notes", "clear edge");

    expect(parseAnnotationActionPayload(formData)).toMatchObject({
      ok: true,
      payload: {
        intent: "save",
        reviewTaskId: "task-1",
        defectId: "defect-1",
        evidenceId: "event-1",
        polygon: [[1, 2], [3, 4], [5, 6]],
        source: "manual",
        notes: "clear edge",
      },
    });
  });
});

describe("buildAnnotationLabel", () => {
  test("marks submit payloads as final labels", () => {
    const label = buildAnnotationLabel({
      intent: "submit",
      reviewTaskId: "task-1",
      defectId: "defect-1",
      evidenceId: "event-1",
      polygon: [[1, 2], [3, 4], [5, 6]],
      source: "manual_refined_sam3",
      notes: "refined",
    });

    expect(label.status).toBe("final");
    expect(label.source).toBe("manual_refined_sam3");
    expect(label.created_from).toEqual({
      review_task_id: "task-1",
      evidence_id: "event-1",
    });
  });
});

describe("buildSam3AssistResponse", () => {
  test("returns a deterministic adjusted polygon and model metadata", () => {
    expect(
      buildSam3AssistResponse({
        current_polygon: [[200, 120], [350, 100], [380, 230], [220, 260]],
      })
    ).toEqual({
      polygon: [[198, 118], [354, 104], [382, 232], [218, 258]],
      confidence: 0.74,
      source: "sam3_assist",
      model: {
        name: "sam3-assist-contract-stub",
        version: "demo-contract-v1",
      },
    });
  });
});
