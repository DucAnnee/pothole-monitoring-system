export type AnnotationIntent = "save" | "submit";
export type AnnotationSource = "manual" | "sam3_assist" | "manual_refined_sam3";
export type PolygonPoint = [number, number];
export type Polygon = PolygonPoint[];

export interface ParsedAnnotationPayload {
  intent: AnnotationIntent;
  reviewTaskId: string;
  defectId: string;
  evidenceId: string | null;
  polygon: Polygon;
  source: AnnotationSource;
  notes: string;
}

export interface AnnotationLabel {
  status: "draft" | "final";
  polygon: Polygon;
  source: AnnotationSource;
  notes: string;
  confidence: number | null;
  model: {
    name: string;
    version: string;
  } | null;
  created_from: {
    review_task_id: string;
    evidence_id: string | null;
  };
}

export interface Sam3AssistRequest {
  review_task_id?: string;
  evidence_id?: string | null;
  image_object_key?: string | null;
  current_polygon?: unknown;
  prompt?: string;
}

export interface Sam3AssistResponse {
  polygon: Polygon;
  confidence: number;
  source: "sam3_assist";
  model: {
    name: "sam3-assist-contract-stub";
    version: "demo-contract-v1";
  };
}

type PolygonResult =
  | { ok: true; polygon: Polygon }
  | { ok: false; error: string };

type PayloadResult =
  | { ok: true; payload: ParsedAnnotationPayload }
  | { ok: false; error: string };

const SOURCES = new Set<AnnotationSource>([
  "manual",
  "sam3_assist",
  "manual_refined_sam3",
]);

function readRequired(formData: FormData, key: string): string | null {
  const value = formData.get(key);
  if (typeof value !== "string" || value.trim() === "") return null;
  return value.trim();
}

export function validatePolygon(value: unknown): PolygonResult {
  if (!Array.isArray(value) || value.length < 3) {
    return { ok: false, error: "Annotation polygon requires at least three points." };
  }

  const polygon: Polygon = [];
  for (const point of value) {
    if (!Array.isArray(point) || point.length !== 2) {
      return { ok: false, error: "Annotation polygon contains invalid coordinates." };
    }
    const x = Number(point[0]);
    const y = Number(point[1]);
    if (!Number.isFinite(x) || !Number.isFinite(y)) {
      return { ok: false, error: "Annotation polygon contains invalid coordinates." };
    }
    polygon.push([x, y]);
  }

  return { ok: true, polygon };
}

export function parseAnnotationActionPayload(formData: FormData): PayloadResult {
  const intent = readRequired(formData, "intent");
  if (intent !== "save" && intent !== "submit") {
    return { ok: false, error: "Unsupported annotation action." };
  }

  const reviewTaskId = readRequired(formData, "review_task_id");
  const defectId = readRequired(formData, "defect_id");
  const polygonJson = readRequired(formData, "polygon");
  const source = readRequired(formData, "source");

  if (!reviewTaskId || !defectId || !polygonJson || !source) {
    return { ok: false, error: "Missing required annotation fields." };
  }
  if (!SOURCES.has(source as AnnotationSource)) {
    return { ok: false, error: "Unsupported annotation source." };
  }

  let parsedPolygon: unknown;
  try {
    parsedPolygon = JSON.parse(polygonJson);
  } catch {
    return { ok: false, error: "Annotation polygon must be valid JSON." };
  }

  const polygon = validatePolygon(parsedPolygon);
  if (!polygon.ok) return polygon;

  const evidenceId = readRequired(formData, "evidence_id");
  const notesValue = formData.get("notes");
  const notes = typeof notesValue === "string" ? notesValue : "";

  return {
    ok: true,
    payload: {
      intent,
      reviewTaskId,
      defectId,
      evidenceId,
      polygon: polygon.polygon,
      source: source as AnnotationSource,
      notes,
    },
  };
}

export function buildAnnotationLabel(payload: ParsedAnnotationPayload): AnnotationLabel {
  return {
    status: payload.intent === "submit" ? "final" : "draft",
    polygon: payload.polygon,
    source: payload.source,
    notes: payload.notes,
    confidence: null,
    model:
      payload.source === "sam3_assist" || payload.source === "manual_refined_sam3"
        ? { name: "sam3-assist-contract-stub", version: "demo-contract-v1" }
        : null,
    created_from: {
      review_task_id: payload.reviewTaskId,
      evidence_id: payload.evidenceId,
    },
  };
}

export function buildSam3AssistResponse(
  request: Sam3AssistRequest,
): Sam3AssistResponse {
  const polygon = validatePolygon(request.current_polygon);
  const base = polygon.ok
    ? polygon.polygon
    : [[200, 120], [350, 100], [380, 230], [220, 260]];

  const adjusted = base.map(([x, y], index): PolygonPoint => {
    const offsets: Polygon = [[-2, -2], [4, 4], [2, 2], [-2, -2]];
    const [dx, dy] = offsets[index % offsets.length];
    return [Math.max(0, x + dx), Math.max(0, y + dy)];
  });

  return {
    polygon: adjusted,
    confidence: 0.74,
    source: "sam3_assist",
    model: {
      name: "sam3-assist-contract-stub",
      version: "demo-contract-v1",
    },
  };
}
