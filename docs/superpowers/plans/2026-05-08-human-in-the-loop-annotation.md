# Human-in-the-loop Annotation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a demo-ready human-in-the-loop review flow where low-confidence pothole detections are flagged, corrected by operators, assisted by a SAM3 contract stub, and persisted as annotation records.

**Architecture:** Keep the current edge and Kafka schemas unchanged. Add typed web-layer contracts, PostGIS review helpers, real `/lowconf` loader data, `/annotation` loader/action handling, and a resource route for SAM3 assist. Use a deterministic SAM3 assist stub now so a later SAM3 microservice can replace it without changing UI contracts.

**Tech Stack:** React Router v7, React 19, MUI 6, TypeScript strict mode, PostgreSQL/PostGIS via `pg`, MinIO image proxy, Vitest for focused TypeScript helper tests, existing Python pytest contract tests.

---

## File Structure

- Create `web/vitest.config.ts` to run focused TypeScript tests with Vite path aliases.
- Modify `web/package.json` and `web/package-lock.json` to add `vitest` and a `test` script.
- Create `web/app/lib/annotation-contract.ts` for polygon validation, annotation action parsing, label JSON construction, and deterministic SAM3 assist response generation.
- Create `web/app/lib/annotation-contract.test.ts` for pure annotation and SAM3 contract tests.
- Create `web/app/lib/review-contract.ts` for review queue types, low-confidence threshold parsing, review queue row mapping, SQL text builders, and task id derivation.
- Create `web/app/lib/review-contract.test.ts` for pure review queue tests.
- Modify `web/app/lib/postgis.server.ts` to expose review queue, review detail, annotation insert, task status update, and audit insert helpers.
- Modify `web/app/routes.ts` to register the SAM3 assist resource route.
- Create `web/app/routes/api.annotation.assist.tsx` for SAM3 assist requests.
- Modify `web/app/routes/_app.lowconf.tsx` to use loader data and link to annotation tasks.
- Modify `web/app/routes/_app.annotation.tsx` to load real review task data, save draft, submit final annotations, call SAM3 assist, and preserve the existing editor interactions.

## Task 1: Add Web Test Harness

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Create: `web/vitest.config.ts`

- [ ] **Step 1: Add the failing test command expectation**

Run:

```powershell
cd web
npm run test -- --run
```

Expected: FAIL with `Missing script: "test"` because the web app has no test script yet.

- [ ] **Step 2: Install Vitest**

Run:

```powershell
cd web
npm install -D vitest
```

Expected: `package.json` and `package-lock.json` change, and `vitest` appears under `devDependencies`.

- [ ] **Step 3: Add the test script**

Edit `web/package.json` so `scripts` contains:

```json
{
  "scripts": {
    "build": "react-router build",
    "dev": "react-router dev",
    "start": "react-router-serve ./build/server/index.js",
    "typecheck": "tsc",
    "test": "vitest run"
  }
}
```

- [ ] **Step 4: Create Vitest config**

Create `web/vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  plugins: [tsconfigPaths()],
  test: {
    environment: "node",
    include: ["app/**/*.test.ts"],
  },
});
```

- [ ] **Step 5: Verify the harness runs**

Run:

```powershell
cd web
npm run test -- --run
```

Expected: PASS with `No test files found` or equivalent zero-test Vitest output. If Vitest exits non-zero because no tests exist, continue; Task 2 will add the first test and make the command meaningful.

- [ ] **Step 6: Commit**

Run:

```powershell
git add web/package.json web/package-lock.json web/vitest.config.ts
git commit -m "test(web): add vitest harness"
```

## Task 2: Annotation Contract Helpers

**Files:**
- Create: `web/app/lib/annotation-contract.test.ts`
- Create: `web/app/lib/annotation-contract.ts`

- [ ] **Step 1: Write failing tests for annotation contracts**

Create `web/app/lib/annotation-contract.test.ts`:

```ts
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
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
cd web
npm run test -- app/lib/annotation-contract.test.ts
```

Expected: FAIL because `./annotation-contract` does not exist.

- [ ] **Step 3: Create minimal annotation contract implementation**

Create `web/app/lib/annotation-contract.ts`:

```ts
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
  const notes = typeof formData.get("notes") === "string" ? String(formData.get("notes")) : "";

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
    model: payload.source === "sam3_assist" || payload.source === "manual_refined_sam3"
      ? { name: "sam3-assist-contract-stub", version: "demo-contract-v1" }
      : null,
    created_from: {
      review_task_id: payload.reviewTaskId,
      evidence_id: payload.evidenceId,
    },
  };
}

export function buildSam3AssistResponse(request: Sam3AssistRequest): Sam3AssistResponse {
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
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```powershell
cd web
npm run test -- app/lib/annotation-contract.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add web/app/lib/annotation-contract.ts web/app/lib/annotation-contract.test.ts
git commit -m "feat(web): add annotation contract helpers"
```

## Task 3: Review Queue Contracts

**Files:**
- Create: `web/app/lib/review-contract.test.ts`
- Create: `web/app/lib/review-contract.ts`

- [ ] **Step 1: Write failing tests for review queue contracts**

Create `web/app/lib/review-contract.test.ts`:

```ts
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
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
cd web
npm run test -- app/lib/review-contract.test.ts
```

Expected: FAIL because `./review-contract` does not exist.

- [ ] **Step 3: Create minimal review contract implementation**

Create `web/app/lib/review-contract.ts`:

```ts
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

export function normalizeSeverity(value: string | null): ReviewQueueItem["severity"] {
  const normalized = String(value ?? "minor").toLowerCase();
  if (normalized === "critical" || normalized === "high" || normalized === "moderate") {
    return normalized;
  }
  return "minor";
}

export function normalizeTaskStatus(value: string | null): ReviewTaskStatus {
  if (value === "in_progress" || value === "completed") return value;
  return "pending";
}

export function buildReviewQueueSql(threshold: number): { sql: string; params: unknown[] } {
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
    lastSeenAt: row.last_seen_at,
    location: row.ward && row.district ? `${row.ward}, ${row.district}` : row.district ?? row.ward ?? "Unknown location",
    reason: "Model confidence below review threshold",
  };
}
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```powershell
cd web
npm run test -- app/lib/review-contract.test.ts
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add web/app/lib/review-contract.ts web/app/lib/review-contract.test.ts
git commit -m "feat(web): add review queue contracts"
```

## Task 4: PostGIS Review and Annotation Helpers

**Files:**
- Modify: `web/app/lib/postgis.server.ts`

- [ ] **Step 1: Write the intended public API in comments before implementation**

At the bottom of `web/app/lib/postgis.server.ts`, add this temporary comment block:

```ts
// Planned review API:
// - queryReviewQueue(threshold?: number): Promise<ReviewQueueItem[]>
// - queryReviewTaskDetail(reviewTaskId: string): Promise<ReviewTaskDetail | null>
// - insertAnnotation(input: InsertAnnotationInput): Promise<string>
// - updateReviewTaskStatus(reviewTaskId: string, status: ReviewTaskStatus): Promise<void>
// - insertAuditLog(input: AuditLogInput): Promise<void>
```

- [ ] **Step 2: Run typecheck to confirm the comment does not change behavior**

Run:

```powershell
cd web
npm run typecheck
```

Expected: PASS.

- [ ] **Step 3: Add imports and interfaces**

In `web/app/lib/postgis.server.ts`, import review and annotation types near the top:

```ts
import { randomUUID } from "node:crypto";
import type { AnnotationLabel } from "~/lib/annotation-contract";
import {
  buildReviewQueueSql,
  mapReviewQueueRow,
  readLowConfidenceThreshold,
  type ReviewQueueItem,
  type ReviewQueueRow,
  type ReviewTaskStatus,
} from "~/lib/review-contract";
```

Add these interfaces near existing exported interfaces:

```ts
export interface ReviewTaskDetail extends ReviewQueueItem {
  imageProxyUrl: string | null;
  originalPolygon: Array<[number, number]>;
  latestAnnotation: AnnotationLabel | null;
}

export interface InsertAnnotationInput {
  defectId: string;
  evidenceId: string | null;
  label: AnnotationLabel;
  annotatorId: string | null;
}

export interface AuditLogInput {
  actorId: string | null;
  action: string;
  entityType: string;
  entityId: string;
  payload: Record<string, unknown>;
}
```

- [ ] **Step 4: Add helper functions**

Add these exports at the bottom of `web/app/lib/postgis.server.ts` and remove the temporary comment:

```ts
export async function queryReviewQueue(threshold = readLowConfidenceThreshold(process.env.LOW_CONFIDENCE_THRESHOLD)): Promise<ReviewQueueItem[]> {
  const query = buildReviewQueueSql(threshold);
  const rows = await queryPostgis<ReviewQueueRow>(query.sql, query.params);
  return rows.map(mapReviewQueueRow);
}

export async function queryReviewTaskDetail(reviewTaskId: string): Promise<ReviewTaskDetail | null> {
  const rows = await queryReviewQueue();
  const item = rows.find((row) => row.reviewTaskId === reviewTaskId);
  if (!item) return null;

  const annotationRows = await queryPostgis<{ label_json: AnnotationLabel }>(
    `
      SELECT label_json
      FROM serving.annotations
      WHERE defect_id = $1
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [item.defectId]
  );

  return {
    ...item,
    imageProxyUrl: item.rawImageObjectKey
      ? `/api/image/proxy?path=${encodeURIComponent(item.rawImageObjectKey)}`
      : null,
    originalPolygon: [[200, 120], [350, 100], [380, 230], [220, 260]],
    latestAnnotation: annotationRows[0]?.label_json ?? null,
  };
}

export async function insertAnnotation(input: InsertAnnotationInput): Promise<string> {
  const annotationId = randomUUID();
  await queryPostgis(
    `
      INSERT INTO serving.annotations (
        annotation_id,
        defect_id,
        evidence_id,
        label_json,
        annotator_id
      )
      VALUES ($1, $2, $3, $4::jsonb, $5)
    `,
    [
      annotationId,
      input.defectId,
      input.evidenceId,
      JSON.stringify(input.label),
      input.annotatorId,
    ]
  );
  return annotationId;
}

export async function updateReviewTaskStatus(reviewTaskId: string, status: ReviewTaskStatus): Promise<void> {
  await queryPostgis(
    `
      UPDATE serving.review_tasks
      SET status = $2, updated_at = now()
      WHERE review_task_id = $1
    `,
    [reviewTaskId, status]
  );
}

export async function insertAuditLog(input: AuditLogInput): Promise<void> {
  await queryPostgis(
    `
      INSERT INTO serving.audit_log (
        actor_id,
        action,
        entity_type,
        entity_id,
        payload
      )
      VALUES ($1, $2, $3, $4, $5::jsonb)
    `,
    [
      input.actorId,
      input.action,
      input.entityType,
      input.entityId,
      JSON.stringify(input.payload),
    ]
  );
}
```

- [ ] **Step 5: Run web tests and typecheck**

Run:

```powershell
cd web
npm run test -- app/lib/annotation-contract.test.ts app/lib/review-contract.test.ts
npm run typecheck
```

Expected: both commands PASS.

- [ ] **Step 6: Commit**

Run:

```powershell
git add web/app/lib/postgis.server.ts
git commit -m "feat(web): add review persistence helpers"
```

## Task 5: Low-confidence Queue Page

**Files:**
- Modify: `web/app/routes/_app.lowconf.tsx`

- [ ] **Step 1: Add loader imports and types**

Modify imports in `web/app/routes/_app.lowconf.tsx`:

```ts
import type { Route } from "./+types/_app.lowconf";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import Slider from "@mui/material/Slider";
import Typography from "@mui/material/Typography";
import { Link, useLoaderData } from "react-router";
import { useState } from "react";
import { SEVERITY } from "~/constants/severity";
import { queryReviewQueue } from "~/lib/postgis.server";
```

- [ ] **Step 2: Add the loader**

Below `export const handle`, add:

```ts
export async function loader(_args: Route.LoaderArgs) {
  const items = await queryReviewQueue().catch((error) => {
    console.warn("[PostGIS] review queue unavailable:", error instanceof Error ? error.message : error);
    return [];
  });
  return { items };
}
```

- [ ] **Step 3: Remove mock data**

Delete the `MOCK_DETECTIONS` constant. Keep:

```ts
const SEVERITY_KEYS = ["critical", "high", "moderate", "minor"] as const;
const TASK_STATUSES = ["pending", "in_progress"] as const;
```

- [ ] **Step 4: Replace component data source**

Inside `LowConfPage`, add loader data and task status state:

```ts
const { items } = useLoaderData<typeof loader>();
const [confRange, setConfRange] = useState<number[]>([0, 60]);
const [activeSeverities, setActiveSeverities] = useState<Set<string>>(new Set());
const [activeStatuses, setActiveStatuses] = useState<Set<string>>(new Set());
```

Add:

```ts
function toggleStatus(k: string) {
  setActiveStatuses((prev) => {
    const next = new Set(prev);
    next.has(k) ? next.delete(k) : next.add(k);
    return next;
  });
}

const filtered = items.filter(
  (d) =>
    d.confidencePercent >= confRange[0] &&
    d.confidencePercent <= confRange[1] &&
    (activeSeverities.size === 0 || activeSeverities.has(d.severity)) &&
    (activeStatuses.size === 0 || activeStatuses.has(d.taskStatus))
);
```

- [ ] **Step 5: Replace card rendering**

In the card grid, render `filtered.map((d) => ...)` with these fields:

```tsx
<Card key={d.reviewTaskId}>
  <CardContent>
    <Box sx={{ display: "flex", gap: 1, mb: 1.5 }}>
      <Box
        component={d.rawImageObjectKey ? "img" : "div"}
        src={d.rawImageObjectKey ? `/api/image/proxy?path=${encodeURIComponent(d.rawImageObjectKey)}` : undefined}
        sx={{
          flex: 1,
          height: 120,
          objectFit: "cover",
          bgcolor: "#F0F0F0",
          borderRadius: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        {!d.rawImageObjectKey && <Typography variant="caption" color="text.disabled">Raw</Typography>}
      </Box>
    </Box>

    <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 0.5 }}>
      <Box>
        <Typography variant="body2" fontWeight={600}>{d.confidencePercent}% confidence</Typography>
        <Typography variant="caption" color="text.secondary">{d.location}</Typography>
      </Box>
      <Chip
        label={SEVERITY[d.severity].label}
        size="small"
        sx={{ color: SEVERITY[d.severity].color, bgcolor: SEVERITY[d.severity].bg }}
      />
    </Box>

    <Typography variant="caption" color="text.secondary" display="block">{d.lastSeenAt ?? "Unknown time"}</Typography>
    <Typography variant="caption" color="warning.main" display="block" mt={0.5}>
      Reason: {d.reason}
    </Typography>
    <Chip label={d.taskStatus.replace("_", " ")} size="small" sx={{ mt: 1, mr: 1 }} />
    <Button component={Link} to={`/annotation?task=${encodeURIComponent(d.reviewTaskId)}`} size="small" variant="outlined" sx={{ mt: 1 }}>
      Review
    </Button>
  </CardContent>
</Card>
```

- [ ] **Step 6: Add empty state**

Before the grid, add:

```tsx
{filtered.length === 0 && (
  <Box sx={{ p: 4, border: "1px solid #E2E8F0", borderRadius: 2, bgcolor: "background.paper" }}>
    <Typography variant="body2" color="text.secondary">
      No low-confidence detections match the current filters.
    </Typography>
  </Box>
)}
```

- [ ] **Step 7: Run typecheck**

Run:

```powershell
cd web
npm run typecheck
```

Expected: PASS. If MUI `Box` with dynamic component types creates a type issue, replace the image preview with a plain `<Box component="img" ... />` branch and a separate placeholder branch.

- [ ] **Step 8: Commit**

Run:

```powershell
git add web/app/routes/_app.lowconf.tsx
git commit -m "feat(web): load low-confidence review queue"
```

## Task 6: SAM3 Assist Resource Route

**Files:**
- Modify: `web/app/routes.ts`
- Create: `web/app/routes/api.annotation.assist.tsx`

- [ ] **Step 1: Register the route**

In `web/app/routes.ts`, add this route near the other API routes:

```ts
route("api/annotation/assist", "routes/api.annotation.assist.tsx"),
```

- [ ] **Step 2: Create the resource route**

Create `web/app/routes/api.annotation.assist.tsx`:

```ts
import type { Route } from "./+types/api.annotation.assist";
import { buildSam3AssistResponse } from "~/lib/annotation-contract";

export async function action({ request }: Route.ActionArgs) {
  if (request.method !== "POST") {
    return Response.json({ error: "Method not allowed" }, { status: 405 });
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "Request body must be JSON." }, { status: 400 });
  }

  const response = buildSam3AssistResponse(
    typeof body === "object" && body !== null ? body : {}
  );
  return Response.json(response);
}
```

- [ ] **Step 3: Run typecheck**

Run:

```powershell
cd web
npm run typecheck
```

Expected: PASS. If generated route types are missing, run `npm run typecheck` once more after React Router type generation updates `.react-router/types`.

- [ ] **Step 4: Commit**

Run:

```powershell
git add web/app/routes.ts web/app/routes/api.annotation.assist.tsx
git commit -m "feat(web): add sam3 assist endpoint"
```

## Task 7: Annotation Loader, Actions, and Editor Integration

**Files:**
- Modify: `web/app/routes/_app.annotation.tsx`

- [ ] **Step 1: Add route imports**

Update imports in `web/app/routes/_app.annotation.tsx`:

```ts
import type { Route } from "./+types/_app.annotation";
import {
  buildAnnotationLabel,
  parseAnnotationActionPayload,
  type Polygon,
} from "~/lib/annotation-contract";
import {
  insertAnnotation,
  insertAuditLog,
  queryReviewTaskDetail,
  updateReviewTaskStatus,
} from "~/lib/postgis.server";
import { Form, useActionData, useLoaderData, useNavigation } from "react-router";
```

Keep existing MUI, icon, `useState`, and `useNavigate` imports.

- [ ] **Step 2: Add loader**

Below `export const handle`, add:

```ts
export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const task = url.searchParams.get("task");
  if (!task) {
    throw new Response("Missing review task", { status: 400 });
  }

  const detail = await queryReviewTaskDetail(task).catch((error) => {
    console.warn("[PostGIS] review task unavailable:", error instanceof Error ? error.message : error);
    return null;
  });
  if (!detail) {
    throw new Response("Review task not found", { status: 404 });
  }

  return { detail };
}
```

- [ ] **Step 3: Add action**

Below the loader, add:

```ts
export async function action({ request }: Route.ActionArgs) {
  const formData = await request.formData();
  const parsed = parseAnnotationActionPayload(formData);
  if (!parsed.ok) {
    return Response.json({ ok: false, error: parsed.error }, { status: 400 });
  }

  const label = buildAnnotationLabel(parsed.payload);
  const annotationId = await insertAnnotation({
    defectId: parsed.payload.defectId,
    evidenceId: parsed.payload.evidenceId,
    label,
    annotatorId: "operator-demo",
  });

  if (parsed.payload.intent === "submit") {
    await updateReviewTaskStatus(parsed.payload.reviewTaskId, "completed");
    await insertAuditLog({
      actorId: "operator-demo",
      action: "annotation.submit",
      entityType: "review_task",
      entityId: parsed.payload.reviewTaskId,
      payload: { annotation_id: annotationId, defect_id: parsed.payload.defectId },
    });
  }

  return Response.json({ ok: true, annotationId, status: label.status });
}
```

- [ ] **Step 4: Initialize editor from loader data**

Inside `AnnotationPage`, replace `INITIAL_POLYGON` usage with loader data:

```ts
const { detail } = useLoaderData<typeof loader>();
const actionData = useActionData<typeof action>();
const navigation = useNavigation();
const initialPolygon = detail.latestAnnotation?.polygon ?? detail.originalPolygon;
const [vertices, setVertices] = useState(initialPolygon.map(([x, y]) => ({ x, y })));
const [notes, setNotes] = useState(detail.latestAnnotation?.notes ?? "");
const [annotationSource, setAnnotationSource] = useState("manual");
const isSubmitting = navigation.state !== "idle";
```

- [ ] **Step 5: Wire SAM3 assist**

Replace the current `runAutoSeg` with:

```ts
async function runAutoSeg() {
  setAutoSegRunning(true);
  try {
    const response = await fetch("/api/annotation/assist", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        review_task_id: detail.reviewTaskId,
        evidence_id: detail.evidenceId,
        image_object_key: detail.rawImageObjectKey,
        current_polygon: vertices.map((v) => [v.x, v.y]),
        prompt: "pothole",
      }),
    });
    if (!response.ok) throw new Error("SAM3 assist failed");
    const result = await response.json() as { polygon: Polygon };
    setVertices(result.polygon.map(([x, y]) => ({ x, y })));
    setAnnotationSource("sam3_assist");
  } catch (error) {
    console.warn("[SAM3 assist]", error instanceof Error ? error.message : error);
  } finally {
    setAutoSegRunning(false);
  }
}
```

- [ ] **Step 6: Render the evidence image**

Inside the SVG, replace simulated road-only background with:

```tsx
{detail.imageProxyUrl ? (
  <image href={detail.imageProxyUrl} x={0} y={0} width={600} height={400} preserveAspectRatio="xMidYMid slice" />
) : (
  <>
    <rect x={0} y={0} width={600} height={400} fill="#2A3444" />
    <line x1={0} y1={200} x2={600} y2={200} stroke="#3A4A5A" strokeWidth={60} />
    {[50, 150, 250, 350, 450, 550].map((x) => (
      <rect key={x} x={x} y={194} width={40} height={12} fill="#4A5A6A" rx={2} />
    ))}
  </>
)}
```

- [ ] **Step 7: Add hidden form fields and buttons**

Wrap the right toolbar save and submit buttons in a `<Form method="post">` and include:

```tsx
<input type="hidden" name="review_task_id" value={detail.reviewTaskId} />
<input type="hidden" name="defect_id" value={detail.defectId} />
<input type="hidden" name="evidence_id" value={detail.evidenceId ?? ""} />
<input type="hidden" name="polygon" value={JSON.stringify(vertices.map((v) => [v.x, v.y]))} />
<input type="hidden" name="source" value={annotationSource} />
<input type="hidden" name="notes" value={notes} />
```

Use submit buttons:

```tsx
<Button name="intent" value="save" type="submit" disabled={isSubmitting} size="small" variant="outlined" fullWidth sx={{ fontSize: 9, px: 0.5, minWidth: 0, color: "#8895A7", borderColor: "#2A3444" }}>
  Save
</Button>
<Button name="intent" value="submit" type="submit" disabled={isSubmitting} size="small" variant="contained" color="success" fullWidth sx={{ fontSize: 9, px: 0.5, minWidth: 0 }}>
  Submit
</Button>
```

- [ ] **Step 8: Show action errors**

In the left panel below notes, add:

```tsx
{actionData && !actionData.ok && (
  <Typography variant="caption" color="error" display="block">
    {actionData.error}
  </Typography>
)}
{actionData && actionData.ok && (
  <Typography variant="caption" color="#86EFAC" display="block">
    Annotation {actionData.status === "final" ? "submitted" : "saved"}.
  </Typography>
)}
```

- [ ] **Step 9: Run tests and typecheck**

Run:

```powershell
cd web
npm run test -- app/lib/annotation-contract.test.ts app/lib/review-contract.test.ts
npm run typecheck
```

Expected: both commands PASS.

- [ ] **Step 10: Commit**

Run:

```powershell
git add web/app/routes/_app.annotation.tsx
git commit -m "feat(web): connect annotation editor to review tasks"
```

## Task 8: Final Verification and Graph Update

**Files:**
- No new files unless verification reveals a required fix.

- [ ] **Step 1: Run focused TypeScript tests**

Run:

```powershell
cd web
npm run test -- app/lib/annotation-contract.test.ts app/lib/review-contract.test.ts
```

Expected: PASS.

- [ ] **Step 2: Run web typecheck**

Run:

```powershell
cd web
npm run typecheck
```

Expected: PASS.

- [ ] **Step 3: Run existing lakehouse contract tests if SQL changed**

Only run this if implementation touched `lakehouse/` SQL files:

```powershell
pytest tests/contract/test_lakehouse_standardization.py -q
```

Expected: PASS. If no SQL files changed, record `Not run; SQL schema unchanged`.

- [ ] **Step 4: Check worktree**

Run:

```powershell
git status --short
```

Expected: only intentional tracked changes are present. `.superpowers/` should stay ignored.

- [ ] **Step 5: Update graphify graph**

Run:

```powershell
graphify update .
```

Expected: graphify completes successfully and updates `graphify-out/` files. These files are ignored by `.gitignore`.

- [ ] **Step 6: Commit graph-independent final fixes**

If verification required code fixes, commit them:

```powershell
git add web
git commit -m "fix(web): finalize human review flow"
```

If no fixes were needed, do not create an empty commit.

## Self-review Notes

- Spec coverage: Tasks cover the review queue, manual annotation, SAM3 assist contract stub, annotation persistence, review status updates, audit insert, tests, typecheck, and `graphify update .`.
- Scope control: Edge schema, real SAM3 subprocess execution, model retraining, and dataset export remain out of scope.
- Type consistency: The plan consistently uses `reviewTaskId` in TypeScript, `review_task_id` in form/API JSON, `AnnotationSource` values from the spec, and `ReviewTaskStatus` values `pending`, `in_progress`, and `completed`.
