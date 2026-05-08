# Human-in-the-loop annotation design

## Context

The pothole monitoring system already sends edge detections through Kafka, lakehouse processing, PostGIS serving tables, and the React Router web dashboard. Edge raw events include `detection_confidence`, raw image object keys, and original mask polygons. The web app already has draft `/lowconf` and `/annotation` pages, PostGIS access helpers, and an image proxy for MinIO-backed evidence.

The approved direction is **flag and correct**: low-confidence detections remain visible in the system with review state, while operators can correct segmentation masks through an annotation page. The first implementation will be demo-ready and UI-first, with a deterministic SAM3 assist contract stub that can later be replaced by a dedicated SAM3 service.

## Goals

- Show low-confidence edge detections in an operator review queue.
- Let operators manually segment or refine a pothole mask on the annotation page.
- Provide a SAM3 assist action that returns candidate segmentation polygons using a stable request and response contract.
- Persist annotation drafts and final submissions in PostGIS `serving.annotations`.
- Track review lifecycle through PostGIS `serving.review_tasks`.
- Keep the feature compatible with a future real SAM3 microservice.

## Non-goals

- Do not change the edge raw event Avro schema in this iteration.
- Do not block low-confidence detections from the dashboard.
- Do not invoke the full SAM3 Python/CUDA stack from the web runtime yet.
- Do not build model retraining or dataset export flows in this iteration.

## Architecture

The feature is implemented in the web/lakehouse serving layer.

- `web/app/lib/postgis.server.ts` gains review queue, review detail, annotation write, task status update, and audit log helpers.
- `/lowconf` reads real review queue data instead of mock cards.
- `/annotation` loads one review task and evidence image, supports polygon editing, and submits annotation actions.
- The `/annotation` route action handles annotation save and submit requests.
- A resource route handles SAM3 assist requests.
- The SAM3 assist endpoint returns deterministic polygons and metadata now, matching the contract expected from a future service.

Low-confidence threshold defaults to `0.6` through web configuration. Rows below that threshold are eligible for review. Existing `serving.review_tasks` rows are used when present; low-confidence road defects without a completed task can be surfaced as queue candidates so the demo works even if task creation is not yet automated upstream.

## Data flow

1. Edge uploads raw events with `detection_confidence`, image object key, and original mask.
2. Existing lakehouse and PostGIS projection exposes road defects, confidence, evidence rows, and latest image object keys.
3. `/lowconf` queries pending or in-progress review tasks plus low-confidence road defects that need review.
4. Operators open `/annotation?task=<review_task_id>`.
5. The annotation page loads task metadata, evidence image URL, original mask, and the latest draft annotation if one exists.
6. The operator edits polygon vertices manually or requests SAM3 assist.
7. SAM3 assist returns candidate polygon, assist confidence, and model metadata.
8. Save draft writes a `serving.annotations` row with `label_json.status = "draft"`.
9. Submit writes a final annotation row, updates `serving.review_tasks.status = "completed"`, and appends an audit log entry.

## Data contracts

Review task statuses:

- `pending`
- `in_progress`
- `completed`

Annotation `label_json` shape:

```json
{
  "status": "draft",
  "polygon": [[200, 120], [350, 100], [380, 230], [220, 260]],
  "source": "manual",
  "notes": "operator note",
  "confidence": 0.82,
  "model": {
    "name": "sam3-assist-contract-stub",
    "version": "demo-contract-v1"
  },
  "created_from": {
    "review_task_id": "task-123",
    "evidence_id": "event-123"
  }
}
```

Valid `source` values:

- `manual`
- `sam3_assist`
- `manual_refined_sam3`

SAM3 assist request:

```json
{
  "review_task_id": "task-123",
  "evidence_id": "event-123",
  "image_object_key": "s3://warehouse/raw_images/event-123.jpg",
  "current_polygon": [[200, 120], [350, 100], [380, 230], [220, 260]],
  "prompt": "pothole"
}
```

SAM3 assist response:

```json
{
  "polygon": [[198, 118], [354, 104], [382, 232], [218, 258]],
  "confidence": 0.74,
  "source": "sam3_assist",
  "model": {
    "name": "sam3-assist-contract-stub",
    "version": "demo-contract-v1"
  }
}
```

## UI behavior

`/lowconf`:

- Shows confidence range, severity, and task status filters.
- Displays raw image preview, current confidence, severity, location, task status, and reason.
- Opens annotation with a review task identifier.
- Shows an empty state when PostGIS is unavailable or no tasks match.

`/annotation`:

- Keeps the existing dark editor layout.
- Loads real task data through the route loader.
- Shows evidence image through the existing image proxy when available.
- Lets operators drag vertices, reset to original mask, run SAM3 assist, save draft, and submit.
- Labels SAM3 as an assist action, not ground truth.
- Keeps the current polygon unchanged if SAM3 assist fails.

## Error handling

- PostGIS query failures are caught and shown as empty or unavailable states.
- Missing evidence images do not block annotation metadata from loading.
- Invalid polygons are rejected before writes.
- Annotation submit requires at least three polygon points.
- Failed save or submit returns a route action error without mutating task status.
- SAM3 assist failures show an inline error and keep the existing operator polygon.

## Testing

- Add focused tests for PostGIS review queue SQL and low-confidence filtering.
- Add tests for annotation label validation and SAM3 assist response shape.
- Add pure helper tests for annotation action parsing and validation.
- Update contract tests if serving schema changes are required.
- Run `npm run typecheck` in `web`.
- Run focused Python contract tests if SQL or lakehouse schema files change.
- After code changes, run `graphify update .` to refresh the project graph.

## Implementation sequence

1. Add typed review and annotation helpers in `postgis.server.ts`.
2. Replace `/lowconf` mock data with loader data and queue filters.
3. Convert `/annotation` to loader/action data while preserving the current editor interactions.
4. Add deterministic SAM3 assist endpoint or action.
5. Add focused tests and typecheck.
6. Run `graphify update .` after code changes.
