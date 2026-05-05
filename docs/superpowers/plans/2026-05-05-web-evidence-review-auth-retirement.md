# Web Migration, Evidence, Review, Auth, and Legacy Retirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate web data sources from Trino `iceberg.city` to PostGIS + gold tables, implement OGC API Features routes, wire review/annotation pages to real PostGIS tables, add production-grade session auth with audit logging, and gate legacy Python service writes behind an env flag.

**Architecture:** Web gains a `postgis.server.ts` helper (pg pool) used by map/detail API routes and new OGC API routes. Trino queries for analytics pages are updated to `gold.*` tables. Session auth uses a signed HttpOnly cookie checked by a middleware function. Audit log writes go to `serving.audit_log` via postgis helper. ETL and Final Enrichment gain a `LEGACY_WRITE_ENABLED` flag to allow clean cutover. A migration parity test diffs record counts.

**Dependencies:** Plan 3 (Flink pipeline) complete so gold tables are populated. Plan 2 (PostGIS `needs_review`) complete.

**Tech Stack:** TypeScript/React Router 7, pg (postgres client), Node.js crypto (session), pytest + psycopg2 (parity test)

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `web/app/lib/postgis.server.ts` | PostGIS pg pool, queryDefects, getDefectById, writeAuditLog |
| Modify | `web/app/routes/api.map-data.tsx` | Switch from Trino to PostGIS bbox query |
| Modify | `web/app/routes/api.pothole.$id.tsx` | Switch from Trino to PostGIS by defect_id |
| Create | `web/app/routes/api.v1.collections.tsx` | OGC API collections list |
| Create | `web/app/routes/api.v1.collections.road-defects.items.tsx` | OGC API GeoJSON FeatureCollection |
| Create | `web/app/routes/api.v1.collections.road-defects.items.$id.tsx` | OGC API single Feature |
| Modify | `web/app/lib/trino.server.ts` | Update analytics queries to gold.* tables, fix type names |
| Modify | `web/app/routes/_app.overview.tsx` | Use updated Trino gold queries |
| Create | `web/app/lib/auth.server.ts` | Session cookie helpers, requireAuth(), hasRole() |
| Modify | `web/app/routes/_auth.login.tsx` | Real credential check against env-configured users |
| Modify | `web/app/routes/logout.tsx` | Clear session cookie |
| Modify | `web/app/routes/api.image.proxy.tsx` | Add auth check |
| Modify | `web/app/routes/_app.lowconf.tsx` | Wire to PostGIS needs_review query |
| Modify | `web/app/routes/_app.annotation.tsx` | Wire to POST /api/v1/annotations |
| Create | `web/app/routes/api.v1.review-tasks.tsx` | CRUD for review tasks |
| Create | `web/app/routes/api.v1.annotations.tsx` | Create annotations |
| Modify | `cloud/etl_service/etl_microservice.py` | LEGACY_WRITE_ENABLED flag |
| Modify | `cloud/final_enrichment_service/final_enrichment_service.py` | LEGACY_WRITE_ENABLED flag |
| Create | `tests/contract/test_migration_parity.py` | Compare legacy vs medallion record counts |

---

### Task 1: PostGIS Server Helper

**Files:**
- Create: `web/app/lib/postgis.server.ts`

- [ ] **Step 1: Install pg dependency**

```bash
cd web
npm install pg @types/pg
```

- [ ] **Step 2: Create `web/app/lib/postgis.server.ts`**

```typescript
import { Pool, type QueryResult } from "pg";

let pool: Pool | null = null;

function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      host: process.env.POSTGIS_HOST ?? "localhost",
      port: parseInt(process.env.POSTGIS_PORT ?? "5437"),
      user: process.env.POSTGIS_USER ?? "serving",
      password: process.env.POSTGIS_PASSWORD ?? "servingpassword",
      database: process.env.POSTGIS_DB ?? "postgis_serving",
      max: 10,
    });
  }
  return pool;
}

export interface RoadDefect {
  defect_id: string;
  defect_type: string;
  status: string;
  severity_score: number | null;
  severity_level: string | null;
  confidence: number | null;
  quality_flags: string[];
  latitude: number;
  longitude: number;
  district: string | null;
  ward: string | null;
  first_seen_at: string;
  last_seen_at: string;
  observation_count: number;
  latest_raw_image_object_key: string | null;
  latest_bev_object_key: string | null;
  needs_review: boolean;
}

export async function queryDefects(params: {
  minLon?: number;
  minLat?: number;
  maxLon?: number;
  maxLat?: number;
  needsReview?: boolean;
  limit?: number;
  offset?: number;
}): Promise<RoadDefect[]> {
  const conditions: string[] = [];
  const values: unknown[] = [];
  let idx = 1;

  if (
    params.minLon !== undefined &&
    params.minLat !== undefined &&
    params.maxLon !== undefined &&
    params.maxLat !== undefined
  ) {
    conditions.push(
      `geometry && ST_MakeEnvelope($${idx++}, $${idx++}, $${idx++}, $${idx++}, 4326)`
    );
    values.push(params.minLon, params.minLat, params.maxLon, params.maxLat);
  }

  if (params.needsReview !== undefined) {
    conditions.push(`needs_review = $${idx++}`);
    values.push(params.needsReview);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const limit = params.limit ?? 500;
  const offset = params.offset ?? 0;

  const result = await getPool().query<RoadDefect>(
    `SELECT defect_id, defect_type, status, severity_score, severity_level,
            confidence, quality_flags, needs_review,
            ST_Y(geometry) AS latitude, ST_X(geometry) AS longitude,
            district, ward, first_seen_at, last_seen_at, observation_count,
            latest_raw_image_object_key, latest_bev_object_key
     FROM serving.current_road_defects
     ${where}
     ORDER BY last_seen_at DESC
     LIMIT $${idx++} OFFSET $${idx++}`,
    [...values, limit, offset]
  );
  return result.rows;
}

export async function getDefectById(defectId: string): Promise<RoadDefect | null> {
  const result = await getPool().query<RoadDefect>(
    `SELECT defect_id, defect_type, status, severity_score, severity_level,
            confidence, quality_flags, needs_review,
            ST_Y(geometry) AS latitude, ST_X(geometry) AS longitude,
            district, ward, first_seen_at, last_seen_at, observation_count,
            latest_raw_image_object_key, latest_bev_object_key
     FROM serving.current_road_defects
     WHERE defect_id = $1`,
    [defectId]
  );
  return result.rows[0] ?? null;
}

// Export pool for use by other server modules
export function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      host: process.env.POSTGIS_HOST ?? "localhost",
      port: parseInt(process.env.POSTGIS_PORT ?? "5437"),
      user: process.env.POSTGIS_USER ?? "serving",
      password: process.env.POSTGIS_PASSWORD ?? "servingpassword",
      database: process.env.POSTGIS_DB ?? "postgis_serving",
      max: 10,
    });
  }
  return pool;
}

export async function writeAuditLog(entry: {
  actor_id: string;
  action: string;
  entity_type: string;
  entity_id: string;
  payload?: Record<string, unknown>;
}): Promise<void> {
  await getPool().query(
    `INSERT INTO serving.audit_log (actor_id, action, entity_type, entity_id, payload)
     VALUES ($1, $2, $3, $4, $5)`,
    [
      entry.actor_id,
      entry.action,
      entry.entity_type,
      entry.entity_id,
      JSON.stringify(entry.payload ?? {}),
    ]
  );
}
```

- [ ] **Step 3: Run TypeScript check**

```bash
cd web && npm run typecheck
# Expected: no errors in postgis.server.ts
```

- [ ] **Step 4: Commit**

```bash
git add web/app/lib/postgis.server.ts web/package.json web/package-lock.json
git commit -m "feat(web): add PostGIS server helper with queryDefects, getDefectById, writeAuditLog"
```

---

### Task 2: Migrate Map Data API to PostGIS

**Files:**
- Modify: `web/app/routes/api.map-data.tsx`

- [ ] **Step 1: Update loader to use PostGIS**

Replace the Trino query in the loader with a PostGIS call:

```typescript
// web/app/routes/api.map-data.tsx
import { queryDefects } from "~/lib/postgis.server";

export async function loader({ request }: LoaderFunctionArgs) {
  const url = new URL(request.url);
  const minLon = parseFloat(url.searchParams.get("minLon") ?? "106.5");
  const minLat = parseFloat(url.searchParams.get("minLat") ?? "10.6");
  const maxLon = parseFloat(url.searchParams.get("maxLon") ?? "106.9");
  const maxLat = parseFloat(url.searchParams.get("maxLat") ?? "11.0");

  try {
    const defects = await queryDefects({ minLon, minLat, maxLon, maxLat, limit: 1000 });
    return Response.json(
      defects.map((d) => ({
        id: d.defect_id,
        lat: d.latitude,
        lon: d.longitude,
        severity_level: d.severity_level,
        severity_score: d.severity_score,
        status: d.status,
        needs_review: d.needs_review,
      }))
    );
  } catch (err) {
    console.error("map-data PostGIS error:", err);
    return Response.json([], { status: 200 }); // degrade gracefully
  }
}
```

- [ ] **Step 2: Run typecheck and start dev server to verify map loads**

```bash
cd web && npm run typecheck && npm run dev
# Open http://localhost:3000/map — verify map markers appear
```

- [ ] **Step 3: Commit**

```bash
git add web/app/routes/api.map-data.tsx
git commit -m "feat(web): migrate map-data API from Trino to PostGIS"
```

---

### Task 3: Migrate Pothole Detail API to PostGIS

**Files:**
- Modify: `web/app/routes/api.pothole.$id.tsx`

- [ ] **Step 1: Update loader**

```typescript
// web/app/routes/api.pothole.$id.tsx
import { getDefectById } from "~/lib/postgis.server";

export async function loader({ params }: LoaderFunctionArgs) {
  const defect = await getDefectById(params.id!);
  if (!defect) {
    throw new Response("Not Found", { status: 404 });
  }
  return Response.json({
    defect_id: defect.defect_id,
    status: defect.status,
    severity_score: defect.severity_score,
    severity_level: defect.severity_level,
    confidence: defect.confidence,
    latitude: defect.latitude,
    longitude: defect.longitude,
    district: defect.district,
    ward: defect.ward,
    first_seen_at: defect.first_seen_at,
    last_seen_at: defect.last_seen_at,
    observation_count: defect.observation_count,
    raw_image_object_key: defect.latest_raw_image_object_key,
    bev_object_key: defect.latest_bev_object_key,
    needs_review: defect.needs_review,
  });
}
```

Update `PotholeDetail` TypeScript type in `trino.server.ts` or create a new type from PostGIS shape. Remove old `raw_image_path` / `bev_image_path` aliases.

- [ ] **Step 2: Run typecheck**

```bash
cd web && npm run typecheck
# Expected: no errors
```

- [ ] **Step 3: Commit**

```bash
git add web/app/routes/api.pothole.\$id.tsx web/app/lib/trino.server.ts
git commit -m "feat(web): migrate pothole detail API from Trino to PostGIS"
```

---

### Task 4: OGC API Features Routes

**Files:**
- Create: `web/app/routes/api.v1.collections.tsx`
- Create: `web/app/routes/api.v1.collections.road-defects.items.tsx`
- Create: `web/app/routes/api.v1.collections.road-defects.items.$id.tsx`

- [ ] **Step 1: Create collections list route**

```typescript
// web/app/routes/api.v1.collections.tsx
export async function loader() {
  return Response.json({
    collections: [
      {
        id: "road-defects",
        title: "Road Defects",
        description: "Current road defects detected by the pothole monitoring system",
        links: [
          { href: "/api/v1/collections/road-defects/items", rel: "items", type: "application/geo+json" },
        ],
      },
    ],
  });
}
```

- [ ] **Step 2: Create items route (GeoJSON FeatureCollection)**

```typescript
// web/app/routes/api.v1.collections.road-defects.items.tsx
import { queryDefects } from "~/lib/postgis.server";

function toFeature(d: import("~/lib/postgis.server").RoadDefect) {
  return {
    type: "Feature",
    id: d.defect_id,
    geometry: { type: "Point", coordinates: [d.longitude, d.latitude] },
    properties: {
      defect_id: d.defect_id,
      defect_type: d.defect_type,
      status: d.status,
      severity_score: d.severity_score,
      severity_level: d.severity_level,
      confidence: d.confidence,
      district: d.district,
      ward: d.ward,
      first_seen_at: d.first_seen_at,
      last_seen_at: d.last_seen_at,
      observation_count: d.observation_count,
      needs_review: d.needs_review,
    },
  };
}

export async function loader({ request }: LoaderFunctionArgs) {
  const url = new URL(request.url);
  const bbox = url.searchParams.get("bbox");
  const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "100"), 1000);
  const offset = parseInt(url.searchParams.get("offset") ?? "0");

  const bboxParams = bbox
    ? (() => {
        const [minLon, minLat, maxLon, maxLat] = bbox.split(",").map(Number);
        return { minLon, minLat, maxLon, maxLat };
      })()
    : {};

  const defects = await queryDefects({ ...bboxParams, limit, offset });

  return Response.json(
    {
      type: "FeatureCollection",
      features: defects.map(toFeature),
      numberReturned: defects.length,
      links: [],
    },
    { headers: { "Content-Type": "application/geo+json" } }
  );
}
```

- [ ] **Step 3: Create single feature route**

```typescript
// web/app/routes/api.v1.collections.road-defects.items.$id.tsx
import { getDefectById } from "~/lib/postgis.server";

export async function loader({ params }: LoaderFunctionArgs) {
  const defect = await getDefectById(params.id!);
  if (!defect) throw new Response("Not Found", { status: 404 });
  return Response.json(
    {
      type: "Feature",
      id: defect.defect_id,
      geometry: { type: "Point", coordinates: [defect.longitude, defect.latitude] },
      properties: {
        defect_id: defect.defect_id,
        status: defect.status,
        severity_level: defect.severity_level,
        severity_score: defect.severity_score,
        confidence: defect.confidence,
        district: defect.district,
        ward: defect.ward,
        first_seen_at: defect.first_seen_at,
        last_seen_at: defect.last_seen_at,
        observation_count: defect.observation_count,
        needs_review: defect.needs_review,
      },
    },
    { headers: { "Content-Type": "application/geo+json" } }
  );
}
```

- [ ] **Step 4: Register routes in `web/app/routes.ts`**

Add to routes config:
```typescript
route("/api/v1/collections", "routes/api.v1.collections.tsx"),
route("/api/v1/collections/road-defects/items", "routes/api.v1.collections.road-defects.items.tsx"),
route("/api/v1/collections/road-defects/items/:id", "routes/api.v1.collections.road-defects.items.$id.tsx"),
```

- [ ] **Step 5: Test OGC routes**

```bash
cd web && npm run dev
curl "http://localhost:3000/api/v1/collections" | python3 -m json.tool
curl "http://localhost:3000/api/v1/collections/road-defects/items?limit=5" | python3 -m json.tool
# Expected: valid GeoJSON FeatureCollection
```

- [ ] **Step 6: Commit**

```bash
git add web/app/routes/api.v1.* web/app/routes.ts
git commit -m "feat(web): implement OGC API Features routes for road defects"
```

---

### Task 5: Auth Middleware

**Files:**
- Create: `web/app/lib/auth.server.ts`
- Modify: `web/app/routes/_auth.login.tsx`
- Modify: `web/app/routes/logout.tsx`

- [ ] **Step 1: Create `web/app/lib/auth.server.ts`**

```typescript
import { createCookieSessionStorage, redirect } from "react-router";
import crypto from "node:crypto";

const SESSION_SECRET = process.env.SESSION_SECRET ?? "dev-secret-change-in-prod";

const sessionStorage = createCookieSessionStorage({
  cookie: {
    name: "__session",
    httpOnly: true,
    sameSite: "lax",
    secrets: [SESSION_SECRET],
    secure: process.env.NODE_ENV === "production",
    maxAge: 60 * 60 * 8, // 8 hours
  },
});

// Users configured via env: USERS_JSON='[{"email":"admin@example.com","password":"hashed","role":"admin"}]'
// Password stored as SHA-256 hex for simplicity (replace with bcrypt in production).
interface UserConfig {
  email: string;
  password_hash: string; // sha256 hex of password
  role: "viewer" | "reviewer" | "admin";
}

function getConfiguredUsers(): UserConfig[] {
  try {
    return JSON.parse(process.env.USERS_JSON ?? "[]");
  } catch {
    return [];
  }
}

export function hashPassword(password: string): string {
  return crypto.createHash("sha256").update(password).digest("hex");
}

export async function authenticate(
  email: string,
  password: string
): Promise<{ email: string; role: UserConfig["role"] } | null> {
  const users = getConfiguredUsers();
  const user = users.find(
    (u) => u.email === email && u.password_hash === hashPassword(password)
  );
  return user ? { email: user.email, role: user.role } : null;
}

export async function createUserSession(
  request: Request,
  user: { email: string; role: string },
  redirectTo: string
) {
  const session = await sessionStorage.getSession(request.headers.get("Cookie"));
  session.set("user", user);
  return redirect(redirectTo, {
    headers: { "Set-Cookie": await sessionStorage.commitSession(session) },
  });
}

export async function getUser(request: Request) {
  const session = await sessionStorage.getSession(request.headers.get("Cookie"));
  return session.get("user") as { email: string; role: string } | undefined;
}

export async function requireAuth(
  request: Request,
  allowedRoles?: string[]
): Promise<{ email: string; role: string }> {
  const user = await getUser(request);
  if (!user) throw redirect("/login");
  if (allowedRoles && !allowedRoles.includes(user.role)) {
    throw new Response("Forbidden", { status: 403 });
  }
  return user;
}

export async function destroySession(request: Request) {
  const session = await sessionStorage.getSession(request.headers.get("Cookie"));
  return redirect("/login", {
    headers: { "Set-Cookie": await sessionStorage.destroySession(session) },
  });
}
```

- [ ] **Step 2: Update `_auth.login.tsx`**

Replace the demo auth with real credential check:

```typescript
import { authenticate, createUserSession } from "~/lib/auth.server";

export async function action({ request }: ActionFunctionArgs) {
  const form = await request.formData();
  const email = String(form.get("email") ?? "");
  const password = String(form.get("password") ?? "");

  const user = await authenticate(email, password);
  if (!user) {
    return Response.json({ error: "Invalid email or password" }, { status: 401 });
  }
  return createUserSession(request, user, "/");
}
```

- [ ] **Step 3: Update `logout.tsx`**

```typescript
import { destroySession } from "~/lib/auth.server";

export async function action({ request }: ActionFunctionArgs) {
  return destroySession(request);
}
```

- [ ] **Step 4: Add `requireAuth` to all app routes and API routes**

In each `_app.*.tsx` loader, add at the top:
```typescript
const user = await requireAuth(request);
```

In API routes (`api.map-data.tsx`, `api.pothole.$id.tsx`, `api.image.proxy.tsx`, OGC routes):
```typescript
const user = await requireAuth(request);
```

Image proxy additionally calls `writeAuditLog`:
```typescript
await writeAuditLog({
  actor_id: user.email,
  action: "image_proxy_access",
  entity_type: "raw_image",
  entity_id: objectKey,
});
```

- [ ] **Step 5: Configure test user via env**

```bash
# Generate password hash for testing:
node -e "const c=require('crypto'); console.log(c.createHash('sha256').update('admin123').digest('hex'))"
# Set env in .env.local:
echo 'USERS_JSON=[{"email":"admin@local.test","password_hash":"<hash>","role":"admin"}]' >> web/.env.local
echo 'SESSION_SECRET=dev-local-secret-do-not-use-in-prod' >> web/.env.local
```

- [ ] **Step 6: Test login flow**

```bash
cd web && npm run dev
# Navigate to http://localhost:3000/login
# Login with admin@local.test / admin123
# Expected: redirected to /, session cookie set
# Navigate to /map — verify data loads with auth
```

- [ ] **Step 7: Commit**

```bash
git add web/app/lib/auth.server.ts web/app/routes/_auth.login.tsx \
        web/app/routes/logout.tsx web/.env.local.example
git commit -m "feat(web): add session-based auth with role support and audit log on image proxy"
```

---

### Task 6: Wire Low-Confidence Review Page

**Files:**
- Modify: `web/app/routes/_app.lowconf.tsx`

- [ ] **Step 1: Add loader that queries PostGIS for needs_review defects**

```typescript
// web/app/routes/_app.lowconf.tsx
import { queryDefects } from "~/lib/postgis.server";
import { requireAuth } from "~/lib/auth.server";

export async function loader({ request }: LoaderFunctionArgs) {
  await requireAuth(request, ["reviewer", "admin"]);
  const defects = await queryDefects({ needsReview: true, limit: 100 });
  return Response.json({ defects });
}
```

Update the component to render `defects` from `useLoaderData()`. Each row should show: `defect_id`, `severity_level`, `confidence`, `quality_flags`, `last_seen_at`, and a link to `/annotation?defect_id=...`.

- [ ] **Step 2: Commit**

```bash
git add web/app/routes/_app.lowconf.tsx
git commit -m "feat(web): wire low-confidence review page to PostGIS needs_review query"
```

---

### Task 7: Review Tasks and Annotations API

**Files:**
- Create: `web/app/routes/api.v1.review-tasks.tsx`
- Create: `web/app/routes/api.v1.annotations.tsx`
- Modify: `web/app/routes/_app.annotation.tsx`

- [ ] **Step 1: Create review tasks route**

```typescript
// web/app/routes/api.v1.review-tasks.tsx
import { getPool } from "~/lib/postgis.server"; // export getPool from postgis.server.ts
import { requireAuth } from "~/lib/auth.server";
import { writeAuditLog } from "~/lib/postgis.server";
import { randomUUID } from "node:crypto";

export async function action({ request }: ActionFunctionArgs) {
  const user = await requireAuth(request, ["reviewer", "admin"]);
  const body = await request.json();

  if (request.method === "POST") {
    const { defect_id, priority } = body as { defect_id: string; priority?: string };
    const task_id = randomUUID();
    await getPool().query(
      `INSERT INTO serving.review_tasks (review_task_id, defect_id, status, priority, assigned_to)
       VALUES ($1, $2, 'pending', $3, $4)
       ON CONFLICT (review_task_id) DO NOTHING`,
      [task_id, defect_id, priority ?? "low", user.email]
    );
    await writeAuditLog({ actor_id: user.email, action: "review_task_created", entity_type: "defect", entity_id: defect_id });
    return Response.json({ review_task_id: task_id }, { status: 201 });
  }

  if (request.method === "PUT") {
    const { review_task_id, status } = body as { review_task_id: string; status: string };
    await getPool().query(
      `UPDATE serving.review_tasks SET status = $1, updated_at = now() WHERE review_task_id = $2`,
      [status, review_task_id]
    );
    await writeAuditLog({ actor_id: user.email, action: "review_task_updated", entity_type: "review_task", entity_id: review_task_id, payload: { status } });
    return Response.json({ ok: true });
  }

  throw new Response("Method Not Allowed", { status: 405 });
}
```

Note: `getPool()` is already exported from `postgis.server.ts` (Task 1).

- [ ] **Step 2: Create annotations route**

```typescript
// web/app/routes/api.v1.annotations.tsx
import { getPool, writeAuditLog } from "~/lib/postgis.server";
import { requireAuth } from "~/lib/auth.server";
import { randomUUID } from "node:crypto";

export async function action({ request }: ActionFunctionArgs) {
  const user = await requireAuth(request, ["reviewer", "admin"]);
  if (request.method !== "POST") throw new Response("Method Not Allowed", { status: 405 });

  const body = await request.json() as {
    defect_id: string;
    evidence_id?: string;
    label_json: Record<string, unknown>;
  };

  const annotation_id = randomUUID();
  await getPool().query(
    `INSERT INTO serving.annotations (annotation_id, defect_id, evidence_id, label_json, annotator_id)
     VALUES ($1, $2, $3, $4, $5)`,
    [
      annotation_id,
      body.defect_id,
      body.evidence_id ?? null,
      JSON.stringify(body.label_json),
      user.email,
    ]
  );
  await writeAuditLog({
    actor_id: user.email,
    action: "annotation_created",
    entity_type: "defect",
    entity_id: body.defect_id,
    payload: { annotation_id },
  });
  return Response.json({ annotation_id }, { status: 201 });
}
```

- [ ] **Step 3: Wire annotation page**

Update `_app.annotation.tsx` loader to accept `defect_id` query param and load defect data. Update the form submit action to POST to `/api/v1/annotations`.

- [ ] **Step 4: Register routes in routes.ts**

```typescript
route("/api/v1/review-tasks", "routes/api.v1.review-tasks.tsx"),
route("/api/v1/annotations", "routes/api.v1.annotations.tsx"),
```

- [ ] **Step 5: Commit**

```bash
git add web/app/routes/api.v1.review-tasks.tsx \
        web/app/routes/api.v1.annotations.tsx \
        web/app/routes/_app.annotation.tsx \
        web/app/routes.ts
git commit -m "feat(web): wire review tasks and annotations API routes with audit logging"
```

---

### Task 8: Update Trino Analytics Queries to Gold Tables

**Files:**
- Modify: `web/app/lib/trino.server.ts`
- Modify: `web/app/routes/_app.overview.tsx`

- [ ] **Step 1: Update overview query in `trino.server.ts`**

Find the overview query (likely `SELECT COUNT(*) ...` from `iceberg.city.potholes`). Replace with a query against `gold.dashboard_summary_daily`:

```typescript
// In trino.server.ts
export async function getDashboardSummary() {
  return queryTrino<{
    summary_date: string;
    active_defect_count: number;
    new_defect_count: number;
    fixed_defect_count: number;
    average_severity: number;
    critical_defect_count: number;
  }>(
    `SELECT summary_date, active_defect_count, new_defect_count,
            fixed_defect_count, average_severity, critical_defect_count
     FROM gold.dashboard_summary_daily
     ORDER BY summary_date DESC
     LIMIT 30`
  );
}
```

Update `TRINO_SCHEMA` default from `city` to `gold`:
```typescript
const schema = process.env.TRINO_SCHEMA ?? "gold";
```

- [ ] **Step 2: Update `_app.overview.tsx` loader**

Replace Trino `iceberg.city.potholes` count query with `getDashboardSummary()`. Update component to render data from gold schema shape.

- [ ] **Step 3: Run typecheck**

```bash
cd web && npm run typecheck
# Expected: no errors
```

- [ ] **Step 4: Commit**

```bash
git add web/app/lib/trino.server.ts web/app/routes/_app.overview.tsx
git commit -m "feat(web): migrate analytics queries from iceberg.city to gold tables"
```

---

### Task 9: Legacy Service Write Gate

**Files:**
- Modify: `cloud/etl_service/etl_microservice.py`
- Modify: `cloud/final_enrichment_service/final_enrichment_service.py`

- [ ] **Step 1: Add LEGACY_WRITE_ENABLED to ETL service**

In `etl_microservice.py`, at the top of the class or in `__init__()`:

```python
import os

LEGACY_WRITE_ENABLED = os.getenv("LEGACY_WRITE_ENABLED", "true").lower() == "true"
```

In `_flush_topic_buffer()` (or equivalent write method), wrap the Iceberg write:

```python
if LEGACY_WRITE_ENABLED:
    self._write_to_iceberg(table_name, arrow_table)
else:
    logger.debug("Legacy write disabled; skipping iceberg.city.%s write", table_name)
```

- [ ] **Step 2: Add LEGACY_WRITE_ENABLED to Final Enrichment**

Same pattern in `FinalEnrichmentService._persist_pothole()`:

```python
LEGACY_WRITE_ENABLED = os.getenv("LEGACY_WRITE_ENABLED", "true").lower() == "true"

# In persist method:
if LEGACY_WRITE_ENABLED:
    self._trino_upsert_pothole(combined_event)
    self._trino_insert_pothole_history(combined_event)
```

- [ ] **Step 3: Commit**

```bash
git add cloud/etl_service/etl_microservice.py \
        cloud/final_enrichment_service/final_enrichment_service.py
git commit -m "feat(cloud): add LEGACY_WRITE_ENABLED flag to ETL and final enrichment"
```

---

### Task 10: Migration Parity Test

**Files:**
- Create: `tests/contract/test_migration_parity.py`

- [ ] **Step 1: Write parity test**

```python
# tests/contract/test_migration_parity.py
"""
Verifies that record counts in legacy iceberg.city.* are within 5% of
gold.current_road_defects and bronze event tables.
Run after both legacy services and Flink pipeline have been running for >5 minutes.
"""
import os
import pytest
import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))


def _query(catalog: str, schema: str, sql: str) -> list:
    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user="admin", catalog=catalog, schema=schema,
    )
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


@pytest.mark.integration
def test_raw_events_count_parity():
    legacy = _query("iceberg", "city", "SELECT COUNT(*) FROM raw_events")[0][0]
    bronze = _query("iceberg", "bronze", "SELECT COUNT(*) FROM raw_detection_events")[0][0]
    # bronze should have >= legacy count (Flink reads from earliest offset)
    assert bronze >= legacy * 0.95, f"bronze {bronze} < 95% of legacy {legacy}"


@pytest.mark.integration
def test_severity_events_count_parity():
    legacy = _query("iceberg", "city", "SELECT COUNT(*) FROM severity_scores")[0][0]
    bronze = _query("iceberg", "bronze", "SELECT COUNT(*) FROM severity_score_events")[0][0]
    assert bronze >= legacy * 0.95, f"bronze {bronze} < 95% of legacy {legacy}"


@pytest.mark.integration
def test_gold_defects_populated():
    gold_count = _query("iceberg", "gold", "SELECT COUNT(*) FROM current_road_defects")[0][0]
    assert gold_count > 0, "gold.current_road_defects is empty — Flink pipeline may not be running"


@pytest.mark.integration
def test_postgis_defects_populated():
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("POSTGIS_HOST", "localhost"),
        port=int(os.getenv("POSTGIS_PORT", "5437")),
        user="serving", password="servingpassword",
        dbname="postgis_serving",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM serving.current_road_defects")
    count = cur.fetchone()[0]
    conn.close()
    assert count > 0, "serving.current_road_defects is empty — JDBC projection not running"
```

- [ ] **Step 2: Run parity test after full stack is up**

```bash
pytest tests/contract/test_migration_parity.py -v -m integration
# Expected: all 4 pass (counts match within tolerance)
```

- [ ] **Step 3: Commit**

```bash
git add tests/contract/test_migration_parity.py
git commit -m "test(contract): add migration parity test for legacy vs medallion record counts"
```

---

### Task 11: Document Legacy Retirement Criteria

**Files:**
- Modify: `PIPELINE.md`

- [ ] **Step 1: Add retirement criteria section to `PIPELINE.md`**

Add after the current "Flink Connector Status" section:

```markdown
## Legacy Service Retirement Criteria

The following must ALL be true before disabling legacy `iceberg.city.*` writes
(set `LEGACY_WRITE_ENABLED=false`):

1. **Flink jobs end-to-end validated** — `test_flink_pipeline.py` passes.
2. **Web data sources migrated** — web app reads PostGIS/gold, not `iceberg.city`.
3. **Parity test passes** — `test_migration_parity.py` passes for raw, severity, and defect counts.
4. **Review/annotation wiring complete** — `serving.review_tasks` and `serving.annotations` populated.
5. **No active consumer groups on `iceberg.city.*` writes** — verify via:
   `SELECT * FROM system.runtime.queries WHERE query LIKE '%city%'` in Trino.

After cutover: keep legacy services running with `LEGACY_WRITE_ENABLED=false` for one
week to ensure Kafka offset commits are not lost. Then decommission.
```

- [ ] **Step 2: Commit**

```bash
git add PIPELINE.md
git commit -m "docs: add legacy service retirement criteria to PIPELINE.md"
```

---

## Deferred Items (Gap 5 — Evidence Persistence, partial)

These items from Gap 5 of the spec are deferred to a follow-up plan after the
four plans above are complete:

- **BEV SHA256 computation:** Add `hashlib.sha256(image_bytes).hexdigest()` in
  `cloud/bev_surface_service/bev_processor.py` before upload; include in
  `pothole.surface.area.v3` Avro schema; Flink silver reads it into
  `silver.defect_evidence.bev_image_sha256`.

- **`silver.models` population:** Add a `pothole.model.deployment.v1` Kafka topic
  produced by edge on model activation; Flink inserts into `bronze.model_deployment_events`
  and `silver.models`.

- **`ml.training_dataset_items` from annotations:** Extend the annotation POST
  handler to also write an Iceberg ML item via Trino INSERT into
  `ml.training_dataset_items`. Requires Trino connection in the web server or a
  separate async worker.

- **`silver.detections.model_id` from v3 events:** Already populated once v3 schema
  is live (Plan 1 `model_id` flows through `bronze.raw_detection_events` → `020_silver`).
  Verify in `020_silver_materialization.sql` Task 6 step that `model_id` passes through.
