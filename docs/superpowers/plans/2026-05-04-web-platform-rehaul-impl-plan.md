# Web Platform Rehaul — Implementation Plan
**Date:** 2026-05-04  
**Spec:** `docs/superpowers/specs/2026-05-04-web-platform-rehaul-design.md`  
**Branch:** `feat/web-platform`  
**Output:** `web/` (React Router V7 full-stack app)

---

## Phases Overview

| Phase | Scope | Verification Gate |
|-------|-------|-------------------|
| 1 | Project scaffold + dev toolchain | `npm run dev` serves blank app |
| 2 | Design system (theme, constants, shared components) | Storybook-free visual spot-check via `/dev-ui` route |
| 3 | Auth flow (session, login page, `requireAuth`) | Login/logout round-trip works; protected route redirects |
| 4 | AppShell layout (sidebar, header, routing skeleton) | All nav links render correct empty route |
| 5 | Data layer (Trino, Redis, MinIO, session libs) | Unit tests + manual loader smoke tests with real infra |
| 6 | Overview page (`/`) | KPIs, sparkline, donut render with live data |
| 7 | Map page (`/map`) | Markers render, detail panel opens, pan/zoom reloads data |
| 8 | Health + Devices pages | Live metrics, auto-revalidation works |
| 9 | Placeholder pages (Dataset, LowConf, Annotation, Settings) | All routes mount without error |
| 10 | Integration + cutover | End-to-end smoke test; old `dashboard/` + `dashboard-backend/` retired |

---

## Phase 1 — Project Scaffold

**Goal:** Bare RR7 app boots in dev.

### Tasks

1. Create `web/` directory at repo root.
2. Init `package.json` with all dependencies from spec §9.
3. Create `react-router.config.ts`:
   ```ts
   import type { Config } from '@react-router/dev/config';
   export default { ssr: true } satisfies Config;
   ```
4. Create `vite.config.ts` with `@react-router/dev/vite` plugin.
5. Create `tsconfig.json` (strict, path alias `~/*` → `./app/*`).
6. Create `app/root.tsx` — minimal HTML shell (no MUI yet).
7. Create `app/routes/_app.tsx` — empty layout returning `<Outlet />`.
8. Create `app/routes/_app.overview.tsx` — returns `<p>Overview placeholder</p>`.
9. Create `.env` from spec §8 template.
10. Add `web/` entry to `.gitignore` node_modules exclusion if not already covered.

### Verification Gate ✓
```bash
cd web && npm install && npm run dev
# → http://localhost:3000 serves "Overview placeholder" text
# → No TypeScript errors: npm run typecheck
```

---

## Phase 2 — Design System

**Goal:** MUI theme, SEVERITY constants, and all shared components ready for use.

### Tasks

1. `app/theme.ts` — full MUI theme from spec §3 (palette, typography, shape, MuiCard override).
2. `app/constants/severity.ts` — SEVERITY map (critical/high/moderate/minor).
3. `app/root.tsx` — wrap app in `ThemeProvider` + `CssBaseline`.
4. `app/components/SeverityBadge.tsx` — MUI `Chip` size="small", color from SEVERITY map.
5. `app/components/StatusDot.tsx` — 8px `Box` circle, color prop.
6. `app/components/SparklineChart.tsx` — pure SVG area+line+dot, props: `data: number[]`, `width`, `height`, `color`.
7. Add a dev route `app/routes/dev-ui.tsx` (dev-only, remove before cutover) that renders all components with mock props for visual check.

### Verification Gate ✓
- Browse `http://localhost:3000/dev-ui` — all components render, no console errors.
- `npm run typecheck` passes.

---

## Phase 3 — Auth Flow

**Goal:** Login/logout with cookie session; all `_app.*` routes protected.

### Tasks

1. `app/lib/session.server.ts`:
   - `createCookieSessionStorage` with cookie name `__pothole_session`, `httpOnly: true`, `secure` from `NODE_ENV`.
   - Export `getSession`, `commitSession`, `destroySession`, `requireAuth(request)`.
   - `requireAuth` throws `redirect('/login')` if no session.
2. `app/routes/_auth.login.tsx`:
   - Loader: if session exists → redirect `/`.
   - Action: validate `AUTH_USERNAME`/`AUTH_PASSWORD` env vars → commit session → redirect `/`.
   - UI: full-page, HCMUT hexagon SVG logo (gradient), hexagon SVG background pattern, MUI `TextField` x2, `Button`, spinner via `useNavigation`.
   - Show demo credentials (`admin` / `admin123`) beneath form.
3. `app/routes/_auth.tsx` — bare layout (no AppShell), returns `<Outlet />`.
4. `app/routes/logout.tsx` — action only: destroy session → redirect `/login`.
5. Update `_app.tsx` loader: call `requireAuth(request)`.

### Verification Gate ✓
- `GET /` unauthenticated → redirects to `/login`.
- Correct credentials → session cookie set → redirects to `/`.
- Logout button → cookie cleared → redirects to `/login`.
- Wrong credentials → form re-renders with error message.

---

## Phase 4 — AppShell Layout

**Goal:** Sidebar + header shell with full nav; all route stubs render inside it.

### Tasks

1. `app/components/Sidebar.tsx`:
   - Props: `collapsed: boolean`, `onToggle: () => void`.
   - Nav items: Overview, Map, Dataset, LowConf, Annotation, Health, Devices, Settings — each with lucide-react icon (spec §3).
   - Active state from `useLocation`.
   - Width: 220px expanded, 56px collapsed; `transition: width 0.2s`.
   - Footer: collapse/expand toggle.
2. `app/components/Header.tsx`:
   - Left: page title (from `useMatches` meta) + "Last sync: …" timestamp.
   - Center: decorative MUI `InputBase` search bar.
   - Right: `Bell` icon (red dot Badge), divider, avatar + "Operator / admin", `LogOut` icon → POST `/logout`.
3. `app/components/AppShell.tsx`:
   - Manages `collapsed` state.
   - Renders `<Sidebar>` + `<Box flex=1>` with `<Header>` + `<Box overflow=auto padding=24px><Outlet /></Box>`.
4. `app/routes/_app.tsx` — render `<AppShell />`.
5. Create stub routes (each returns a `<p>Page placeholder</p>`):
   - `_app.map.tsx`, `_app.health.tsx`, `_app.dataset.tsx`, `_app.lowconf.tsx`, `_app.annotation.tsx`, `_app.devices.tsx`, `_app.settings.tsx`
6. `api.map-data.tsx` — stub returning `{ markers: [] }`.

### Verification Gate ✓
- All 8 nav links navigate without full reload.
- Sidebar collapses/expands, labels hide/show.
- Header shows correct page title per route.
- No TypeScript errors.

---

## Phase 5 — Data Layer

**Goal:** Server-only libs for Trino, Redis, MinIO fully implemented and tested.

### Tasks

1. `app/lib/trino.server.ts`:
   - `trinoFetch(sql: string): Promise<Row[]>` — POST `/v1/statement`, poll `nextUri` until `FINISHED`/`FAILED`, accumulate pages.
   - Headers: `X-Trino-User`, `X-Trino-Catalog`, `X-Trino-Schema`, `X-Trino-Source: pothole-web` from env vars.
   - Typed helpers:
     - `querySummary(): Promise<SummaryData>`
     - `queryMapPotholes(lat?, lon?, radiusKm?): Promise<PotholeMarker[]>`
     - `queryPotholeDetail(id: string): Promise<PotholeDetail>`
     - `queryLatencyStats(): Promise<LatencyStats>`
   - SQL patterns ported from `dashboard-backend/` controllers.
2. `app/lib/redis.server.ts`:
   - Singleton `ioredis` client (reconnect on error).
   - `cached<T>(key, ttlSec, fn): Promise<T>`.
   - `getLatencyMetrics(): Promise<LatencyData>` — reads sorted sets written by enrichment service.
   - `pingRedis(): Promise<boolean>`.
3. `app/lib/minio.server.ts`:
   - `presignedUrl(s3Path): Promise<string>` — 1hr expiry, cached in Redis 55min.
   - `proxyStream(s3Path): Response` — streams binary.
4. Add `app/routes/api.image.proxy.tsx` resource route — calls `proxyStream`.

### Verification Gate ✓
- With infra running (`docker-compose up -d`):
  - `querySummary()` returns non-empty object (manual loader test).
  - `queryMapPotholes()` returns array.
  - `cached()` populates Redis key; second call skips fn.
  - `presignedUrl()` returns valid URL; second call hits Redis cache.
- `npm run typecheck` passes.

---

## Phase 6 — Overview Page (`/`)

**Goal:** Executive dashboard with live data.

### Tasks

1. `_app.overview.tsx` loader:
   - Call `requireAuth`, then `querySummary()` with 60s Redis cache.
   - Return typed loader data.
2. KPI cards section:
   - 5-column CSS Grid (`display: grid, gridTemplateColumns: repeat(5, 1fr)`).
   - Cards: Total Active, New Today, Resolved This Week, Avg Severity, In Progress.
   - Each: MUI `Card` + `CardContent`, value + label + trend icon.
3. 30-day sparkline row — `<SparklineChart>` with historical count data.
4. Severity donut — custom SVG (no chart lib), slices from SEVERITY map, legend.
5. Bottom 3-column row:
   - Recent Critical Detections: list, click → `navigate('/map')`.
   - Top Affected Districts: list + MUI `LinearProgress`.
   - Operator Action Queue: list + "Act" `Button` (placeholder `onClick`).
6. `useRevalidator` polling every 60s.

### Verification Gate ✓
- Page loads with real data from Trino.
- KPI values match `dashboard-backend/` `/api/v1/summary` response.
- Sparkline and donut render with correct proportions.
- 60s auto-revalidation triggers loader re-run (verify via network tab).

---

## Phase 7 — Map Page (`/map`)

**Goal:** Full interactive pothole map with detail panel.

### Tasks

1. `_app.map.tsx` loader: `requireAuth`, initial `queryMapPotholes()` centred on HCMC, 30s Redis cache. Override content padding to 0 via route meta or sx prop on content wrapper.
2. `ClientOnly` guard component (`app/components/ClientOnly.tsx`):
   ```tsx
   export function ClientOnly({ children, fallback }: { children: React.ReactNode; fallback?: React.ReactNode }) {
     const [mounted, setMounted] = useState(false);
     useEffect(() => setMounted(true), []);
     return mounted ? <>{children}</> : <>{fallback ?? null}</>;
   }
   ```
3. `app/components/PotholeMap.tsx` (client-only):
   - `react-leaflet` `MapContainer`, center `[10.78, 106.70]`, zoom 12.
   - OSM tile layer.
   - `CircleMarker` per pothole: radius by severity, fillColor from SEVERITY.
   - `moveend` event → `fetch('/api/map-data?lat=&lon=')` → update markers state.
   - Click marker → set selected pothole → detail panel opens.
4. Filter chip row above map: Critical/High/Moderate/Minor counts, toggle filters on marker display.
5. Detail panel (340px, right side, slides in via CSS transition):
   - Severity score + `SeverityBadge`, status badge, detected date.
   - Location card (address, GPS coords, Road ID).
   - Measurements grid (depth, surface area, diameter).
   - Timeline (detected event).
   - Image thumbnails from MinIO presigned URLs → lightbox modal on click.
   - Action buttons: Assign Repair, Mark Reviewed (placeholders), Annotate → `/annotation`.
6. `api.map-data.tsx` resource route: parse `lat`/`lon` query params → `queryMapPotholes(lat, lon, 1)` → JSON response, 30s Redis cache per bbox.

### Verification Gate ✓
- Map renders client-side; SSR fallback (skeleton) visible briefly on hard refresh.
- Markers display with correct colors/sizes.
- Filter chips toggle marker visibility.
- Click marker → detail panel slides in with correct data.
- Pan/zoom triggers `/api/map-data` fetch; new markers appear.
- Image thumbnails load from MinIO.

---

## Phase 8 — Health + Devices Pages

**Goal:** Live system health metrics with auto-revalidation.

### Tasks

1. Shared server function `app/lib/health.server.ts`:
   - `fetchHealthData()`: queries Redis for service metrics, Kafka broker status, MinIO stats, Polaris stats, edge device telemetry.
   - Returns typed `HealthData` object.
2. `_app.health.tsx` loader: `requireAuth` + `fetchHealthData()` (10s Redis cache). Return full `HealthData`.
3. Health page sections:
   - Kafka card: 3 brokers + 1 controller, each row: name, IP, last heartbeat, Active `StatusDot`.
   - MinIO card: storage usage `LinearProgress`, type distribution bar, GB breakdown.
   - Polaris card: catalogs/tables/queries-per-min stats.
   - Microservices 3-column grid (6 services): name, icon, `SeverityBadge`-style health badge, uptime %, latency ms, last check.
4. `app/components/EdgeDevicesTable.tsx`:
   - Full-width MUI `Table`.
   - Columns: Vehicle, Device, GPS `StatusDot`, Camera `StatusDot`, Model version, Last Upload, Battery `LinearProgress`, Storage %, Connection badge, Pending count, Health badge.
5. `_app.health.tsx` renders all sections + `<EdgeDevicesTable>`.
6. `_app.devices.tsx` loader: `requireAuth` + `fetchHealthData()` (10s Redis cache, same function). Renders only `<EdgeDevicesTable>`.
7. Both routes: `useRevalidator` every 30s.

### Verification Gate ✓
- `/health` shows populated Kafka, MinIO, Polaris, microservices sections.
- `/devices` shows same edge device table as health page.
- 30s revalidation triggers (verify via network tab).
- `StatusDot` and health badges reflect actual service states.

---

## Phase 9 — Placeholder Pages

**Goal:** All remaining routes mount without error, matching spec §§6.5–6.9 visually.

### Tasks

#### Dataset (`/dataset`)
- 6 stat cards (static mock numbers).
- Tab bar (Images / Annotations / Versions / Low Confidence) — MUI `Tabs`, no functionality.
- Left tree sidebar (static mock folders), center image grid (24 placeholder items), right detail panel on selection.
- No loader; all static mock data.

#### Low Confidence (`/lowconf`)
- Filter bar: confidence range display (static), severity filter chips (toggle UI only).
- Detection card grid: dual image panes (Raw/Segmentation placeholders), confidence %, severity badge, location, date, reason, Review button (no-op).
- Static mock data.

#### Annotation Editor (`/annotation`)
- Full-height layout (override 24px padding to 0).
- Top toolbar: back `Button`, filename text, opacity `Slider`, zoom controls.
- Left panel: detection metadata (static), operator notes `TextField`, confidence meter (static `LinearProgress`).
- Center: dark `Box` canvas, static SVG polygon with 4 draggable vertex dots (mouse events, local state).
- Right toolbar: Select/Pan/Polygon/Edit Vertex tool buttons (toggle state), undo/redo/reset (no-op), Auto Segment button (shows loading spinner for 2s then stops), Save + Submit buttons (no-op).

#### Settings (`/settings`)
- Centered `Settings` lucide icon + Typography "Configuration panel coming soon."

### Verification Gate ✓
- All 4 routes render without console errors.
- No TypeScript errors.
- Annotation polygon vertices drag correctly.
- Tab/filter/tool toggle UI state works.

---

## Phase 10 — Integration & Cutover

**Goal:** `web/` fully replaces `dashboard/` + `dashboard-backend/`.

### Tasks

1. End-to-end smoke test checklist:
   - [ ] Login, logout, session expiry redirect.
   - [ ] Overview KPIs match DB values.
   - [ ] Map loads markers; pan/zoom updates; detail panel complete.
   - [ ] Health metrics live; 30s revalidation confirmed.
   - [ ] Devices table matches health page.
   - [ ] All placeholder pages mount cleanly.
   - [ ] Image proxy route serves MinIO images.
2. Performance check:
   - [ ] Redis cache hit on second load (check Redis `TTL` on keys).
   - [ ] No N+1 Trino queries per render.
3. Update `CLAUDE.md`:
   - Replace `dashboard/` + `dashboard-backend/` references with `web/`.
   - Add `web/` commands section.
4. Remove dev-only route `app/routes/dev-ui.tsx`.
5. Update `docker-compose.yml` or run scripts if web app needs to be containerized.
6. Delete `dashboard/` and `dashboard-backend/` directories.
   - **Note:** Confirm with user before deletion — irreversible.
7. `dashboard-ui-reference/` — keep permanently, do not delete.

### Verification Gate ✓
- All smoke test items checked.
- `npm run typecheck` passes in `web/`.
- `npm run build` completes without error.
- Old directories gone; `CLAUDE.md` updated.

---

## Cross-Cutting Constraints

| Constraint | Detail |
|------------|--------|
| No Tailwind | MUI `sx` prop only |
| No Recharts | SVG-only for sparkline and donut |
| No `presto-client` | Custom `trino.server.ts` via HTTP REST |
| No `React.lazy` alone for map | Use `ClientOnly` guard + `Suspense` |
| SSR safe | All `window`/`document` access inside `ClientOnly` or `useEffect` |
| Server lib isolation | Files ending `.server.ts` never imported from client code |
| Auth on every `_app.*` loader | `requireAuth(request)` first line of every loader |

---

## File Creation Order (dependency-safe)

```
Phase 1:  package.json, tsconfig, vite.config, react-router.config, app/root.tsx, routes stubs
Phase 2:  theme.ts, constants/severity.ts, components/{SeverityBadge,StatusDot,SparklineChart}
Phase 3:  lib/session.server.ts, routes/{_auth.tsx,_auth.login.tsx,logout.tsx}
Phase 4:  components/{Sidebar,Header,AppShell}, routes/_app.tsx + all stub routes
Phase 5:  lib/{trino,redis,minio}.server.ts, routes/api.image.proxy.tsx
Phase 6:  routes/_app.overview.tsx (full)
Phase 7:  components/{ClientOnly,PotholeMap}, routes/{_app.map.tsx,api.map-data.tsx} (full)
Phase 8:  lib/health.server.ts, components/EdgeDevicesTable, routes/{_app.health,_app.devices}.tsx (full)
Phase 9:  routes/{_app.dataset,_app.lowconf,_app.annotation,_app.settings}.tsx (full)
Phase 10: cleanup, CLAUDE.md update, deletion
```
