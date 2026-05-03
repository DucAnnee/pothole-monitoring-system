# Web Platform Rehaul — Design Spec
**Date:** 2026-05-04  
**Scope:** Replace `dashboard/` + `dashboard-backend/` with a unified React Router V7 full-stack app in `web/`

---

## 1. Overview

Replace the existing Next.js frontend (`dashboard/`) and Express backend (`dashboard-backend/`) with a single full-stack React Router V7 (framework mode) application. All server-side data access — Trino queries, Redis reads, MinIO presigned URLs — moves into RR7 `loader`/`action` functions. The Express backend is fully eliminated.

**Stack:**
- Framework: React Router V7 (framework mode, formerly Remix)
- UI: MUI v6 + lucide-react icons
- Map: react-leaflet + OpenStreetMap tiles
- DB: Trino via direct HTTP REST API (custom `trino.server.ts` wrapper)
- Cache/metrics: Redis via ioredis
- Object storage: MinIO via minio SDK
- Auth: RR7 cookie sessions (`createCookieSessionStorage`)
- Build: Vite (bundled with RR7)

---

## 2. Project Structure

```
web/
├── app/
│   ├── root.tsx                      # MUI ThemeProvider, global HTML shell
│   ├── routes/
│   │   ├── _auth.login.tsx           # /login — no AppShell
│   │   ├── _app.tsx                  # AppShell layout parent (sidebar + header)
│   │   ├── _app.overview.tsx         # / — Executive Overview
│   │   ├── _app.map.tsx              # /map — Pothole Map View
│   │   ├── _app.health.tsx           # /health — System Health
│   │   ├── _app.dataset.tsx          # /dataset — Dataset Mgmt (placeholder)
│   │   ├── _app.lowconf.tsx          # /lowconf — Low Confidence (placeholder)
│   │   ├── _app.annotation.tsx       # /annotation — Annotation Editor (placeholder)
│   │   ├── _app.devices.tsx          # /devices — Edge Devices (reuses health data)
│   │   ├── _app.settings.tsx         # /settings — placeholder
│   │   └── api.map-data.tsx          # Resource route: GET /api/map-data?lat&lon → JSON
│   ├── lib/
│   │   ├── trino.server.ts           # Trino HTTP REST client + typed query helpers
│   │   ├── redis.server.ts           # ioredis client + generic cache wrapper
│   │   ├── minio.server.ts           # MinIO presigned URL + proxy stream helpers
│   │   └── session.server.ts         # Cookie session: getSession/commitSession/destroySession
│   ├── components/
│   │   ├── AppShell.tsx              # Collapsible sidebar + header
│   │   ├── Sidebar.tsx
│   │   ├── Header.tsx
│   │   ├── SeverityBadge.tsx         # MUI Chip with severity color mapping
│   │   ├── StatusDot.tsx             # Colored dot for online/offline/warning
│   │   └── SparklineChart.tsx        # SVG sparkline (no chart lib)
│   ├── constants/
│   │   └── severity.ts               # SEVERITY color map (critical/high/moderate/minor)
│   └── theme.ts                      # MUI theme definition
├── public/
├── package.json
├── react-router.config.ts
├── vite.config.ts
└── .env                              # TRINO_*, REDIS_*, MINIO_*, AUTH_*, SESSION_SECRET
```

**Route naming convention:** RR7 uses dot-separated file names. Prefix `_app.` groups routes under the AppShell layout; prefix `_auth.` groups routes under the bare auth layout. Resource routes (no component export) use `api.` prefix by convention.

---

## 3. Design System

### MUI Theme
```ts
// app/theme.ts
palette: {
  primary:    { main: '#1488DB', dark: '#030391', light: '#E8F4FD' },
  error:      { main: '#DC2626' },   // critical severity
  warning:    { main: '#D97706' },   // moderate severity
  success:    { main: '#16A34A' },   // minor / healthy
  background: { default: '#F5F7FA', paper: '#FFFFFF' },
  text:       { primary: '#1A2332', secondary: '#5A6B7F', disabled: '#8895A7' },
}
typography: { fontFamily: '"Inter", "Arial", sans-serif' }
shape: { borderRadius: 8 }
MuiCard: boxShadow none, border '1px solid #E2E8F0'
```

### Severity Constants
```ts
// app/constants/severity.ts
export const SEVERITY = {
  critical: { color: '#DC2626', bg: '#FEF2F2', label: 'Critical' },
  high:     { color: '#EA580C', bg: '#FFF7ED', label: 'High' },
  moderate: { color: '#D97706', bg: '#FFFBEB', label: 'Moderate' },
  minor:    { color: '#16A34A', bg: '#F0FDF4', label: 'Minor' },
}
```

### Component Mapping (reference → MUI)
| Reference | MUI equivalent |
|-----------|---------------|
| `Card` | `MuiCard` + `CardContent` |
| `Badge`/`SeverityBadge` | `Chip` size="small" with `sx` from SEVERITY |
| `Btn` primary | `Button` variant="contained" |
| `Btn` secondary | `Button` variant="outlined" color="primary" |
| `Btn` ghost | `Button` variant="text" |
| `Btn` outline | `Button` variant="outlined" color="inherit" |
| `StatusDot` | Custom `Box` component (8px circle) |
| `Icon` | lucide-react (1:1 name mapping) |

### Icons (lucide-react equivalents)
`Grid2X2` → Overview, `Map` → Map, `Database` → Dataset, `AlertTriangle` → LowConf, `Pencil` → Annotation, `Activity` → Health, `Cpu` → Devices, `Settings` → Settings, `Bell` → notifications, `LogOut` → logout, `Search` → search bar, `MapPin` → location, `Clock` → time, `CheckCircle` → resolved, `TrendingUp`/`TrendingDown` → trend indicators.

---

## 4. Data Layer

### 4.1 Trino Client (`lib/trino.server.ts`)

Custom fetch-based wrapper targeting the Trino HTTP REST API (not the Presto compat layer).

**Protocol:**
1. `POST /v1/statement` with SQL body → returns `{ id, nextUri, columns?, data?, stats }`
2. Poll `nextUri` with `GET` until `stats.state === 'FINISHED'` or `'FAILED'`
3. Accumulate `data` rows across pages
4. Return typed result

**Headers per request:** `X-Trino-User`, `X-Trino-Catalog`, `X-Trino-Schema`, `X-Trino-Source: pothole-web`

**Query helpers exposed:**
```ts
querySummary(): Promise<SummaryData>
queryMapPotholes(lat?: number, lon?: number, radiusKm?: number): Promise<PotholeMarker[]>
queryPotholeDetail(id: string): Promise<PotholeDetail>
queryLatencyStats(): Promise<LatencyStats>
```

Queries match the existing SQL patterns from `dashboard-backend/` controllers (single aggregation query for summary, bounding box for map, ID lookup for detail).

### 4.2 Redis Client (`lib/redis.server.ts`)

```ts
// Generic cache wrapper
cached<T>(key: string, ttlSec: number, fn: () => Promise<T>): Promise<T>

// Latency metrics (reads sorted sets written by enrichment service)
getLatencyMetrics(): Promise<LatencyData>

// Health check
pingRedis(): Promise<boolean>
```

### 4.3 MinIO Client (`lib/minio.server.ts`)

```ts
presignedUrl(s3Path: string): Promise<string>   // 1hr expiry, cached in Redis 55min
proxyStream(s3Path: string): Response            // streams binary for /api/image/proxy
```

### 4.4 Session (`lib/session.server.ts`)

RR7 built-in `createCookieSessionStorage`. Session cookie: `__pothole_session`, `httpOnly: true`, `secure: true` in production, signed with `SESSION_SECRET` env var.

```ts
requireAuth(request: Request): Promise<void>  // throws redirect('/login') if no session
```

### 4.5 Loader Caching & Revalidation

| Route | Cache TTL | Client revalidation |
|-------|-----------|---------------------|
| `/` (overview) | Redis 60s | `useRevalidator` every 60s |
| `/map` (initial load) | Redis 30s | none (resource route handles pan/zoom) |
| `/api/map-data` | Redis 30s per bbox | on map pan/zoom via `fetch` |
| `/health` | Redis 10s | `useRevalidator` every 30s |
| `/devices` | Redis 10s | `useRevalidator` every 30s |

---

## 5. Auth Flow

1. All `_app.*` loaders call `requireAuth(request)` → redirects to `/login` if no session
2. `_auth.login.tsx` loader: if session exists → redirect to `/`
3. `_auth.login.tsx` action: validate credentials against `AUTH_USERNAME`/`AUTH_PASSWORD` env vars → create session → redirect to `/`
4. Header logout button → POSTs to `/logout` action → destroys session → redirects to `/login`
5. Demo credentials shown on login page (same as reference design)

---

## 6. Pages

### 6.1 Login (`/login`)
- Full-page, no AppShell
- HCMUT hexagon logo (gradient SVG, matches reference)
- Geometric hexagon SVG background pattern
- Username + password fields (MUI `TextField`)
- `useNavigation` → spinner on submit
- `action` validates env credentials, sets session cookie

### 6.2 Executive Overview (`/`)
- 5 KPI cards in CSS Grid (5 columns): Total Active, New Today, Resolved This Week, Avg Severity, In Progress
- 30-day sparkline: custom SVG `SparklineChart` component (area + line + dot), no chart library
- Severity donut: custom SVG (matches reference exactly)
- Bottom 3-column row:
  - Recent Critical Detections (click → navigate to `/map`)
  - Top Affected Districts (progress bars)
  - Operator Action Queue (with "Act" buttons — placeholder action for now)
- `useRevalidator` polls every 60s

### 6.3 Map View (`/map`)
- Full viewport height (`calc(100vh - 56px)`), zero padding override
- Filter chip row: Critical/High/Moderate/Minor counts, toggle-filter behavior
- `react-leaflet` `MapContainer` — OSM tiles, center HCMC `[10.78, 106.70]`, zoom 12
- `CircleMarker` per pothole, radius by severity (critical→9, high→7, moderate→6, minor→5), color from SEVERITY map
- Click marker → right detail panel (340px, slides in):
  - Severity score + badge, status badge, detected date
  - Location card (address, GPS coords, Road ID from OSM)
  - Measurements grid (depth cm, surface area cm², estimated diameter)
  - Timeline (detected event)
  - Image thumbnails → expand to modal lightbox (MinIO presigned URLs)
  - Action buttons: Assign Repair (placeholder), Mark Reviewed (placeholder), Annotate → `/annotation`
- Map pan/zoom: `moveend` event → `fetch('/api/map-data?lat&lon')` → update markers state
- `react-leaflet` loaded client-only: map component wrapped in a `ClientOnly` guard (`typeof window !== 'undefined'`) rendered inside `Suspense`. RR7's SSR will render the fallback (skeleton); client hydration mounts Leaflet. Do NOT use `React.lazy` alone — RR7 SSR will attempt to render it server-side and throw.

### 6.4 System Health (`/health`)
- 2-column top section:
  - Kafka card: 3 brokers + 1 controller, each row shows name, IP, last heartbeat time, Active badge
  - Right column: MinIO card (storage usage progress bar, type distribution bar, GB breakdown) + Polaris Catalog card (catalogs/tables/queries-per-min stats)
- Microservices: 3-column grid, 6 services — name, icon, Healthy/Warning badge, uptime %, latency ms, last check time
- Edge Devices: full-width table — Vehicle, Device, GPS dot, Camera dot, Model version, Last Upload, Battery bar, Storage %, Connection badge, Pending count, Health badge
- `useRevalidator` every 30s

### 6.5 Dataset (`/dataset`) — placeholder
- 6 stat cards (Total Images, Annotations, Train, Valid, Test, Version)
- Tab bar (Images / Annotations / Versions / Low Confidence)
- Left split tree sidebar, center image grid (24 mock items), right detail panel on selection
- Static mock data — no loader

### 6.6 Low Confidence (`/lowconf`) — placeholder
- Filter bar (confidence range display, severity filter chips)
- Detection card grid — each card: dual image panes (Raw / Segmentation), confidence %, severity badge, location, date, reason, Review button
- Static mock data — no loader

### 6.7 Annotation Editor (`/annotation`) — placeholder
- Full-height layout (no padding)
- Top toolbar: back button, filename, opacity slider, zoom controls
- Left panel: detection metadata + operator notes textarea + confidence meter
- Center: dark canvas with simulated road background + interactive SVG polygon (draggable vertices, polygon tool)
- Right toolbar: Select/Pan/Polygon/Edit Vertex tools, undo/redo/reset actions, Auto Segment (SAM3 loading animation), Save + Submit buttons
- Static mock data — no loader

### 6.8 Edge Devices (`/devices`)
- Reuses the `EdgeDevicesTable` component from Health page
- Own loader calls the same shared `fetchHealthData()` server function as `/health`; renders only the edge device section

### 6.9 Settings (`/settings`)
- Centered Settings icon + "Configuration panel coming soon." text
- No data

---

## 7. AppShell Layout

**Sidebar** (collapsible: 220px ↔ 56px, `transition: width 0.2s`):
- Logo: HCMUT hexagon + "Pothole Monitor" / "HCMUT • MoC&T" text (hidden when collapsed)
- Nav items: icons always visible, labels hidden when collapsed, active state: `primaryLight` bg + primary color
- Footer: Collapse/Expand toggle button

**Header** (56px fixed):
- Left: page title (from route meta) + "Last sync: …" timestamp
- Center: search bar (decorative, non-functional placeholder)
- Right: notification bell (red dot), divider, user avatar + "Operator / admin" + logout icon

**Content area:** `flex: 1, overflow: auto, padding: 24px`

---

## 8. Environment Variables

```env
# Trino
TRINO_HOST=localhost
TRINO_PORT=8081
TRINO_USER=admin
TRINO_CATALOG=iceberg
TRINO_SCHEMA=city

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# MinIO
MINIO_ENDPOINT=localhost
MINIO_PORT=9000
MINIO_USE_SSL=false
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=warehouse

# Auth
AUTH_USERNAME=admin
AUTH_PASSWORD=admin123
SESSION_SECRET=change-me-in-production

# App
PORT=3000
NODE_ENV=development
```

---

## 9. Dependencies

```json
{
  "dependencies": {
    "@react-router/node": "^7",
    "@react-router/serve": "^7",
    "react-router": "^7",
    "react": "^19",
    "react-dom": "^19",
    "@mui/material": "^6",
    "@emotion/react": "^11",
    "@emotion/styled": "^11",
    "lucide-react": "latest",
    "react-leaflet": "^4",
    "leaflet": "^1",
    "@types/leaflet": "^1",
    "ioredis": "^5",
    "minio": "^8"
  },
  "devDependencies": {
    "@react-router/dev": "^7",
    "vite": "^6",
    "typescript": "^5"
  }
}
```

No `presto-client` — replaced by custom `trino.server.ts`. No Tailwind, no Radix UI, no Recharts, no `next`.

---

## 10. Migration Notes

- `dashboard/` and `dashboard-backend/` kept as-is during implementation (reference only)
- Delete both after `web/` is verified working end-to-end
- CLAUDE.md to be updated: replace dashboard/backend references with `web/`
- `dashboard-ui-reference/` stays permanently as design reference
