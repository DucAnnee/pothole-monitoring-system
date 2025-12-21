# Pothole Monitoring Dashboard Backend

Node.js backend service that reads from Iceberg tables via Trino and exposes REST APIs for the frontend dashboard.

## Prerequisites

- Node.js >= 20.0.0
- Docker services running (Trino, MinIO, Redis)

## Quick Start

1. **Install dependencies:**
   ```bash
   cd dashboard-backend
   npm install
   ```

2. **Configure environment:**
   ```bash
   # Copy example env file (already done, but for reference)
   cp .env.example .env
   
   # Edit .env if needed to match your setup
   ```

3. **Start the server:**
   ```bash
   # Development mode (with auto-reload)
   npm run dev
   
   # Production mode
   npm start
   ```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3002 | Server port |
| `NODE_ENV` | development | Environment mode |
| `TRINO_HOST` | localhost | Trino server host |
| `TRINO_PORT` | 8081 | Trino server port |
| `TRINO_USER` | trino | Trino user |
| `TRINO_CATALOG` | iceberg | Trino catalog |
| `TRINO_SCHEMA` | city | Trino schema |
| `REDIS_HOST` | localhost | Redis host |
| `REDIS_PORT` | 6379 | Redis port |
| `CORS_ORIGIN` | http://localhost:3000 | Allowed CORS origin |

## API Endpoints

### Base Path: `/api/v1`

### Health Check
```
GET /api/v1/health
```
Returns service health status including Trino and Redis connectivity.

### Dashboard Summary
```
GET /api/v1/summary
```
Returns dashboard summary metrics including:
- Active potholes count
- New potholes today/this week with comparisons
- Average severity
- Active potholes last 30 days (for line chart)
- Severity distribution
- Status change metrics

### Map View
```
GET /api/v1/map-view?lat={lat}&lon={lon}
```
Returns up to 30 potholes within 1km radius of the specified coordinates.

**Parameters:**
- `lat` (required): Latitude (-90 to 90)
- `lon` (required): Longitude (-180 to 180)

### Pothole Details
```
GET /api/v1/pothole/{pothole_id}
```
Returns complete information for a specific pothole.

## Caching

Redis is used for caching with the following TTLs:
- Summary: 60 seconds
- Map view: 30 seconds
- Pothole details: 120 seconds
- Health check: 10 seconds

## Architecture

```
dashboard-backend/
├── src/
│   ├── config/         # Configuration
│   ├── controllers/    # Route handlers
│   ├── db/             # Database clients (Trino, Redis)
│   ├── middleware/     # Express middleware
│   ├── routes/         # API route definitions
│   └── index.js        # Application entry point
├── .env                # Environment variables
├── .env.example        # Example environment file
├── package.json        # Dependencies
└── README.md           # This file
```

## Data Sources

The backend queries Iceberg tables via Trino:
- `iceberg.city.raw_events` - Raw pothole detection events
- `iceberg.city.severity_scores` - ML severity calculations
- `iceberg.city.potholes` - Current pothole state
- `iceberg.city.pothole_history` - Historical observations

See [SCHEMA.md](../SCHEMA.md) for complete table schemas.

## Development

```bash
# Run with auto-reload
npm run dev

# Run linting
npm run lint
```

## Production

```bash
# Set environment
export NODE_ENV=production

# Start server
npm start
```
