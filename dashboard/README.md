# 🕳️ Pothole Monitoring Dashboard

This repository contains the **Next.js web application** for our **Pothole Monitoring System** — a city-scale platform that visualizes road surface conditions detected by stereo camera sensors on public buses.

The dashboard provides:

* **Interactive heatmaps** of road health and pothole density
* **Detailed event information** (location, severity, timestamp, etc.)
* **REST APIs** to serve map tiles, metadata, and analytics for other services

---

## 🚀 Overview

Our end-to-end system processes pothole detections from edge devices mounted on buses:

1. **Edge tier:** Stereo cameras and embedded processors detect and segment potholes.
2. **Cloud tier:** Data is ingested, processed (BEV transform & severity classification), and stored.
3. **Dashboard tier (this project):** Displays a live, reliable overview of the aggregated results.

This web app is built with **Next.js** (App Router) and includes **basic REST APIs** for fetching processed pothole data and heatmap tiles from the backend.

---

## 🧩 Tech Stack

* **Framework:** [Next.js](https://nextjs.org) (React + TypeScript)
* **Styling:** Tailwind CSS
* **Map Visualization:** MapLibre GL / Deck.GL
* **APIs:** REST endpoints implemented via Next.js API routes
* **Deployment:** Vercel / Docker

---

## 🛠️ Getting Started

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-org/pothole-monitoring-dashboard.git
cd pothole-monitoring-dashboard
npm install --legacy-peer-deps
```

### 2. Run the development server

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
```

Then open [http://localhost:3000](http://localhost:3000) to view the dashboard.

---

## 📁 Project Structure

```
pothole-monitoring-dashboard/
├── app/                # Next.js App Router pages and layouts
├── components/         # UI and map components
├── public/             # Static assets
├── styles/             # Tailwind styles and globals
├── pages/api/          # REST API endpoints
└── utils/              # Helper modules and constants
```

---

## ⚙️ Environment Variables

Create a `.env.local` file to configure the app:

```bash
NEXT_PUBLIC_MAPBOX_TOKEN=<your_map_token>
API_BASE_URL=<backend_api_url>
```

---

## 🧭 API Endpoints (examples)

| Endpoint           | Description                                              |
| ------------------ | -------------------------------------------------------- |
| `/api/heatmap`     | Get aggregated pothole density and severity per map tile |
| `/api/events/[id]` | Get detail of a specific pothole event                   |
| `/api/health`      | System health and version info                           |

---

## 🧑‍💻 Development Notes

* The map and analytics automatically update when new data is pushed to the backend.
* UI components are built to be modular, with hooks for integrating live WebSocket or SSE updates later.
* Fonts are optimized using [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) with [Geist](https://vercel.com/font).

---

## ☁️ Deployment

You can deploy directly on [Vercel](https://vercel.com/new) (recommended) or using Docker:

```bash
docker build -t pothole-dashboard .
docker run -p 3000:3000 pothole-dashboard
```

Once deployed, the dashboard connects automatically to the backend API to serve live data.

---

## 📚 Learn More

* [Next.js Documentation](https://nextjs.org/docs)
* [Deck.GL](https://deck.gl) for map visualizations
* [Vercel Deployment Guide](https://nextjs.org/docs/app/building-your-application/deploying)