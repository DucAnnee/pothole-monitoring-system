import Box from "@mui/material/Box";
import "leaflet/dist/leaflet.css";
import { useEffect, useRef, useState } from "react";
import { CircleMarker, MapContainer, TileLayer, useMapEvents } from "react-leaflet";
import { SEVERITY, normalizeSeverity } from "~/constants/severity";
import type { PotholeMarker } from "~/lib/trino.server";

const SEVERITY_RADIUS: Record<string, number> = {
  critical: 9,
  high: 7,
  moderate: 6,
  minor: 5,
};

interface Props {
  initialMarkers: PotholeMarker[];
  onSelect: (p: PotholeMarker) => void;
  selected: PotholeMarker | null;
  activeFilters: Set<string>;
}

function MapMoveHandler({ onMove }: { onMove: (lat: number, lon: number) => void }) {
  useMapEvents({
    moveend(e) {
      const c = e.target.getCenter();
      onMove(c.lat, c.lng);
    },
  });
  return null;
}

export function PotholeMap({ initialMarkers, onSelect, selected, activeFilters }: Props) {
  const [markers, setMarkers] = useState<PotholeMarker[]>(initialMarkers);
  const abortRef = useRef<AbortController | null>(null);

  async function handleMove(lat: number, lon: number) {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    try {
      const res = await fetch(`/api/map-data?lat=${lat}&lon=${lon}`, { signal: ctrl.signal });
      const json = await res.json() as { markers: PotholeMarker[] };
      setMarkers(json.markers);
    } catch {
      // aborted or network error
    }
  }

  useEffect(() => {
    setMarkers(initialMarkers);
  }, [initialMarkers]);

  const visible = markers.filter((m) => {
    const key = normalizeSeverity(m.severity_level);
    return activeFilters.size === 0 || activeFilters.has(key);
  });

  return (
    <Box sx={{ flex: 1, "& .leaflet-container": { width: "100%", height: "100%" } }}>
      <MapContainer
        center={[10.78, 106.7]}
        zoom={12}
        style={{ width: "100%", height: "100%" }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <MapMoveHandler onMove={handleMove} />
        {visible.map((m) => {
          const key = normalizeSeverity(m.severity_level);
          const { color } = SEVERITY[key];
          const radius = SEVERITY_RADIUS[key] ?? 5;
          return (
            <CircleMarker
              key={m.pothole_id}
              center={[m.gps_lat, m.gps_lon]}
              radius={radius}
              pathOptions={{
                fillColor: color,
                fillOpacity: 0.85,
                color: selected?.pothole_id === m.pothole_id ? "#fff" : color,
                weight: selected?.pothole_id === m.pothole_id ? 2 : 1,
              }}
              eventHandlers={{ click: () => onSelect(m) }}
            />
          );
        })}
      </MapContainer>
    </Box>
  );
}
