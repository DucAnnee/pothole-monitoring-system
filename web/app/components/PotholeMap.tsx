import Box from "@mui/material/Box";
import L, { type CircleMarker as LeafletCircleMarker } from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useRef, useState } from "react";
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

export function PotholeMap({ initialMarkers, onSelect, selected, activeFilters }: Props) {
  const [markers, setMarkers] = useState<PotholeMarker[]>(initialMarkers);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const markerLayerRef = useRef<L.LayerGroup | null>(null);
  const markerRefs = useRef<Map<string, LeafletCircleMarker>>(new Map());
  const abortRef = useRef<AbortController | null>(null);
  const onSelectRef = useRef(onSelect);

  useEffect(() => {
    onSelectRef.current = onSelect;
  }, [onSelect]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const leafletContainer = container as HTMLDivElement & { _leaflet_id?: number };
    delete leafletContainer._leaflet_id;

    const map = L.map(container, {
      center: [10.78, 106.7],
      zoom: 12,
    });
    const markerLayer = L.layerGroup().addTo(map);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(map);

    async function handleMove() {
      abortRef.current?.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      const { lat, lng } = map.getCenter();

      try {
        const res = await fetch(`/api/map-data?lat=${lat}&lon=${lng}`, { signal: ctrl.signal });
        if (!res.ok) return;
        const json = await res.json() as { markers: PotholeMarker[] };
        setMarkers(json.markers);
      } catch {
        // aborted or network error
      }
    }

    map.on("moveend", handleMove);
    mapRef.current = map;
    markerLayerRef.current = markerLayer;

    setTimeout(() => map.invalidateSize(), 0);

    return () => {
      abortRef.current?.abort();
      map.off("moveend", handleMove);
      markerRefs.current.clear();
      markerLayer.clearLayers();
      map.remove();
      markerLayerRef.current = null;
      mapRef.current = null;
      delete leafletContainer._leaflet_id;
    };
  }, []);

  useEffect(() => {
    setMarkers(initialMarkers);
  }, [initialMarkers]);

  useEffect(() => {
    const markerLayer = markerLayerRef.current;
    if (!markerLayer) return;

    markerLayer.clearLayers();
    markerRefs.current.clear();

    for (const marker of markers) {
      const key = normalizeSeverity(marker.severity_level);
      if (activeFilters.size > 0 && !activeFilters.has(key)) continue;

      const { color } = SEVERITY[key];
      const circle = L.circleMarker([marker.gps_lat, marker.gps_lon], {
        radius: SEVERITY_RADIUS[key] ?? 5,
        fillColor: color,
        fillOpacity: 0.85,
        color: selected?.pothole_id === marker.pothole_id ? "#fff" : color,
        weight: selected?.pothole_id === marker.pothole_id ? 2 : 1,
      });

      circle.on("click", () => onSelectRef.current(marker));
      circle.addTo(markerLayer);
      markerRefs.current.set(marker.pothole_id, circle);
    }
  }, [activeFilters, markers, selected?.pothole_id]);

  return (
    <Box
      ref={containerRef}
      sx={{ flex: 1, width: "100%", height: "100%", minHeight: 0 }}
    />
  );
}
