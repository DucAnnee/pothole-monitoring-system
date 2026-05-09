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

// Skip moveend fetches when center hasn't moved beyond this threshold (handles pure zoom).
// Leaflet zoom-to-cursor drifts center ~0.0003–0.0007°; threshold must exceed that
// to avoid spurious fetches on zoom. 0.002° ≈ 200 m — detects intentional pan only.
const CENTER_MOVE_THRESHOLD = 0.002;
const MOVEEND_DEBOUNCE_MS = 250;

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
  const selectedRef = useRef(selected);
  const markersDataRef = useRef(markers);
  const prevCenterRef = useRef<{ lat: number; lng: number } | null>(null);
  const moveDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const prevSelectedIdRef = useRef<string | null>(selected?.pothole_id ?? null);

  useEffect(() => { onSelectRef.current = onSelect; }, [onSelect]);
  useEffect(() => { selectedRef.current = selected; }, [selected]);
  useEffect(() => { markersDataRef.current = markers; }, [markers]);

  // Sync initialMarkers into local state when loader data refreshes.
  useEffect(() => {
    setMarkers(initialMarkers);
  }, [initialMarkers]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const leafletContainer = container as HTMLDivElement & { _leaflet_id?: number };
    delete leafletContainer._leaflet_id;

    const map = L.map(container, {
      center: [10.78, 106.7],
      zoom: 12,
    });
    // Seed prevCenterRef so zoom-only moveend events (same center) skip the fetch.
    prevCenterRef.current = { lat: 10.78, lng: 106.7 };
    const markerLayer = L.layerGroup().addTo(map);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(map);

    function doFetch(lat: number, lng: number) {
      abortRef.current?.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;

      fetch(`/api/map-data?lat=${lat}&lon=${lng}`, { signal: ctrl.signal })
        .then((res) => (res.ok ? res.json() : null))
        .then((json) => {
          if (json) setMarkers((json as { markers: PotholeMarker[] }).markers);
        })
        .catch(() => {});
    }

    function handleMove() {
      const { lat, lng } = map.getCenter();

      // Skip fetch on pure zoom (center hasn't moved).
      const prev = prevCenterRef.current;
      if (
        prev &&
        Math.abs(prev.lat - lat) < CENTER_MOVE_THRESHOLD &&
        Math.abs(prev.lng - lng) < CENTER_MOVE_THRESHOLD
      ) {
        return;
      }
      prevCenterRef.current = { lat, lng };
      doFetch(lat, lng);
    }

    function onMoveEnd() {
      if (moveDebounceRef.current) clearTimeout(moveDebounceRef.current);
      moveDebounceRef.current = setTimeout(handleMove, MOVEEND_DEBOUNCE_MS);
    }

    map.on("moveend", onMoveEnd);
    mapRef.current = map;
    markerLayerRef.current = markerLayer;

    setTimeout(() => map.invalidateSize(), 0);

    return () => {
      if (moveDebounceRef.current) clearTimeout(moveDebounceRef.current);
      abortRef.current?.abort();
      map.off("moveend", onMoveEnd);
      markerRefs.current.clear();
      markerLayer.clearLayers();
      map.remove();
      markerLayerRef.current = null;
      mapRef.current = null;
      delete leafletContainer._leaflet_id;
    };
  }, []);

  // Incrementally add/remove markers when data or filters change.
  // Does NOT include `selected` in deps — selection styling is handled separately below.
  useEffect(() => {
    const markerLayer = markerLayerRef.current;
    if (!markerLayer) return;

    const currentSelected = selectedRef.current;

    const visible = new Map<string, PotholeMarker>();
    for (const m of markers) {
      const key = normalizeSeverity(m.severity_level);
      if (activeFilters.size === 0 || activeFilters.has(key)) {
        visible.set(m.pothole_id, m);
      }
    }

    // Remove circles that are no longer in the visible set.
    for (const [id, circle] of markerRefs.current) {
      if (!visible.has(id)) {
        markerLayer.removeLayer(circle);
        markerRefs.current.delete(id);
      }
    }

    // Add circles for newly visible markers.
    for (const [id, marker] of visible) {
      if (!markerRefs.current.has(id)) {
        const key = normalizeSeverity(marker.severity_level);
        const { color } = SEVERITY[key];
        const isSelected = currentSelected?.pothole_id === id;
        const circle = L.circleMarker([marker.gps_lat, marker.gps_lon], {
          radius: SEVERITY_RADIUS[key] ?? 5,
          fillColor: color,
          fillOpacity: 0.85,
          color: isSelected ? "#fff" : color,
          weight: isSelected ? 2 : 1,
        });
        circle.on("click", () => onSelectRef.current(marker));
        circle.addTo(markerLayer);
        markerRefs.current.set(id, circle);
      }
    }
  }, [activeFilters, markers]);

  // Update only the two affected markers' styles when selection changes.
  useEffect(() => {
    const newId = selected?.pothole_id ?? null;
    const oldId = prevSelectedIdRef.current;

    if (oldId !== null && oldId !== newId) {
      const circle = markerRefs.current.get(oldId);
      if (circle) {
        const marker = markersDataRef.current.find((m) => m.pothole_id === oldId);
        if (marker) {
          const key = normalizeSeverity(marker.severity_level);
          circle.setStyle({ color: SEVERITY[key].color, weight: 1 });
        }
      }
    }

    if (newId !== null && newId !== oldId) {
      const circle = markerRefs.current.get(newId);
      if (circle) circle.setStyle({ color: "#fff", weight: 2 });
    }

    prevSelectedIdRef.current = newId;
  }, [selected?.pothole_id]);

  return (
    <Box
      ref={containerRef}
      sx={{ flex: 1, width: "100%", height: "100%", minHeight: 0 }}
    />
  );
}
