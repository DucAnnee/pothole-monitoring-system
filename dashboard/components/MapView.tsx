"use client";

import { useEffect, useRef, useMemo, useState } from "react";
import type { MapPothole, SeverityLevel } from "@/lib/api";
import { SEVERITY_COLORS, getMarkerSize } from "@/lib/api";

interface MapViewProps {
  potholes: MapPothole[];
  selectedPotholeId: string | null;
  onPotholeClick: (pothole: MapPothole) => void;
}

/**
 * Calculate the center and zoom level to fit all potholes
 */
function calculateBounds(potholes: MapPothole[]) {
  if (potholes.length === 0) {
    // Default to Ho Chi Minh City center if no potholes
    return { center: [10.7769, 106.7009] as [number, number], zoom: 13 };
  }

  let minLat = Infinity, maxLat = -Infinity;
  let minLon = Infinity, maxLon = -Infinity;

  potholes.forEach((p) => {
    minLat = Math.min(minLat, p.gps_lat);
    maxLat = Math.max(maxLat, p.gps_lat);
    minLon = Math.min(minLon, p.gps_lon);
    maxLon = Math.max(maxLon, p.gps_lon);
  });

  const center: [number, number] = [
    (minLat + maxLat) / 2,
    (minLon + maxLon) / 2,
  ];

  // Calculate zoom level based on bounds
  const latDiff = maxLat - minLat;
  const lonDiff = maxLon - minLon;
  const maxDiff = Math.max(latDiff, lonDiff);

  // Approximate zoom level calculation
  // Larger diff = lower zoom, smaller diff = higher zoom
  let zoom = 13;
  if (maxDiff > 0.5) zoom = 10;
  else if (maxDiff > 0.2) zoom = 11;
  else if (maxDiff > 0.1) zoom = 12;
  else if (maxDiff > 0.05) zoom = 13;
  else if (maxDiff > 0.02) zoom = 14;
  else if (maxDiff > 0.01) zoom = 15;
  else zoom = 16;

  return { center, zoom };
}

export function MapView({
  potholes,
  selectedPotholeId,
  onPotholeClick,
}: MapViewProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<any>(null);
  const markersRef = useRef<Map<string, any>>(new Map());
  const [mapReady, setMapReady] = useState(false);

  // Calculate initial bounds
  const { center, zoom } = useMemo(() => calculateBounds(potholes), [potholes]);

  // Initialize map
  useEffect(() => {
    // Only run on client side
    if (typeof window === "undefined") return;

    const initMap = async () => {
      // Dynamically import Leaflet to avoid SSR issues
      const L = (await import("leaflet")).default;

      if (mapRef.current && !mapInstanceRef.current) {
        // Initialize map with default center
        const map = L.map(mapRef.current).setView([10.7769, 106.7009], 13);

        // Add OpenStreetMap tiles
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          attribution: '© OpenStreetMap contributors',
        }).addTo(map);

        mapInstanceRef.current = map;
        setMapReady(true);
      }
    };

    initMap();

    return () => {
      if (mapInstanceRef.current) {
        mapInstanceRef.current.remove();
        mapInstanceRef.current = null;
        markersRef.current.clear();
        setMapReady(false);
      }
    };
  }, []);

  // Update map view and markers when potholes change
  useEffect(() => {
    if (!mapReady || !mapInstanceRef.current) return;

    const updateMarkers = async () => {
      const L = (await import("leaflet")).default;
      const map = mapInstanceRef.current;

      // Clear existing markers
      markersRef.current.forEach((marker) => {
        map.removeLayer(marker);
      });
      markersRef.current.clear();

      if (potholes.length === 0) return;

      // Update map view to fit potholes
      map.setView(center, zoom);

      // Add markers for each pothole
      potholes.forEach((pothole) => {
        const colors = SEVERITY_COLORS[pothole.severity_level] || SEVERITY_COLORS.MINOR;
        const radius = getMarkerSize(pothole.severity_level);

        // Create a circle marker
        const marker = L.circleMarker(
          [pothole.gps_lat, pothole.gps_lon],
          {
            radius: radius,
            fillColor: colors.primary,
            color: colors.secondary,
            weight: 2,
            opacity: 0.8,
            fillOpacity: 0.6,
          }
        );

        marker.on("click", () => {
          onPotholeClick(pothole);
        });

        marker.addTo(map);
        markersRef.current.set(pothole.pothole_id, marker);
      });
    };

    updateMarkers();
  }, [mapReady, potholes, center, zoom, onPotholeClick]);

  // Highlight selected pothole
  useEffect(() => {
    if (markersRef.current.size === 0) return;

    markersRef.current.forEach((marker, id) => {
      const pothole = potholes.find((p) => p.pothole_id === id);
      if (!pothole) return;

      const isSelected = selectedPotholeId === id;
      const colors = SEVERITY_COLORS[pothole.severity_level] || SEVERITY_COLORS.MINOR;

      marker.setStyle({
        fillColor: isSelected ? "#fbbf24" : colors.primary,
        color: isSelected ? "#f59e0b" : colors.secondary,
        weight: isSelected ? 3 : 2,
        opacity: isSelected ? 1 : 0.8,
        fillOpacity: isSelected ? 0.8 : 0.6,
      });

      // Bring selected marker to front
      if (isSelected) {
        marker.bringToFront();
      }
    });
  }, [selectedPotholeId, potholes]);

  return <div ref={mapRef} className="w-full h-full" />;
}