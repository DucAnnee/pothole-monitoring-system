"use client";

import { useEffect, useRef } from "react";
import type { Pothole } from "./PotholeData";

interface MapViewProps {
  potholes: Pothole[];
  selectedPothole: Pothole | null;
  onPotholeClick: (pothole: Pothole) => void;
}

export function MapView({
  potholes,
  selectedPothole,
  onPotholeClick,
}: MapViewProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<any>(null);
  const markersRef = useRef<any[]>([]);

  useEffect(() => {
    // Only run on client side
    if (typeof window === "undefined") return;

    const initMap = async () => {
      // Dynamically import Leaflet to avoid SSR issues
      const L = (await import("leaflet")).default;

      if (mapRef.current && !mapInstanceRef.current) {
        // Initialize map centered on the potholes
        const map = L.map(mapRef.current).setView([40.758, -73.9855], 14);

        // Add OpenStreetMap tiles
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          attribution: '© OpenStreetMap contributors',
        }).addTo(map);

        mapInstanceRef.current = map;

        // Add markers for each pothole
        potholes.forEach((pothole) => {
          // Determine color based on severity
          let color = "#ef4444"; // red for critical
          if (pothole.severity < 5) {
            color = "#eab308"; // yellow for minor
          } else if (pothole.severity < 8) {
            color = "#f97316"; // orange for moderate
          }

          // Calculate radius based on severity
          const radius = 10 + pothole.severity * 3;

          // Create a circle marker
          const marker = L.circleMarker(
            [pothole.position.lat, pothole.position.lng],
            {
              radius: radius,
              fillColor: color,
              color: color === "#ef4444" ? "#dc2626" : color === "#f97316" ? "#ea580c" : "#ca8a04",
              weight: 2,
              opacity: 0.8,
              fillOpacity: 0.6,
            }
          );

          marker.on("click", () => {
            onPotholeClick(pothole);
          });

          marker.addTo(map);
          markersRef.current.push({ id: pothole.id, marker });
        });
      }
    };

    initMap();

    return () => {
      if (mapInstanceRef.current) {
        mapInstanceRef.current.remove();
        mapInstanceRef.current = null;
        markersRef.current = [];
      }
    };
  }, [potholes, onPotholeClick]);

  // Highlight selected pothole
  useEffect(() => {
    if (markersRef.current.length > 0) {
      markersRef.current.forEach(({ id, marker }) => {
        const pothole = potholes.find((p) => p.id === id);
        if (!pothole) return;

        const isSelected = selectedPothole?.id === id;

        // Determine base color
        let baseColor = "#ef4444";
        let borderColor = "#dc2626";
        if (pothole.severity < 5) {
          baseColor = "#eab308";
          borderColor = "#ca8a04";
        } else if (pothole.severity < 8) {
          baseColor = "#f97316";
          borderColor = "#ea580c";
        }

        marker.setStyle({
          fillColor: isSelected ? "#fbbf24" : baseColor,
          color: isSelected ? "#f59e0b" : borderColor,
          weight: isSelected ? 3 : 2,
          opacity: isSelected ? 1 : 0.8,
          fillOpacity: isSelected ? 0.8 : 0.6,
        });
      });
    }
  }, [selectedPothole, potholes]);

  return <div ref={mapRef} className="w-full h-full" />;
}