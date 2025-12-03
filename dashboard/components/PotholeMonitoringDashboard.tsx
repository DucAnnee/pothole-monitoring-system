"use client";

import { useState, useCallback } from "react";
import { MapView } from "./MapView";
import { PotholeDetails } from "./PotholeDetails";
import { potholes, type Pothole } from "./PotholeData";
import { Card } from "./ui/card";
import { Badge } from "./ui/badge";
import { AlertCircle, MapPin, ChevronDown, ChevronUp } from "lucide-react";
import { Button } from "./ui/button";

export function PotholeMonitoringDashboard() {
  const [selectedPothole, setSelectedPothole] = useState<Pothole | null>(null);
  const [isLegendExpanded, setIsLegendExpanded] = useState(true);

  const handlePotholeClick = useCallback((pothole: Pothole) => {
    setSelectedPothole(pothole);
  }, []);

  const handleCloseDetails = useCallback(() => {
    setSelectedPothole(null);
  }, []);

  // Calculate stats
  const criticalCount = potholes.filter((p) => p.severity >= 8).length;
  const moderateCount = potholes.filter(
    (p) => p.severity >= 5 && p.severity < 8
  ).length;
  const minorCount = potholes.filter((p) => p.severity < 5).length;

  return (
    <div className="w-screen h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4 shadow-sm">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-red-500 rounded-lg flex items-center justify-center">
              <MapPin className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1>Pothole Monitoring Dashboard</h1>
              <p className="text-gray-500">Road condition tracking</p>
            </div>
          </div>

          <div className="flex items-center gap-4">
            <Card className="px-4 py-2 bg-red-50 border-red-200">
              <div className="flex items-center gap-2">
                <AlertCircle className="w-5 h-5 text-red-500" />
                <div>
                  <div className="text-gray-500">Critical</div>
                  <div className="text-red-600">{criticalCount} potholes</div>
                </div>
              </div>
            </Card>

            <Card className="px-4 py-2 bg-orange-50 border-orange-200">
              <div className="flex items-center gap-2">
                <AlertCircle className="w-5 h-5 text-orange-500" />
                <div>
                  <div className="text-gray-500">Moderate</div>
                  <div className="text-orange-600">{moderateCount} potholes</div>
                </div>
              </div>
            </Card>

            <Card className="px-4 py-2 bg-yellow-50 border-yellow-200">
              <div className="flex items-center gap-2">
                <AlertCircle className="w-5 h-5 text-yellow-500" />
                <div>
                  <div className="text-gray-500">Minor</div>
                  <div className="text-yellow-600">{minorCount} potholes</div>
                </div>
              </div>
            </Card>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Map Container */}
        <div className="flex-1 relative">
          <MapView
            potholes={potholes}
            selectedPothole={selectedPothole}
            onPotholeClick={handlePotholeClick}
          />

          {/* Legend */}
          <Card className="absolute bottom-6 left-6 shadow-lg overflow-hidden">
            <div className="flex items-center justify-between p-4 pb-3">
              <h3>Severity Scale</h3>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                onClick={() => setIsLegendExpanded(!isLegendExpanded)}
              >
                {isLegendExpanded ? (
                  <ChevronDown className="w-4 h-4" />
                ) : (
                  <ChevronUp className="w-4 h-4" />
                )}
              </Button>
            </div>

            {isLegendExpanded && (
              <div className="px-4 pb-4">
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <div className="w-8 h-8 bg-red-500 rounded-full" />
                    <span className="text-gray-600">Critical (8-10)</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-6 h-6 bg-orange-500 rounded-full" />
                    <span className="text-gray-600">Moderate (5-7)</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-yellow-500 rounded-full" />
                    <span className="text-gray-600">Minor (1-4)</span>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t border-gray-200">
                  <p className="text-gray-500">
                    Click on any marker to view details
                  </p>
                </div>
              </div>
            )}
          </Card>
        </div>

        {/* Details Panel */}
        {selectedPothole && (
          <div className="w-96 border-l border-gray-200 bg-white shadow-xl">
            <PotholeDetails
              pothole={selectedPothole}
              onClose={handleCloseDetails}
            />
          </div>
        )}
      </div>
    </div>
  );
}