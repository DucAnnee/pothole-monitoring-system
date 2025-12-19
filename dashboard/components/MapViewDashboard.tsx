'use client';

import { useState, useCallback } from "react";
import { MapView } from "@/components/MapView";
import { PotholeDetails } from "@/components/PotholeDetails";
import { potholes, type Pothole } from "@/components/PotholeData";
import {
  Box,
  Card,
  CardContent,
  IconButton,
  Typography,
} from "@mui/material";
import {
  AlertCircle,
  MapPin,
  ChevronDown,
  ChevronUp,
  ArrowLeft,
  LogOut,
} from "lucide-react";
import { useAuth } from "./auth/AuthContext";

interface MapViewDashboardProps {
  onNavigateToOverview: () => void;
}

export function MapViewDashboard({ onNavigateToOverview }: MapViewDashboardProps) {
  const { logout } = useAuth();
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
  const moderateCount = potholes.filter((p) => p.severity >= 5 && p.severity < 8).length;
  const minorCount = potholes.filter((p) => p.severity < 5).length;

  return (
    <Box sx={{ width: '100vw', height: '100vh', display: 'flex', flexDirection: 'column', bgcolor: 'grey.50' }}>
      {/* Header */}
      <Box
        component="header"
        sx={{
          bgcolor: 'background.paper',
          borderBottom: '1px solid',
          borderColor: 'grey.200',
          px: 3,
          py: 2,
          boxShadow: '0 1px 2px 0 rgb(0 0 0 / 0.05)',
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Box
              component="button"
              onClick={onNavigateToOverview}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                px: 2,
                py: 1,
                bgcolor: 'grey.100',
                border: 'none',
                borderRadius: 1.5,
                cursor: 'pointer',
                color: 'grey.700',
                fontWeight: 500,
                fontSize: '0.875rem',
                '&:hover': {
                  bgcolor: 'grey.200',
                },
              }}
            >
              <ArrowLeft style={{ width: 18, height: 18 }} />
              Overview
            </Box>
            
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <Box
                sx={{
                  width: 40,
                  height: 40,
                  bgcolor: '#84cc16',
                  borderRadius: 2,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                }}
              >
                <MapPin style={{ width: 24, height: 24, color: 'white' }} />
              </Box>
              <Box>
                <Typography variant="h1" component="h1">Pothole Map View</Typography>
                <Typography sx={{ color: 'grey.500' }}>Interactive road condition map</Typography>
              </Box>
            </Box>
          </Box>

          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Card sx={{ px: 2, py: 1, bgcolor: '#fef2f2', borderColor: '#fecaca' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: '#ef4444' }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Critical</Typography>
                  <Typography sx={{ color: '#dc2626', fontWeight: 500 }}>{criticalCount} potholes</Typography>
                </Box>
              </CardContent>
            </Card>

            <Card sx={{ px: 2, py: 1, bgcolor: '#fff7ed', borderColor: '#fed7aa' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: '#f97316' }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Moderate</Typography>
                  <Typography sx={{ color: '#ea580c', fontWeight: 500 }}>{moderateCount} potholes</Typography>
                </Box>
              </CardContent>
            </Card>

            <Card sx={{ px: 2, py: 1, bgcolor: '#fefce8', borderColor: '#fde047' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: '#eab308' }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Minor</Typography>
                  <Typography sx={{ color: '#ca8a04', fontWeight: 500 }}>{minorCount} potholes</Typography>
                </Box>
              </CardContent>
            </Card>
            
            <Box
              component="button"
              onClick={logout}
              sx={{
                px: 2,
                py: 1,
                bgcolor: 'transparent',
                color: 'grey.600',
                border: '1px solid',
                borderColor: 'grey.300',
                borderRadius: 1.5,
                fontWeight: 500,
                fontSize: '0.875rem',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: 0.75,
                '&:hover': {
                  bgcolor: 'grey.100',
                },
              }}
            >
              <LogOut style={{ width: 16, height: 16 }} />
              Logout
            </Box>
          </Box>
        </Box>
      </Box>

      {/* Main Content */}
      <Box sx={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
        {/* Map Container */}
        <Box sx={{ flex: 1, position: 'relative' }}>
          <MapView
            potholes={potholes}
            selectedPothole={selectedPothole}
            onPotholeClick={handlePotholeClick}
          />

          {/* Legend */}
          <Card
            sx={{
              position: 'absolute',
              bottom: 24,
              left: 24,
              boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1)',
              overflow: 'hidden',
              zIndex: 999,
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', p: 2, pb: 1.5 }}>
              <Typography variant="h3" component="h3">Severity Scale</Typography>
              <IconButton
                size="small"
                onClick={() => setIsLegendExpanded(!isLegendExpanded)}
                sx={{ width: 24, height: 24 }}
              >
                {isLegendExpanded ? (
                  <ChevronDown style={{ width: 16, height: 16 }} />
                ) : (
                  <ChevronUp style={{ width: 16, height: 16 }} />
                )}
              </IconButton>
            </Box>

            {isLegendExpanded && (
              <Box sx={{ px: 2, pb: 2 }}>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 32, height: 32, bgcolor: '#ef4444', borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>Critical (8–10)</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 24, height: 24, bgcolor: '#f97316', borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>Moderate (5–7)</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 16, bgcolor: '#eab308', borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>Minor (1–4)</Typography>
                  </Box>
                </Box>
                <Box sx={{ mt: 1.5, pt: 1.5, borderTop: '1px solid', borderColor: 'grey.200' }}>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>
                    Click on any marker to view details
                  </Typography>
                </Box>
              </Box>
            )}
          </Card>
        </Box>

        {/* Details Panel */}
        {selectedPothole && (
          <Box sx={{ width: 384, borderLeft: '1px solid', borderColor: 'grey.200', bgcolor: 'background.paper', boxShadow: '-4px 0 15px -3px rgb(0 0 0 / 0.1)' }}>
            <PotholeDetails pothole={selectedPothole} onClose={handleCloseDetails} />
          </Box>
        )}
      </Box>
    </Box>
  );
}
