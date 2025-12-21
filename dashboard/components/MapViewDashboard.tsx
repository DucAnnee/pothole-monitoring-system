'use client';

import { useState, useCallback, useEffect } from "react";
import { MapView } from "@/components/MapView";
import { PotholeDetailsPanel } from "@/components/PotholeDetails";
import {
  Box,
  Card,
  CardContent,
  IconButton,
  Typography,
  CircularProgress,
} from "@mui/material";
import {
  AlertCircle,
  MapPin,
  ChevronDown,
  ChevronUp,
  ArrowLeft,
  LogOut,
  RefreshCw,
} from "lucide-react";
import { useAuth } from "./auth/AuthContext";
import {
  getMapView,
  getPotholeDetail,
  type MapPothole,
  type PotholeDetail,
  type SeverityLevel,
  SEVERITY_COLORS,
} from "@/lib/api";

interface MapViewDashboardProps {
  onNavigateToOverview: () => void;
}

export function MapViewDashboard({ onNavigateToOverview }: MapViewDashboardProps) {
  const { logout } = useAuth();
  const [potholes, setPotholes] = useState<MapPothole[]>([]);
  const [selectedPothole, setSelectedPothole] = useState<PotholeDetail | null>(null);
  const [isLegendExpanded, setIsLegendExpanded] = useState(true);
  const [isLoading, setIsLoading] = useState(true);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch potholes from API
  const fetchPotholes = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await getMapView();
      setPotholes(response.potholes);
    } catch (err) {
      console.error('Failed to fetch potholes:', err);
      setError(err instanceof Error ? err.message : 'Failed to load potholes');
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Initial fetch
  useEffect(() => {
    fetchPotholes();
  }, [fetchPotholes]);

  const handlePotholeClick = useCallback(async (pothole: MapPothole) => {
    setIsLoadingDetail(true);
    try {
      const detail = await getPotholeDetail(pothole.pothole_id);
      setSelectedPothole(detail);
    } catch (err) {
      console.error('Failed to fetch pothole detail:', err);
      setSelectedPothole(null);
    } finally {
      setIsLoadingDetail(false);
    }
  }, []);

  const handleCloseDetails = useCallback(() => {
    setSelectedPothole(null);
  }, []);

  // Calculate stats by severity level
  const severityCounts: Record<SeverityLevel, number> = {
    CRITICAL: 0,
    HIGH: 0,
    MODERATE: 0,
    MINOR: 0,
  };
  
  potholes.forEach((p) => {
    if (p.severity_level in severityCounts) {
      severityCounts[p.severity_level]++;
    }
  });

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
            {/* Refresh button */}
            <IconButton
              onClick={fetchPotholes}
              disabled={isLoading}
              sx={{ color: 'grey.600' }}
            >
              <RefreshCw style={{ width: 20, height: 20 }} className={isLoading ? 'animate-spin' : ''} />
            </IconButton>

            {/* Severity counts */}
            <Card sx={{ px: 2, py: 1, bgcolor: SEVERITY_COLORS.CRITICAL.bg, borderColor: '#fecaca' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: SEVERITY_COLORS.CRITICAL.primary }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Critical</Typography>
                  <Typography sx={{ color: SEVERITY_COLORS.CRITICAL.secondary, fontWeight: 500 }}>{severityCounts.CRITICAL} potholes</Typography>
                </Box>
              </CardContent>
            </Card>

            <Card sx={{ px: 2, py: 1, bgcolor: SEVERITY_COLORS.HIGH.bg, borderColor: '#fed7aa' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: SEVERITY_COLORS.HIGH.primary }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>High</Typography>
                  <Typography sx={{ color: SEVERITY_COLORS.HIGH.secondary, fontWeight: 500 }}>{severityCounts.HIGH} potholes</Typography>
                </Box>
              </CardContent>
            </Card>

            <Card sx={{ px: 2, py: 1, bgcolor: SEVERITY_COLORS.MODERATE.bg, borderColor: '#fde047' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: SEVERITY_COLORS.MODERATE.primary }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Moderate</Typography>
                  <Typography sx={{ color: SEVERITY_COLORS.MODERATE.secondary, fontWeight: 500 }}>{severityCounts.MODERATE} potholes</Typography>
                </Box>
              </CardContent>
            </Card>

            <Card sx={{ px: 2, py: 1, bgcolor: SEVERITY_COLORS.MINOR.bg, borderColor: '#bbf7d0' }}>
              <CardContent sx={{ p: '0 !important', display: 'flex', alignItems: 'center', gap: 1 }}>
                <AlertCircle style={{ width: 20, height: 20, color: SEVERITY_COLORS.MINOR.primary }} />
                <Box>
                  <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Minor</Typography>
                  <Typography sx={{ color: SEVERITY_COLORS.MINOR.secondary, fontWeight: 500 }}>{severityCounts.MINOR} potholes</Typography>
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
          {isLoading ? (
            <Box sx={{ 
              width: '100%', 
              height: '100%', 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              bgcolor: 'grey.100',
            }}>
              <CircularProgress sx={{ color: '#84cc16' }} />
            </Box>
          ) : error ? (
            <Box sx={{ 
              width: '100%', 
              height: '100%', 
              display: 'flex', 
              flexDirection: 'column',
              alignItems: 'center', 
              justifyContent: 'center',
              bgcolor: 'grey.100',
              gap: 2,
            }}>
              <Typography color="error">{error}</Typography>
              <Box
                component="button"
                onClick={fetchPotholes}
                sx={{
                  px: 3,
                  py: 1,
                  bgcolor: '#84cc16',
                  color: 'white',
                  border: 'none',
                  borderRadius: 1.5,
                  fontWeight: 500,
                  cursor: 'pointer',
                }}
              >
                Retry
              </Box>
            </Box>
          ) : (
            <MapView
              potholes={potholes}
              selectedPotholeId={selectedPothole?.pothole_id ?? null}
              onPotholeClick={handlePotholeClick}
            />
          )}

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
                    <Box sx={{ width: 16, height: 16, bgcolor: SEVERITY_COLORS.CRITICAL.primary, borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>Critical</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 14, height: 14, bgcolor: SEVERITY_COLORS.HIGH.primary, borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>High</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 12, height: 12, bgcolor: SEVERITY_COLORS.MODERATE.primary, borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>Moderate</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 10, height: 10, bgcolor: SEVERITY_COLORS.MINOR.primary, borderRadius: '50%' }} />
                    <Typography sx={{ color: 'grey.600' }}>Minor</Typography>
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
        {(selectedPothole || isLoadingDetail) && (
          <Box sx={{ width: 384, borderLeft: '1px solid', borderColor: 'grey.200', bgcolor: 'background.paper', boxShadow: '-4px 0 15px -3px rgb(0 0 0 / 0.1)' }}>
            {isLoadingDetail ? (
              <Box sx={{ 
                width: '100%', 
                height: '100%', 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'center',
              }}>
                <CircularProgress sx={{ color: '#84cc16' }} size={32} />
              </Box>
            ) : selectedPothole ? (
              <PotholeDetailsPanel pothole={selectedPothole} onClose={handleCloseDetails} />
            ) : null}
          </Box>
        )}
      </Box>
    </Box>
  );
}
