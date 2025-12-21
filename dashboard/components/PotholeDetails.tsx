import { X, MapPin, AlertCircle, Calendar, Activity, Ruler } from 'lucide-react';
import {
  Box,
  Card,
  CardContent,
  Chip,
  Divider,
  IconButton,
  Typography,
} from '@mui/material';
import type { PotholeDetail, SeverityLevel } from '@/lib/api';
import { SEVERITY_COLORS } from '@/lib/api';

interface PotholeDetailsPanelProps {
  pothole: PotholeDetail;
  onClose: () => void;
}

/**
 * Build address string from available address components
 * Skips any null/undefined values
 */
function buildAddressString(pothole: PotholeDetail): string | null {
  const parts: string[] = [];
  
  if (pothole.street_name) parts.push(pothole.street_name);
  if (pothole.ward) parts.push(pothole.ward);
  if (pothole.district) parts.push(pothole.district);
  if (pothole.city) parts.push(pothole.city);
  
  return parts.length > 0 ? parts.join(', ') : null;
}

export function PotholeDetailsPanel({ pothole, onClose }: PotholeDetailsPanelProps) {
  const colors = SEVERITY_COLORS[pothole.severity_level] || SEVERITY_COLORS.MINOR;
  
  const getStatusStyles = (status: string) => {
    switch (status) {
      case 'reported':
        return { bgcolor: '#fef2f2', color: '#991b1b', borderColor: '#fecaca' };
      case 'in_progress':
        return { bgcolor: '#eff6ff', color: '#1e40af', borderColor: '#bfdbfe' };
      case 'fixed':
        return { bgcolor: '#f0fdf4', color: '#166534', borderColor: '#bbf7d0' };
      default:
        return { bgcolor: '#f3f4f6', color: '#374151', borderColor: '#d1d5db' };
    }
  };

  const formatStatus = (status: string) => {
    return status.replace('_', ' ').toUpperCase();
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return 'N/A';
    try {
      return new Date(dateStr).toLocaleDateString();
    } catch {
      return dateStr;
    }
  };

  const addressString = buildAddressString(pothole);

  // Calculate estimated dimensions from surface area (assuming roughly circular)
  const estimatedDiameter = pothole.surface_area_cm2 
    ? Math.round(Math.sqrt(pothole.surface_area_cm2 / Math.PI) * 2) 
    : null;

  return (
    <Box sx={{ width: '100%', height: '100%', overflow: 'auto', p: 3, bgcolor: 'background.paper' }}>
      <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', mb: 3 }}>
        <Box>
          <Typography variant="h2" component="h2" sx={{ mb: 1 }}>Pothole Details</Typography>
          <Typography sx={{ color: 'grey.500', fontSize: '0.75rem', fontFamily: 'monospace' }}>
            {pothole.pothole_id}
          </Typography>
        </Box>
        <IconButton onClick={onClose} size="small">
          <X style={{ width: 20, height: 20 }} />
        </IconButton>
      </Box>

      {/* Severity Card */}
      <Card sx={{ p: 2, mb: 3 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <AlertCircle style={{ width: 20, height: 20, color: colors.primary }} />
              <Typography>Severity Level</Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Box
                sx={{
                  width: 48,
                  height: 48,
                  borderRadius: '50%',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  bgcolor: colors.primary,
                }}
              >
                <Typography sx={{ color: 'white', fontWeight: 600, fontSize: '0.875rem' }}>
                  {pothole.severity_score != null ? pothole.severity_score.toFixed(1) : 'N/A'}
                </Typography>
              </Box>
              <Chip
                label={pothole.severity_level}
                variant="outlined"
                size="small"
                sx={{ fontWeight: 500, borderColor: colors.primary, color: colors.secondary }}
              />
            </Box>
          </Box>
          <Divider sx={{ my: 2 }} />
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Activity style={{ width: 20, height: 20, color: '#6b7280' }} />
              <Typography sx={{ color: 'grey.500' }}>Status:</Typography>
              <Chip
                label={formatStatus(pothole.status)}
                variant="outlined"
                size="small"
                sx={{
                  ...getStatusStyles(pothole.status),
                  border: '1px solid',
                  fontWeight: 500,
                  fontSize: '0.75rem',
                }}
              />
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Calendar style={{ width: 20, height: 20, color: '#6b7280' }} />
              <Typography sx={{ color: 'grey.500' }}>Detected:</Typography>
              <Typography>{formatDate(pothole.detected_at)}</Typography>
            </Box>
            {pothole.observation_count > 1 && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Activity style={{ width: 20, height: 20, color: '#6b7280' }} />
                <Typography sx={{ color: 'grey.500' }}>Observations:</Typography>
                <Typography>{pothole.observation_count}</Typography>
              </Box>
            )}
          </Box>
        </CardContent>
      </Card>

      {/* Location Card */}
      <Card sx={{ p: 2, mb: 3 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1, mb: 1.5 }}>
            <MapPin style={{ width: 20, height: 20, color: '#6b7280', marginTop: 4 }} />
            <Box>
              <Typography variant="h3" component="h3" sx={{ mb: 0.5 }}>Location</Typography>
              {addressString ? (
                <Typography sx={{ color: 'grey.600' }}>{addressString}</Typography>
              ) : (
                <Typography sx={{ color: 'grey.400', fontStyle: 'italic' }}>Address not available</Typography>
              )}
              <Typography sx={{ color: 'grey.400', mt: 0.5, fontSize: '0.875rem' }}>
                {pothole.gps_lat.toFixed(6)}, {pothole.gps_lon.toFixed(6)}
              </Typography>
              {pothole.road_id && (
                <Typography sx={{ color: 'grey.400', mt: 0.5, fontSize: '0.75rem' }}>
                  Road ID: {pothole.road_id}
                </Typography>
              )}
            </Box>
          </Box>
        </CardContent>
      </Card>

      {/* Measurements Card */}
      <Card sx={{ p: 2, mb: 3 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <Ruler style={{ width: 20, height: 20, color: '#6b7280' }} />
            <Typography variant="h3" component="h3">Measurements</Typography>
          </Box>
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 2 }}>
            <Box sx={{ textAlign: 'center', p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
              <Typography sx={{ color: 'grey.500', mb: 0.5, fontSize: '0.875rem' }}>Depth</Typography>
              <Typography sx={{ color: colors.primary, fontWeight: 600 }}>
                {pothole.depth_cm != null ? `${pothole.depth_cm.toFixed(1)} cm` : 'N/A'}
              </Typography>
            </Box>
            <Box sx={{ textAlign: 'center', p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
              <Typography sx={{ color: 'grey.500', mb: 0.5, fontSize: '0.875rem' }}>Surface Area</Typography>
              <Typography sx={{ color: colors.primary, fontWeight: 600 }}>
                {pothole.surface_area_cm2 != null ? `${pothole.surface_area_cm2.toFixed(0)} cm²` : 'N/A'}
              </Typography>
            </Box>
            {estimatedDiameter && (
              <Box sx={{ gridColumn: 'span 2', textAlign: 'center', p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
                <Typography sx={{ color: 'grey.500', mb: 0.5, fontSize: '0.875rem' }}>Est. Diameter</Typography>
                <Typography sx={{ color: colors.primary, fontWeight: 600 }}>
                  ~{estimatedDiameter} cm
                </Typography>
              </Box>
            )}
          </Box>
        </CardContent>
      </Card>

      {/* Timeline Card */}
      <Card sx={{ p: 2 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Typography variant="h3" component="h3" sx={{ mb: 2 }}>Timeline</Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Box sx={{ width: 8, height: 8, bgcolor: '#ef4444', borderRadius: '50%' }} />
              <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Detected:</Typography>
              <Typography sx={{ fontSize: '0.875rem' }}>{formatDate(pothole.detected_at)}</Typography>
            </Box>
            {pothole.in_progress_at && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Box sx={{ width: 8, height: 8, bgcolor: '#3b82f6', borderRadius: '50%' }} />
                <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>In Progress:</Typography>
                <Typography sx={{ fontSize: '0.875rem' }}>{formatDate(pothole.in_progress_at)}</Typography>
              </Box>
            )}
            {pothole.fixed_at && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Box sx={{ width: 8, height: 8, bgcolor: '#22c55e', borderRadius: '50%' }} />
                <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>Fixed:</Typography>
                <Typography sx={{ fontSize: '0.875rem' }}>{formatDate(pothole.fixed_at)}</Typography>
              </Box>
            )}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1, pt: 1, borderTop: '1px solid', borderColor: 'grey.200' }}>
              <Typography sx={{ color: 'grey.400', fontSize: '0.75rem' }}>
                Last updated: {formatDate(pothole.last_updated)}
              </Typography>
            </Box>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
