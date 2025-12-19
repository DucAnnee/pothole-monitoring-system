import { X, MapPin, AlertCircle, Calendar, Activity } from 'lucide-react';
import {
  Box,
  Card,
  CardContent,
  Chip,
  Divider,
  IconButton,
  Tab,
  Tabs,
  Typography,
} from '@mui/material';
import { useState } from 'react';
import type { Pothole } from './PotholeData';
import { ImageWithFallback } from './figma/ImageWithFallback';

interface PotholeDetailsProps {
  pothole: Pothole;
  onClose: () => void;
}

export function PotholeDetails({ pothole, onClose }: PotholeDetailsProps) {
  const [activeTab, setActiveTab] = useState(0);

  const getSeverityColor = (severity: number) => {
    if (severity >= 8) return '#ef4444';
    if (severity >= 5) return '#f97316';
    return '#eab308';
  };

  const getSeverityLabel = (severity: number) => {
    if (severity >= 8) return 'Critical';
    if (severity >= 5) return 'Moderate';
    return 'Minor';
  };

  const getStatusStyles = (status: string) => {
    switch (status) {
      case 'reported':
        return { bgcolor: '#fef2f2', color: '#991b1b', borderColor: '#fecaca' };
      case 'in-progress':
        return { bgcolor: '#eff6ff', color: '#1e40af', borderColor: '#bfdbfe' };
      case 'fixed':
        return { bgcolor: '#f0fdf4', color: '#166534', borderColor: '#bbf7d0' };
      default:
        return { bgcolor: '#f3f4f6', color: '#374151', borderColor: '#d1d5db' };
    }
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  return (
    <Box sx={{ width: '100%', height: '100%', overflow: 'auto', p: 3, bgcolor: 'background.paper' }}>
      <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', mb: 3 }}>
        <Box>
          <Typography variant="h2" component="h2" sx={{ mb: 1 }}>Pothole Details</Typography>
          <Typography sx={{ color: 'grey.500' }}>ID: #{pothole.id}</Typography>
        </Box>
        <IconButton onClick={onClose} size="small">
          <X style={{ width: 20, height: 20 }} />
        </IconButton>
      </Box>

      <Card sx={{ p: 2, mb: 3 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <AlertCircle style={{ width: 20, height: 20, color: '#ef4444' }} />
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
                  bgcolor: getSeverityColor(pothole.severity),
                }}
              >
                <Typography sx={{ color: 'white', fontWeight: 600 }}>{pothole.severity}</Typography>
              </Box>
              <Chip
                label={getSeverityLabel(pothole.severity)}
                variant="outlined"
                size="small"
                sx={{ fontWeight: 500 }}
              />
            </Box>
          </Box>
          <Divider sx={{ my: 2 }} />
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Activity style={{ width: 20, height: 20, color: '#6b7280' }} />
              <Typography sx={{ color: 'grey.500' }}>Status:</Typography>
              <Chip
                label={pothole.status.replace('-', ' ').toUpperCase()}
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
              <Typography sx={{ color: 'grey.500' }}>Reported:</Typography>
              <Typography>{new Date(pothole.dateReported).toLocaleDateString()}</Typography>
            </Box>
          </Box>
        </CardContent>
      </Card>

      <Card sx={{ p: 2, mb: 3 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1, mb: 1.5 }}>
            <MapPin style={{ width: 20, height: 20, color: '#6b7280', marginTop: 4 }} />
            <Box>
              <Typography variant="h3" component="h3" sx={{ mb: 0.5 }}>Location</Typography>
              <Typography sx={{ color: 'grey.600' }}>{pothole.position.address}</Typography>
              <Typography sx={{ color: 'grey.400', mt: 0.5, fontSize: '0.875rem' }}>
                {pothole.position.lat.toFixed(6)}, {pothole.position.lng.toFixed(6)}
              </Typography>
            </Box>
          </Box>
        </CardContent>
      </Card>

      <Card sx={{ p: 2, mb: 3 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Typography variant="h3" component="h3" sx={{ mb: 2 }}>Measurements</Typography>
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2 }}>
            <Box sx={{ textAlign: 'center', p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
              <Typography sx={{ color: 'grey.500', mb: 0.5, fontSize: '0.875rem' }}>Depth</Typography>
              <Typography sx={{ color: '#ef4444', fontWeight: 600 }}>{pothole.measurements.depth} cm</Typography>
            </Box>
            <Box sx={{ textAlign: 'center', p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
              <Typography sx={{ color: 'grey.500', mb: 0.5, fontSize: '0.875rem' }}>Width</Typography>
              <Typography sx={{ color: '#ef4444', fontWeight: 600 }}>{pothole.measurements.width} cm</Typography>
            </Box>
            <Box sx={{ textAlign: 'center', p: 1.5, bgcolor: 'grey.50', borderRadius: 2 }}>
              <Typography sx={{ color: 'grey.500', mb: 0.5, fontSize: '0.875rem' }}>Length</Typography>
              <Typography sx={{ color: '#ef4444', fontWeight: 600 }}>{pothole.measurements.length} cm</Typography>
            </Box>
          </Box>
        </CardContent>
      </Card>

      <Card sx={{ p: 2 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Typography variant="h3" component="h3" sx={{ mb: 2 }}>Images</Typography>
          <Box sx={{ width: '100%' }}>
            <Box
              sx={{
                bgcolor: '#ececf0',
                borderRadius: 3,
                p: 0.5,
                display: 'flex',
                mb: 2,
              }}
            >
              <Tabs
                value={activeTab}
                onChange={handleTabChange}
                variant="fullWidth"
                sx={{
                  minHeight: 36,
                  width: '100%',
                  '& .MuiTabs-indicator': {
                    display: 'none',
                  },
                  '& .MuiTab-root': {
                    minHeight: 32,
                    borderRadius: 2.5,
                    flex: 1,
                    color: 'text.primary',
                    fontWeight: 500,
                    fontSize: '0.875rem',
                    textTransform: 'none',
                    '&.Mui-selected': {
                      bgcolor: 'background.paper',
                      color: 'text.primary',
                    },
                  },
                }}
              >
                <Tab label="Normal View" />
                <Tab label="Bird's Eye View" />
              </Tabs>
            </Box>
            <Box sx={{ mt: 2 }}>
              {activeTab === 0 && (
                <Box sx={{ position: 'relative', borderRadius: 2, overflow: 'hidden', bgcolor: 'grey.100' }}>
                  <ImageWithFallback
                    src={pothole.images.normal}
                    alt="Normal view of pothole"
                    style={{ width: '100%', height: 256, objectFit: 'cover' }}
                  />
                </Box>
              )}
              {activeTab === 1 && (
                <Box sx={{ position: 'relative', borderRadius: 2, overflow: 'hidden', bgcolor: 'grey.100' }}>
                  <ImageWithFallback
                    src={pothole.images.birdEye}
                    alt="Bird's eye view of pothole"
                    style={{ width: '100%', height: 256, objectFit: 'cover' }}
                  />
                </Box>
              )}
            </Box>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
