import { X, MapPin, AlertCircle, Calendar, Activity, Ruler, Image as ImageIcon, ImageOff, ChevronLeft, ChevronRight, ZoomIn } from 'lucide-react';
import {
  Box,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  Modal,
  Typography,
} from '@mui/material';
import { useCallback, useEffect, useState } from 'react';
import type { PotholeDetail, SeverityLevel } from '@/lib/api';
import { SEVERITY_COLORS, getImageProxyUrl } from '@/lib/api';

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

/**
 * Fullscreen image lightbox modal
 */
interface ImageLightboxProps {
  open: boolean;
  onClose: () => void;
  images: Array<{ title: string; url: string | null }>;
  initialIndex: number;
}

function ImageLightbox({ open, onClose, images, initialIndex }: ImageLightboxProps) {
  const [currentIndex, setCurrentIndex] = useState(initialIndex);
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);

  // Reset state when modal opens with new index
  useEffect(() => {
    if (open) {
      setCurrentIndex(initialIndex);
      setIsLoading(true);
      setHasError(false);
    }
  }, [open, initialIndex]);

  // Handle keyboard navigation
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (!open) return;
    
    switch (e.key) {
      case 'Escape':
        onClose();
        break;
      case 'ArrowLeft':
        if (currentIndex > 0) {
          setCurrentIndex(currentIndex - 1);
          setIsLoading(true);
          setHasError(false);
        }
        break;
      case 'ArrowRight':
        if (currentIndex < images.length - 1) {
          setCurrentIndex(currentIndex + 1);
          setIsLoading(true);
          setHasError(false);
        }
        break;
    }
  }, [open, onClose, currentIndex, images.length]);

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  const currentImage = images[currentIndex];
  const canGoPrev = currentIndex > 0;
  const canGoNext = currentIndex < images.length - 1;

  const handlePrev = () => {
    if (canGoPrev) {
      setCurrentIndex(currentIndex - 1);
      setIsLoading(true);
      setHasError(false);
    }
  };

  const handleNext = () => {
    if (canGoNext) {
      setCurrentIndex(currentIndex + 1);
      setIsLoading(true);
      setHasError(false);
    }
  };

  return (
    <Modal
      open={open}
      onClose={onClose}
      sx={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      <Box
        sx={{
          position: 'relative',
          width: '90vw',
          height: '90vh',
          maxWidth: 1200,
          bgcolor: 'rgba(0, 0, 0, 0.95)',
          borderRadius: 2,
          outline: 'none',
          display: 'flex',
          flexDirection: 'column',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            p: 2,
            borderBottom: '1px solid rgba(255,255,255,0.1)',
          }}
        >
          <Typography sx={{ color: 'white', fontWeight: 500 }}>
            {currentImage?.title}
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>
              {currentIndex + 1} / {images.length}
            </Typography>
            <IconButton onClick={onClose} sx={{ color: 'white' }}>
              <X style={{ width: 24, height: 24 }} />
            </IconButton>
          </Box>
        </Box>

        {/* Image container */}
        <Box
          sx={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            position: 'relative',
            p: 2,
          }}
        >
          {/* Previous button */}
          {images.length > 1 && (
            <IconButton
              onClick={handlePrev}
              disabled={!canGoPrev}
              sx={{
                position: 'absolute',
                left: 16,
                color: 'white',
                bgcolor: 'rgba(0,0,0,0.5)',
                '&:hover': { bgcolor: 'rgba(0,0,0,0.7)' },
                '&:disabled': { opacity: 0.3 },
              }}
            >
              <ChevronLeft style={{ width: 32, height: 32 }} />
            </IconButton>
          )}

          {/* Loading state */}
          {isLoading && currentImage?.url && (
            <Box
              sx={{
                position: 'absolute',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
            >
              <CircularProgress sx={{ color: 'white' }} />
            </Box>
          )}

          {/* Error state */}
          {(hasError || !currentImage?.url) && !isLoading && (
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: 2,
              }}
            >
              <ImageOff style={{ width: 64, height: 64, color: '#6b7280' }} />
              <Typography sx={{ color: 'grey.500' }}>
                {currentImage?.url ? 'Failed to load image' : 'No image available'}
              </Typography>
            </Box>
          )}

          {/* Image */}
          {currentImage?.url && (
            <img
              src={currentImage.url}
              alt={currentImage.title}
              onLoad={() => {
                setIsLoading(false);
                setHasError(false);
              }}
              onError={() => {
                setIsLoading(false);
                setHasError(true);
              }}
              style={{
                maxWidth: '100%',
                maxHeight: '100%',
                objectFit: 'contain',
                display: isLoading || hasError ? 'none' : 'block',
              }}
            />
          )}

          {/* Next button */}
          {images.length > 1 && (
            <IconButton
              onClick={handleNext}
              disabled={!canGoNext}
              sx={{
                position: 'absolute',
                right: 16,
                color: 'white',
                bgcolor: 'rgba(0,0,0,0.5)',
                '&:hover': { bgcolor: 'rgba(0,0,0,0.7)' },
                '&:disabled': { opacity: 0.3 },
              }}
            >
              <ChevronRight style={{ width: 32, height: 32 }} />
            </IconButton>
          )}
        </Box>

        {/* Footer hint */}
        <Box sx={{ p: 1.5, textAlign: 'center', borderTop: '1px solid rgba(255,255,255,0.1)' }}>
          <Typography sx={{ color: 'grey.600', fontSize: '0.75rem' }}>
            Press ESC to close • Use arrow keys to navigate
          </Typography>
        </Box>
      </Box>
    </Modal>
  );
}

/**
 * Image card component with loading and error states
 */
interface ImageCardProps {
  title: string;
  s3Path: string | null;
  onClick?: () => void;
}

function ImageCard({ title, s3Path, onClick }: ImageCardProps) {
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
  const [imageUrl, setImageUrl] = useState<string | null>(null);

  useEffect(() => {
    if (s3Path) {
      // Use proxy URL directly for simplicity and to avoid CORS issues
      const proxyUrl = getImageProxyUrl(s3Path);
      setImageUrl(proxyUrl);
      setIsLoading(false);
    } else {
      setIsLoading(false);
      setHasError(true);
    }
  }, [s3Path]);

  const handleImageLoad = () => {
    setIsLoading(false);
    setHasError(false);
  };

  const handleImageError = () => {
    setIsLoading(false);
    setHasError(true);
  };

  const isClickable = onClick && !hasError && imageUrl;

  return (
    <Box
      onClick={isClickable ? onClick : undefined}
      sx={{
        position: 'relative',
        width: '100%',
        aspectRatio: '4/3',
        bgcolor: 'grey.100',
        borderRadius: 2,
        overflow: 'hidden',
        display: 'flex',
        flexDirection: 'column',
        cursor: isClickable ? 'pointer' : 'default',
        transition: 'transform 0.2s, box-shadow 0.2s',
        '&:hover': isClickable ? {
          transform: 'scale(1.02)',
          boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
        } : {},
      }}
    >
      {/* Title overlay */}
      <Box
        sx={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          bgcolor: 'rgba(0,0,0,0.6)',
          color: 'white',
          px: 1.5,
          py: 0.5,
          zIndex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}
      >
        <Typography sx={{ fontSize: '0.75rem', fontWeight: 500 }}>{title}</Typography>
        {isClickable && (
          <ZoomIn style={{ width: 14, height: 14, opacity: 0.8 }} />
        )}
      </Box>

      {/* Loading state */}
      {isLoading && (
        <Box
          sx={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            bgcolor: 'grey.100',
          }}
        >
          <CircularProgress size={32} />
        </Box>
      )}

      {/* Error state */}
      {hasError && !isLoading && (
        <Box
          sx={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            bgcolor: 'grey.100',
            gap: 1,
          }}
        >
          <ImageOff style={{ width: 32, height: 32, color: '#9ca3af' }} />
          <Typography sx={{ color: 'grey.500', fontSize: '0.75rem' }}>
            {s3Path ? 'Failed to load image' : 'No image available'}
          </Typography>
        </Box>
      )}

      {/* Image */}
      {imageUrl && !hasError && (
        <img
          src={imageUrl}
          alt={title}
          onLoad={handleImageLoad}
          onError={handleImageError}
          style={{
            width: '100%',
            height: '100%',
            objectFit: 'contain',
            display: isLoading ? 'none' : 'block',
          }}
        />
      )}
    </Box>
  );
}

export function PotholeDetailsPanel({ pothole, onClose }: PotholeDetailsPanelProps) {
  const colors = SEVERITY_COLORS[pothole.severity_level] || SEVERITY_COLORS.MINOR;
  
  // Lightbox state
  const [lightboxOpen, setLightboxOpen] = useState(false);
  const [lightboxInitialIndex, setLightboxInitialIndex] = useState(0);
  
  // Prepare images for lightbox
  const lightboxImages = [
    { 
      title: 'Raw Image', 
      url: pothole.raw_image_path ? getImageProxyUrl(pothole.raw_image_path) : null 
    },
    { 
      title: "Bird's Eye View", 
      url: pothole.bev_image_path ? getImageProxyUrl(pothole.bev_image_path) : null 
    },
  ];

  const handleImageClick = (index: number) => {
    setLightboxInitialIndex(index);
    setLightboxOpen(true);
  };
  
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
                  {pothole.severity_score != null ? pothole.severity_score : 'N/A'}
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
      <Card sx={{ p: 2, mb: 3 }}>
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

      {/* Pothole Images Card - at the bottom */}
      <Card sx={{ p: 2 }}>
        <CardContent sx={{ p: '0 !important' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <ImageIcon style={{ width: 20, height: 20, color: '#6b7280' }} />
            <Typography variant="h3" component="h3">Pothole Images</Typography>
            <Typography sx={{ color: 'grey.400', fontSize: '0.75rem', ml: 'auto' }}>
              Click to expand
            </Typography>
          </Box>
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 2 }}>
            <ImageCard 
              title="Raw Image" 
              s3Path={pothole.raw_image_path} 
              onClick={() => handleImageClick(0)}
            />
            <ImageCard 
              title="Bird's Eye View" 
              s3Path={pothole.bev_image_path} 
              onClick={() => handleImageClick(1)}
            />
          </Box>
        </CardContent>
      </Card>

      {/* Image Lightbox Modal */}
      <ImageLightbox
        open={lightboxOpen}
        onClose={() => setLightboxOpen(false)}
        images={lightboxImages}
        initialIndex={lightboxInitialIndex}
      />
    </Box>
  );
}
