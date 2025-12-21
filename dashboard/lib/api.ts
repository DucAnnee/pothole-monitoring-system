/**
 * API service layer for communicating with the dashboard backend
 */

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3002/api/v1';

/**
 * API response types based on backend controllers
 */

// Summary API types
export interface SummaryResponse {
  activePotholes: {
    count: number;
    trend: {
      today: {
        count: number;
        comparison: string;
      };
      thisWeek: {
        count: number;
        comparison: string;
      };
    };
  };
  averageSeverity: number;
  activePotholesLast30Days: Array<{
    date: string;
    count: number;
  }>;
  severityDistribution: {
    MINOR: number;
    MODERATE: number;
    HIGH: number;
    CRITICAL: number;
  };
  statusChanges: {
    reportedToInProgress: {
      thisWeek: number;
      comparison: string;
    };
    inProgressToFixed: {
      thisWeek: number;
      comparison: string;
    };
  };
}

// Map view API types
export type SeverityLevel = 'MINOR' | 'MODERATE' | 'HIGH' | 'CRITICAL';
export type PotholeStatus = 'reported' | 'in_progress' | 'fixed';

export interface MapPothole {
  pothole_id: string;
  gps_lat: number;
  gps_lon: number;
  severity_level: SeverityLevel;
  status: PotholeStatus;
}

export interface MapViewResponse {
  potholes: MapPothole[];
}

// Pothole detail API types
export interface PotholeDetail {
  pothole_id: string;
  first_event_id: string;
  gps_lat: number;
  gps_lon: number;
  geom_h3: string | null;
  city: string | null;
  ward: string | null;
  district: string | null;
  street_name: string | null;
  road_id: string | null;
  depth_cm: number | null;
  surface_area_cm2: number | null;
  severity_score: number;
  severity_level: SeverityLevel;
  /** 
   * Serialized polygon mask as JSON string.
   * Format: [[x1, y1], [x2, y2], ...] - list of pixel coordinates.
   * Originally from edge device's original_mask field.
   */
  pothole_polygon: string | null;
  status: PotholeStatus;
  detected_at: string;
  in_progress_at: string | null;
  fixed_at: string | null;
  last_updated: string;
  observation_count: number;
}

/**
 * Generic fetch wrapper with error handling
 */
async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: 'Unknown error' }));
    throw new Error(error.message || `API error: ${response.status}`);
  }

  return response.json();
}

/**
 * Get dashboard summary metrics
 */
export async function getSummary(): Promise<SummaryResponse> {
  return fetchAPI<SummaryResponse>('/summary');
}

/**
 * Get potholes for map view
 * If no coordinates provided, returns top 30 most recent active potholes
 */
export async function getMapView(lat?: number, lon?: number): Promise<MapViewResponse> {
  const params = new URLSearchParams();
  if (lat !== undefined && lon !== undefined) {
    params.set('lat', lat.toString());
    params.set('lon', lon.toString());
  }
  const query = params.toString();
  return fetchAPI<MapViewResponse>(`/map-view${query ? `?${query}` : ''}`);
}

/**
 * Get detailed information for a specific pothole
 */
export async function getPotholeDetail(potholeId: string): Promise<PotholeDetail> {
  return fetchAPI<PotholeDetail>(`/pothole/${encodeURIComponent(potholeId)}`);
}

/**
 * Severity level color mapping
 */
export const SEVERITY_COLORS: Record<SeverityLevel, { primary: string; secondary: string; bg: string }> = {
  CRITICAL: { primary: '#ef4444', secondary: '#dc2626', bg: '#fef2f2' },
  HIGH: { primary: '#f97316', secondary: '#ea580c', bg: '#fff7ed' },
  MODERATE: { primary: '#eab308', secondary: '#ca8a04', bg: '#fefce8' },
  MINOR: { primary: '#22c55e', secondary: '#16a34a', bg: '#f0fdf4' },
};

/**
 * Get marker size based on severity level
 */
export function getMarkerSize(severityLevel: SeverityLevel): number {
  switch (severityLevel) {
    case 'CRITICAL': return 16;
    case 'HIGH': return 14;
    case 'MODERATE': return 12;
    case 'MINOR': return 10;
    default: return 10;
  }
}
