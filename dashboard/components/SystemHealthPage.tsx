'use client';

import { Box, Typography, Paper, CircularProgress } from '@mui/material';
import { useEffect, useState, useCallback } from 'react';
import {
  Server,
  Database,
  HardDrive,
  Activity,
  CheckCircle,
  AlertTriangle,
  XCircle,
  Clock,
  Cpu,
  ArrowLeft,
  LogOut,
  Layers,
  Brain,
  Calculator,
  Globe,
  Timer,
  RefreshCw,
  Zap,
} from 'lucide-react';
import { useAuth } from './auth/AuthContext';

// API configuration
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080/api/v1';

// Latency data types
interface StageStats {
  count: number;
  avg_ms: number;
  min_ms: number;
  max_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
}

interface LatencyEvent {
  event_id: string;
  edge_detected_at: number;
  pothole_stored_at: number;
  total_pipeline_ms: number;
  raw_to_severity_ms: number;
  severity_to_pothole_ms: number;
}

interface MicroserviceLatency {
  name: string;
  key: string;
  latency_ms: number;
  p95_ms?: number;
  status: string;
}

interface LatencyData {
  timestamp: string;
  stages: Record<string, StageStats>;
  recent_events: LatencyEvent[];
  microservices: MicroserviceLatency[];
  total_pipeline: {
    avg_ms: number;
    p50_ms: number;
    p95_ms: number;
    p99_ms: number;
    min_ms: number;
    max_ms: number;
  };
  error?: string;
}

// Hardcoded realistic sample data for infrastructure (not pipeline latency)
const systemData = {
  kafka: {
    brokers: [
      { id: 'broker-1', ip: '10.0.1.101', status: 'active', lastHeartbeat: '2025-12-19T14:32:45Z' },
      { id: 'broker-2', ip: '10.0.1.102', status: 'active', lastHeartbeat: '2025-12-19T14:32:44Z' },
      { id: 'broker-3', ip: '10.0.1.103', status: 'active', lastHeartbeat: '2025-12-19T14:32:46Z' },
    ],
    controllers: [
      { id: 'controller-1', ip: '10.0.1.201', status: 'active', lastHeartbeat: '2025-12-19T14:32:47Z' },
    ],
  },
  storage: {
    minio: {
      status: 'active',
      lastHeartbeat: '2025-12-19T14:32:50Z',
      used: 847,
      total: 2048,
      distribution: [
        { type: 'Images', size: 512, color: '#84cc16' },
        { type: 'Metadata', size: 128, color: '#facc15' },
        { type: 'Iceberg Tables', size: 207, color: '#a3e635' },
      ],
    },
    polaris: {
      status: 'active',
      lastHeartbeat: '2025-12-19T14:32:48Z',
      catalogsManaged: 3,
      tablesTracked: 12,
      queriesPerMinute: 45,
    },
  },
};

// Status indicator component
function StatusBadge({ status }: { status: 'active' | 'healthy' | 'degraded' | 'down' }) {
  const config = {
    active: { color: '#16a34a', bg: '#f0fdf4', border: '#bbf7d0', label: 'Active', Icon: CheckCircle },
    healthy: { color: '#16a34a', bg: '#f0fdf4', border: '#bbf7d0', label: 'Healthy', Icon: CheckCircle },
    degraded: { color: '#ca8a04', bg: '#fefce8', border: '#fde047', label: 'Degraded', Icon: AlertTriangle },
    down: { color: '#dc2626', bg: '#fef2f2', border: '#fecaca', label: 'Down', Icon: XCircle },
  };

  const { color, bg, border, label, Icon } = config[status];

  return (
    <Box
      sx={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 0.5,
        px: 1.5,
        py: 0.5,
        bgcolor: bg,
        border: '1px solid',
        borderColor: border,
        borderRadius: 1,
      }}
    >
      <Icon style={{ width: 14, height: 14, color }} />
      <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color }}>{label}</Typography>
    </Box>
  );
}

// Format timestamp to relative time
function formatTime(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diff = Math.floor((now.getTime() - date.getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return date.toLocaleTimeString();
}

// Kafka Section
function KafkaSection() {
  const { brokers, controllers } = systemData.kafka;

  return (
    <Paper
      elevation={0}
      sx={{
        p: 3,
        border: '1px solid',
        borderColor: 'grey.200',
        borderRadius: 2,
        height: '100%',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 3 }}>
        <Box
          sx={{
            width: 40,
            height: 40,
            bgcolor: '#f0fdf4',
            borderRadius: 1.5,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Server style={{ width: 20, height: 20, color: '#16a34a' }} />
        </Box>
        <Box>
          <Typography sx={{ fontWeight: 600, color: 'grey.900' }}>Apache Kafka</Typography>
          <Typography sx={{ fontSize: '0.75rem', color: 'grey.500' }}>
            {brokers.length} Brokers • {controllers.length} Controller
          </Typography>
        </Box>
      </Box>

      <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color: 'grey.500', mb: 1.5, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        Brokers
      </Typography>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mb: 3 }}>
        {brokers.map((broker) => (
          <Box
            key={broker.id}
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              p: 1.5,
              bgcolor: 'grey.50',
              borderRadius: 1,
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800', minWidth: 70 }}>
                {broker.id}
              </Typography>
              <Typography sx={{ fontSize: '0.8rem', color: 'grey.500', fontFamily: 'monospace' }}>
                {broker.ip}
              </Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <Clock style={{ width: 12, height: 12, color: '#9ca3af' }} />
                <Typography sx={{ fontSize: '0.7rem', color: 'grey.500' }}>
                  {formatTime(broker.lastHeartbeat)}
                </Typography>
              </Box>
              <StatusBadge status={broker.status as 'active'} />
            </Box>
          </Box>
        ))}
      </Box>

      <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color: 'grey.500', mb: 1.5, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        Controllers
      </Typography>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        {controllers.map((controller) => (
          <Box
            key={controller.id}
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              p: 1.5,
              bgcolor: 'grey.50',
              borderRadius: 1,
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800', minWidth: 70 }}>
                {controller.id}
              </Typography>
              <Typography sx={{ fontSize: '0.8rem', color: 'grey.500', fontFamily: 'monospace' }}>
                {controller.ip}
              </Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <Clock style={{ width: 12, height: 12, color: '#9ca3af' }} />
                <Typography sx={{ fontSize: '0.7rem', color: 'grey.500' }}>
                  {formatTime(controller.lastHeartbeat)}
                </Typography>
              </Box>
              <StatusBadge status={controller.status as 'active'} />
            </Box>
          </Box>
        ))}
      </Box>
    </Paper>
  );
}

// MinIO Section
function MinIOSection() {
  const { minio } = systemData.storage;
  const usedPercent = (minio.used / minio.total) * 100;

  return (
    <Paper
      elevation={0}
      sx={{
        p: 3,
        border: '1px solid',
        borderColor: 'grey.200',
        borderRadius: 2,
        height: '100%',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box
            sx={{
              width: 40,
              height: 40,
              bgcolor: '#fefce8',
              borderRadius: 1.5,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <HardDrive style={{ width: 20, height: 20, color: '#ca8a04' }} />
          </Box>
          <Box>
            <Typography sx={{ fontWeight: 600, color: 'grey.900' }}>MinIO Storage</Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Clock style={{ width: 12, height: 12, color: '#9ca3af' }} />
              <Typography sx={{ fontSize: '0.7rem', color: 'grey.500' }}>
                {formatTime(minio.lastHeartbeat)}
              </Typography>
            </Box>
          </Box>
        </Box>
        <StatusBadge status={minio.status as 'active'} />
      </Box>

      {/* Storage Usage */}
      <Box sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
          <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color: 'grey.500', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Storage Usage
          </Typography>
          <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>
            {minio.used} GB / {minio.total} GB
          </Typography>
        </Box>
        <Box sx={{ height: 8, bgcolor: 'grey.100', borderRadius: 1, overflow: 'hidden' }}>
          <Box
            sx={{
              height: '100%',
              width: `${usedPercent}%`,
              bgcolor: usedPercent > 80 ? '#ef4444' : '#84cc16',
              borderRadius: 1,
            }}
          />
        </Box>
        <Typography sx={{ fontSize: '0.7rem', color: 'grey.500', mt: 0.5 }}>
          {usedPercent.toFixed(1)}% used
        </Typography>
      </Box>

      {/* Storage Distribution */}
      <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color: 'grey.500', mb: 1.5, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        Distribution by Type
      </Typography>
      <Box sx={{ display: 'flex', height: 24, borderRadius: 1, overflow: 'hidden', mb: 2 }}>
        {minio.distribution.map((item, index) => (
          <Box
            key={index}
            sx={{
              width: `${(item.size / minio.used) * 100}%`,
              bgcolor: item.color,
            }}
          />
        ))}
      </Box>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        {minio.distribution.map((item, index) => (
          <Box key={index} sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Box sx={{ width: 10, height: 10, bgcolor: item.color, borderRadius: 0.5 }} />
              <Typography sx={{ fontSize: '0.8rem', color: 'grey.700' }}>{item.type}</Typography>
            </Box>
            <Typography sx={{ fontSize: '0.8rem', fontWeight: 600, color: 'grey.800' }}>
              {item.size} GB
            </Typography>
          </Box>
        ))}
      </Box>
    </Paper>
  );
}

// Polaris Section
function PolarisSection() {
  const { polaris } = systemData.storage;

  return (
    <Paper
      elevation={0}
      sx={{
        p: 3,
        border: '1px solid',
        borderColor: 'grey.200',
        borderRadius: 2,
        height: '100%',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box
            sx={{
              width: 40,
              height: 40,
              bgcolor: '#f0fdf4',
              borderRadius: 1.5,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <Database style={{ width: 20, height: 20, color: '#16a34a' }} />
          </Box>
          <Box>
            <Typography sx={{ fontWeight: 600, color: 'grey.900' }}>Polaris Catalog</Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Clock style={{ width: 12, height: 12, color: '#9ca3af' }} />
              <Typography sx={{ fontSize: '0.7rem', color: 'grey.500' }}>
                {formatTime(polaris.lastHeartbeat)}
              </Typography>
            </Box>
          </Box>
        </Box>
        <StatusBadge status={polaris.status as 'active'} />
      </Box>

      <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 2 }}>
        <Box sx={{ textAlign: 'center', p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
          <Typography sx={{ fontSize: '1.5rem', fontWeight: 700, color: '#16a34a' }}>
            {polaris.catalogsManaged}
          </Typography>
          <Typography sx={{ fontSize: '0.7rem', color: 'grey.500', fontWeight: 500 }}>Catalogs</Typography>
        </Box>
        <Box sx={{ textAlign: 'center', p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
          <Typography sx={{ fontSize: '1.5rem', fontWeight: 700, color: '#ca8a04' }}>
            {polaris.tablesTracked}
          </Typography>
          <Typography sx={{ fontSize: '0.7rem', color: 'grey.500', fontWeight: 500 }}>Tables</Typography>
        </Box>
        <Box sx={{ textAlign: 'center', p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
          <Typography sx={{ fontSize: '1.5rem', fontWeight: 700, color: '#84cc16' }}>
            {polaris.queriesPerMinute}
          </Typography>
          <Typography sx={{ fontSize: '0.7rem', color: 'grey.500', fontWeight: 500 }}>Queries/min</Typography>
        </Box>
      </Box>
    </Paper>
  );
}

// Microservices Section
function MicroservicesSection({ latencyData, loading }: { latencyData: LatencyData | null; loading: boolean }) {
  // Map latency API data to display format with icons
  const iconMap: Record<string, React.ComponentType<{ style?: React.CSSProperties }>> = {
    edge_to_kafka: Zap,
    kafka_to_storage: HardDrive,
    depth_estimation: Brain,
    enrichment: Calculator,
  };

  const services = latencyData?.microservices?.map((svc) => ({
    ...svc,
    icon: iconMap[svc.key] || Globe,
    uptime: '99.9%', // Placeholder - would need additional monitoring
    lastCheck: new Date().toISOString(),
  })) || [];

  // Fall back to defaults if no data
  const displayServices = services.length > 0 ? services : [
    { name: 'Edge to Kafka', key: 'edge_to_kafka', icon: Zap, status: 'unknown', latency_ms: 0, uptime: '-', lastCheck: new Date().toISOString() },
    { name: 'Kafka to Storage', key: 'kafka_to_storage', icon: HardDrive, status: 'unknown', latency_ms: 0, uptime: '-', lastCheck: new Date().toISOString() },
    { name: 'Depth Estimation', key: 'depth_estimation', icon: Brain, status: 'unknown', latency_ms: 0, uptime: '-', lastCheck: new Date().toISOString() },
    { name: 'Final Enrichment', key: 'enrichment', icon: Calculator, status: 'unknown', latency_ms: 0, uptime: '-', lastCheck: new Date().toISOString() },
  ];

  const healthyCount = displayServices.filter((s) => s.status === 'healthy').length;

  return (
    <Paper
      elevation={0}
      sx={{
        p: 3,
        border: '1px solid',
        borderColor: 'grey.200',
        borderRadius: 2,
        height: '100%',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 3 }}>
        <Box
          sx={{
            width: 40,
            height: 40,
            bgcolor: '#f0fdf4',
            borderRadius: 1.5,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Cpu style={{ width: 20, height: 20, color: '#16a34a' }} />
        </Box>
        <Box sx={{ flex: 1 }}>
          <Typography sx={{ fontWeight: 600, color: 'grey.900' }}>Pipeline Microservices</Typography>
          <Typography sx={{ fontSize: '0.75rem', color: 'grey.500' }}>
            {healthyCount}/{displayServices.length} Healthy • Real-time latency metrics
          </Typography>
        </Box>
        {loading && <CircularProgress size={20} />}
      </Box>

      <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 2 }}>
        {displayServices.map((service, index) => {
          const Icon = service.icon;
          const statusColor = service.status === 'healthy' ? 'healthy' : service.status === 'degraded' ? 'degraded' : 'down';
          return (
            <Box
              key={index}
              sx={{
                p: 2,
                bgcolor: 'grey.50',
                borderRadius: 1.5,
                border: '1px solid',
                borderColor: service.status === 'degraded' ? '#fde047' : 'transparent',
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', mb: 2 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                  <Box
                    sx={{
                      width: 32,
                      height: 32,
                      bgcolor: 'white',
                      borderRadius: 1,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      border: '1px solid',
                      borderColor: 'grey.200',
                    }}
                  >
                    <Icon style={{ width: 16, height: 16, color: '#6b7280' }} />
                  </Box>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>
                    {service.name}
                  </Typography>
                </Box>
                <StatusBadge status={statusColor as 'healthy' | 'degraded' | 'down'} />
              </Box>

              <Box sx={{ display: 'flex', gap: 3 }}>
                <Box>
                  <Typography sx={{ fontSize: '0.65rem', color: 'grey.500', textTransform: 'uppercase' }}>Avg Latency</Typography>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>
                    {service.latency_ms > 0 ? `${Math.round(service.latency_ms)}ms` : '-'}
                  </Typography>
                </Box>
                <Box>
                  <Typography sx={{ fontSize: '0.65rem', color: 'grey.500', textTransform: 'uppercase' }}>P95</Typography>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>
                    {service.p95_ms && service.p95_ms > 0 ? `${Math.round(service.p95_ms)}ms` : '-'}
                  </Typography>
                </Box>
                <Box>
                  <Typography sx={{ fontSize: '0.65rem', color: 'grey.500', textTransform: 'uppercase' }}>Status</Typography>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: service.status === 'healthy' ? '#16a34a' : '#ca8a04' }}>
                    {service.status === 'healthy' ? 'Active' : service.status === 'unknown' ? 'No Data' : 'Degraded'}
                  </Typography>
                </Box>
              </Box>
            </Box>
          );
        })}
      </Box>
    </Paper>
  );
}

// Pipeline Latency Overview Section
function PipelineLatencySection({ latencyData, loading, onRefresh }: { latencyData: LatencyData | null; loading: boolean; onRefresh: () => void }) {
  const totalPipeline = latencyData?.total_pipeline;
  const recentEvents = latencyData?.recent_events || [];
  const hasData = totalPipeline && totalPipeline.avg_ms > 0;

  return (
    <Paper
      elevation={0}
      sx={{
        p: 3,
        border: '1px solid',
        borderColor: 'grey.200',
        borderRadius: 2,
        height: '100%',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box
            sx={{
              width: 40,
              height: 40,
              bgcolor: '#eff6ff',
              borderRadius: 1.5,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <Timer style={{ width: 20, height: 20, color: '#2563eb' }} />
          </Box>
          <Box>
            <Typography sx={{ fontWeight: 600, color: 'grey.900' }}>End-to-End Pipeline Latency</Typography>
            <Typography sx={{ fontSize: '0.75rem', color: 'grey.500' }}>
              Edge Detection → MinIO Storage → Dashboard
            </Typography>
          </Box>
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          {loading && <CircularProgress size={16} />}
          <Box
            component="button"
            onClick={onRefresh}
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 0.5,
              px: 1.5,
              py: 0.75,
              bgcolor: 'grey.100',
              border: 'none',
              borderRadius: 1,
              cursor: 'pointer',
              fontSize: '0.75rem',
              color: 'grey.700',
              '&:hover': { bgcolor: 'grey.200' },
            }}
          >
            <RefreshCw style={{ width: 14, height: 14 }} />
            Refresh
          </Box>
        </Box>
      </Box>

      {!hasData ? (
        <Box sx={{ textAlign: 'center', py: 4 }}>
          <Typography sx={{ color: 'grey.500', fontSize: '0.875rem' }}>
            No latency data available yet. Process some events to see metrics.
          </Typography>
        </Box>
      ) : (
        <>
          {/* Main Latency Stats */}
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 2, mb: 3 }}>
            <Box sx={{ textAlign: 'center', p: 2, bgcolor: '#eff6ff', borderRadius: 1.5 }}>
              <Typography sx={{ fontSize: '1.75rem', fontWeight: 700, color: '#2563eb' }}>
                {Math.round(totalPipeline.avg_ms)}
              </Typography>
              <Typography sx={{ fontSize: '0.7rem', color: '#3b82f6', fontWeight: 500 }}>Avg (ms)</Typography>
            </Box>
            <Box sx={{ textAlign: 'center', p: 2, bgcolor: '#f0fdf4', borderRadius: 1.5 }}>
              <Typography sx={{ fontSize: '1.75rem', fontWeight: 700, color: '#16a34a' }}>
                {Math.round(totalPipeline.p50_ms)}
              </Typography>
              <Typography sx={{ fontSize: '0.7rem', color: '#22c55e', fontWeight: 500 }}>P50 (ms)</Typography>
            </Box>
            <Box sx={{ textAlign: 'center', p: 2, bgcolor: '#fefce8', borderRadius: 1.5 }}>
              <Typography sx={{ fontSize: '1.75rem', fontWeight: 700, color: '#ca8a04' }}>
                {Math.round(totalPipeline.p95_ms)}
              </Typography>
              <Typography sx={{ fontSize: '0.7rem', color: '#eab308', fontWeight: 500 }}>P95 (ms)</Typography>
            </Box>
            <Box sx={{ textAlign: 'center', p: 2, bgcolor: '#fef2f2', borderRadius: 1.5 }}>
              <Typography sx={{ fontSize: '1.75rem', fontWeight: 700, color: '#dc2626' }}>
                {Math.round(totalPipeline.p99_ms)}
              </Typography>
              <Typography sx={{ fontSize: '0.7rem', color: '#ef4444', fontWeight: 500 }}>P99 (ms)</Typography>
            </Box>
          </Box>

          {/* Min/Max Range */}
          <Box sx={{ mb: 3 }}>
            <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color: 'grey.500', mb: 1, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Latency Range
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography sx={{ fontSize: '0.875rem', color: 'grey.600' }}>
                <strong>{Math.round(totalPipeline.min_ms)}ms</strong> (min)
              </Typography>
              <Box sx={{ flex: 1, height: 8, bgcolor: 'grey.100', borderRadius: 1, position: 'relative' }}>
                <Box
                  sx={{
                    position: 'absolute',
                    left: `${Math.min((totalPipeline.avg_ms / totalPipeline.max_ms) * 100, 100)}%`,
                    top: '50%',
                    transform: 'translate(-50%, -50%)',
                    width: 12,
                    height: 12,
                    bgcolor: '#2563eb',
                    borderRadius: '50%',
                    border: '2px solid white',
                    boxShadow: '0 1px 3px rgba(0,0,0,0.2)',
                  }}
                />
              </Box>
              <Typography sx={{ fontSize: '0.875rem', color: 'grey.600' }}>
                <strong>{Math.round(totalPipeline.max_ms)}ms</strong> (max)
              </Typography>
            </Box>
          </Box>

          {/* Recent Events */}
          {recentEvents.length > 0 && (
            <>
              <Typography sx={{ fontSize: '0.75rem', fontWeight: 600, color: 'grey.500', mb: 1.5, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Recent Events ({recentEvents.length})
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, maxHeight: 120, overflowY: 'auto' }}>
                {recentEvents.slice(0, 5).map((event, index) => (
                  <Box
                    key={index}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      p: 1,
                      bgcolor: 'grey.50',
                      borderRadius: 1,
                      fontSize: '0.8rem',
                    }}
                  >
                    <Typography sx={{ fontFamily: 'monospace', color: 'grey.600', fontSize: '0.75rem' }}>
                      {event.event_id?.substring(0, 12)}...
                    </Typography>
                    <Typography sx={{ fontWeight: 600, color: event.total_pipeline_ms > 5000 ? '#dc2626' : '#16a34a' }}>
                      {event.total_pipeline_ms}ms
                    </Typography>
                  </Box>
                ))}
              </Box>
            </>
          )}
        </>
      )}
    </Paper>
  );
}

interface SystemHealthPageProps {
  onNavigateToOverview: () => void;
}

export function SystemHealthPage({ onNavigateToOverview }: SystemHealthPageProps) {
  const { logout } = useAuth();
  const [latencyData, setLatencyData] = useState<LatencyData | null>(null);
  const [latencyLoading, setLatencyLoading] = useState(false);

  // Fetch latency data
  const fetchLatencyData = useCallback(async () => {
    setLatencyLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/latency`);
      if (response.ok) {
        const data = await response.json();
        setLatencyData(data);
      }
    } catch (error) {
      console.error('Failed to fetch latency data:', error);
    } finally {
      setLatencyLoading(false);
    }
  }, []);

  // Fetch on mount and periodically
  useEffect(() => {
    fetchLatencyData();
    const interval = setInterval(fetchLatencyData, 30000); // Refresh every 30 seconds
    return () => clearInterval(interval);
  }, [fetchLatencyData]);

  return (
    <Box
      sx={{
        width: '100vw',
        height: '100vh',
        bgcolor: 'grey.50',
        overflow: 'hidden',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      {/* Header */}
      <Box
        component="header"
        sx={{
          bgcolor: 'background.paper',
          borderBottom: '1px solid',
          borderColor: 'grey.200',
          px: 4,
          py: 1.5,
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
                  width: 44,
                  height: 44,
                  bgcolor: '#84cc16',
                  borderRadius: 2,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                }}
              >
                <Activity style={{ width: 24, height: 24, color: 'white' }} />
              </Box>
              <Box>
                <Typography variant="h1" component="h1" sx={{ fontSize: '1.25rem' }}>
                  System Health Monitoring
                </Typography>
                <Typography sx={{ color: 'grey.500', fontSize: '0.8rem' }}>
                  Infrastructure & Service Status • {new Date().toLocaleDateString()}
                </Typography>
              </Box>
            </Box>
          </Box>

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

      {/* Main Content */}
      <Box
        sx={{
          flex: 1,
          p: 3,
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gridTemplateRows: 'auto auto 1fr',
          gap: 2.5,
          minHeight: 0,
          overflow: 'auto',
        }}
      >
        {/* Row 1: Pipeline Latency (full width) */}
        <Box sx={{ gridColumn: 'span 2' }}>
          <PipelineLatencySection latencyData={latencyData} loading={latencyLoading} onRefresh={fetchLatencyData} />
        </Box>
        
        {/* Row 2: Kafka and Storage */}
        <KafkaSection />
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
          <MinIOSection />
          <PolarisSection />
        </Box>
        
        {/* Row 3: Microservices (full width) */}
        <Box sx={{ gridColumn: 'span 2' }}>
          <MicroservicesSection latencyData={latencyData} loading={latencyLoading} />
        </Box>
      </Box>
    </Box>
  );
}
