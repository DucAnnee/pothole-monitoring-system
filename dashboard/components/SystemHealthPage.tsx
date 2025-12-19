'use client';

import { Box, Typography, Paper } from '@mui/material';
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
} from 'lucide-react';
import { useAuth } from './auth/AuthContext';

// Hardcoded realistic sample data
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
  microservices: [
    {
      name: 'Segmentation Model',
      icon: Layers,
      status: 'healthy',
      uptime: '99.8%',
      latency: '120ms',
      lastCheck: '2025-12-19T14:32:52Z',
    },
    {
      name: 'Depth Estimation Model',
      icon: Brain,
      status: 'healthy',
      uptime: '99.5%',
      latency: '185ms',
      lastCheck: '2025-12-19T14:32:51Z',
    },
    {
      name: 'Severity Calculation',
      icon: Calculator,
      status: 'degraded',
      uptime: '97.2%',
      latency: '340ms',
      lastCheck: '2025-12-19T14:32:50Z',
    },
    {
      name: 'Website Backend',
      icon: Globe,
      status: 'healthy',
      uptime: '99.9%',
      latency: '45ms',
      lastCheck: '2025-12-19T14:32:53Z',
    },
  ],
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
function MicroservicesSection() {
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
        <Box>
          <Typography sx={{ fontWeight: 600, color: 'grey.900' }}>Microservices</Typography>
          <Typography sx={{ fontSize: '0.75rem', color: 'grey.500' }}>
            {systemData.microservices.filter((s) => s.status === 'healthy').length}/{systemData.microservices.length} Healthy
          </Typography>
        </Box>
      </Box>

      <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 2 }}>
        {systemData.microservices.map((service, index) => {
          const Icon = service.icon;
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
                <StatusBadge status={service.status as 'healthy' | 'degraded' | 'down'} />
              </Box>

              <Box sx={{ display: 'flex', gap: 3 }}>
                <Box>
                  <Typography sx={{ fontSize: '0.65rem', color: 'grey.500', textTransform: 'uppercase' }}>Uptime</Typography>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>{service.uptime}</Typography>
                </Box>
                <Box>
                  <Typography sx={{ fontSize: '0.65rem', color: 'grey.500', textTransform: 'uppercase' }}>Latency</Typography>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>{service.latency}</Typography>
                </Box>
                <Box>
                  <Typography sx={{ fontSize: '0.65rem', color: 'grey.500', textTransform: 'uppercase' }}>Last Check</Typography>
                  <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>{formatTime(service.lastCheck)}</Typography>
                </Box>
              </Box>
            </Box>
          );
        })}
      </Box>
    </Paper>
  );
}

interface SystemHealthPageProps {
  onNavigateToOverview: () => void;
}

export function SystemHealthPage({ onNavigateToOverview }: SystemHealthPageProps) {
  const { logout } = useAuth();

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
          gridTemplateRows: '1fr 1fr',
          gap: 2.5,
          minHeight: 0,
          overflow: 'auto',
        }}
      >
        <KafkaSection />
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
          <MinIOSection />
          <PolarisSection />
        </Box>
        <Box sx={{ gridColumn: 'span 2' }}>
          <MicroservicesSection />
        </Box>
      </Box>
    </Box>
  );
}
