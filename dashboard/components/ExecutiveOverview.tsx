'use client';

import { Box, Typography, Paper } from '@mui/material';
import {
  TrendingUp,
  TrendingDown,
  AlertTriangle,
  CheckCircle,
  Plus,
  Minus,
  Activity,
  Route,
  LogOut,
} from 'lucide-react';
import { useAuth } from './auth/AuthContext';

// Hardcoded realistic sample data
const dashboardData = {
  totalActivePotholes: 147,
  newToday: 12,
  newThisWeek: 43,
  resolvedToday: 8,
  resolvedThisWeek: 31,
  netChange: 12, // positive = increase
  averageSeverity: 5.8,
  roadsAffectedPercent: 3.2,
  // 30-day active potholes over time (cumulative count)
  activePotholesHistory: [
    112, 115, 118, 121, 119, 122, 125, 128, 126, 129,
    132, 135, 133, 136, 139, 141, 138, 140, 143, 146,
    144, 147, 145, 148, 150, 149, 152, 150, 148, 147
  ],
  // 30-day severity trend
  severityTrend: [
    5.2, 5.4, 5.3, 5.6, 5.5, 5.7, 5.4, 5.8, 5.6, 5.5,
    5.7, 5.9, 5.6, 5.4, 5.8, 5.7, 5.9, 5.6, 5.8, 5.7,
    5.5, 5.8, 5.6, 5.9, 5.7, 5.8, 5.6, 5.9, 5.7, 5.8
  ],
  // Severity distribution
  severityDistribution: {
    critical: 23,
    moderate: 68,
    minor: 56,
  },
};

// Line chart component for active potholes over time
function ActivePotholesChart({ data }: { data: number[] }) {
  const width = 600;
  const height = 240;
  const padding = { top: 24, right: 24, bottom: 36, left: 50 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  
  // Generate path points
  const points = data.map((value, index) => {
    const x = padding.left + (index / (data.length - 1)) * chartWidth;
    const y = padding.top + chartHeight - ((value - min) / range) * chartHeight;
    return { x, y, value };
  });
  
  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
  
  // Area fill path
  const areaD = `${pathD} L ${points[points.length - 1].x} ${padding.top + chartHeight} L ${padding.left} ${padding.top + chartHeight} Z`;
  
  // Y-axis labels
  const yLabels = [min, min + range * 0.5, max].map(v => Math.round(v));
  
  // X-axis labels (every 7 days)
  const xLabels = ['30d ago', '20d ago', '10d ago', 'Today'];

  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      {/* Grid lines */}
      {[0, 0.5, 1].map((ratio, i) => (
        <line
          key={i}
          x1={padding.left}
          y1={padding.top + chartHeight * (1 - ratio)}
          x2={width - padding.right}
          y2={padding.top + chartHeight * (1 - ratio)}
          stroke="#e5e7eb"
          strokeWidth="1"
        />
      ))}
      
      {/* Area fill */}
      <path d={areaD} fill="url(#areaGradient)" />
      
      {/* Gradient definition */}
      <defs>
        <linearGradient id="areaGradient" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#84cc16" stopOpacity="0.3" />
          <stop offset="100%" stopColor="#84cc16" stopOpacity="0.05" />
        </linearGradient>
      </defs>
      
      {/* Line */}
      <path d={pathD} fill="none" stroke="#84cc16" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      
      {/* End point dot */}
      <circle cx={points[points.length - 1].x} cy={points[points.length - 1].y} r="5" fill="#84cc16" />
      <circle cx={points[points.length - 1].x} cy={points[points.length - 1].y} r="8" fill="#84cc16" fillOpacity="0.3" />
      
      {/* Y-axis labels */}
      {yLabels.map((label, i) => (
        <text
          key={i}
          x={padding.left - 8}
          y={padding.top + chartHeight * (1 - i * 0.5) + 4}
          textAnchor="end"
          fontSize="11"
          fill="#6b7280"
        >
          {label}
        </text>
      ))}
      
      {/* X-axis labels */}
      {xLabels.map((label, i) => (
        <text
          key={i}
          x={padding.left + (i / (xLabels.length - 1)) * chartWidth}
          y={height - 8}
          textAnchor="middle"
          fontSize="11"
          fill="#6b7280"
        >
          {label}
        </text>
      ))}
    </svg>
  );
}

// Pie chart component for severity distribution
function SeverityPieChart() {
  const data = [
    { label: 'Critical', value: dashboardData.severityDistribution.critical, color: '#ef4444' },
    { label: 'Moderate', value: dashboardData.severityDistribution.moderate, color: '#f97316' },
    { label: 'Minor', value: dashboardData.severityDistribution.minor, color: '#eab308' },
  ];
  
  const total = data.reduce((sum, d) => sum + d.value, 0);
  const size = 200;
  const center = size / 2;
  const radius = 80;
  const innerRadius = 48;
  
  let currentAngle = -90; // Start from top
  
  const slices = data.map((item) => {
    const percentage = item.value / total;
    const angle = percentage * 360;
    const startAngle = currentAngle;
    const endAngle = currentAngle + angle;
    currentAngle = endAngle;
    
    const startRad = (startAngle * Math.PI) / 180;
    const endRad = (endAngle * Math.PI) / 180;
    
    const x1 = center + radius * Math.cos(startRad);
    const y1 = center + radius * Math.sin(startRad);
    const x2 = center + radius * Math.cos(endRad);
    const y2 = center + radius * Math.sin(endRad);
    
    const ix1 = center + innerRadius * Math.cos(startRad);
    const iy1 = center + innerRadius * Math.sin(startRad);
    const ix2 = center + innerRadius * Math.cos(endRad);
    const iy2 = center + innerRadius * Math.sin(endRad);
    
    const largeArc = angle > 180 ? 1 : 0;
    
    const pathD = [
      `M ${x1} ${y1}`,
      `A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2}`,
      `L ${ix2} ${iy2}`,
      `A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${ix1} ${iy1}`,
      'Z'
    ].join(' ');
    
    return { ...item, pathD, percentage };
  });

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 3 }}>
      <Box sx={{ position: 'relative' }}>
        <svg width={size} height={size}>
          {slices.map((slice, i) => (
            <path key={i} d={slice.pathD} fill={slice.color} />
          ))}
        </svg>
        <Box
          sx={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            textAlign: 'center',
          }}
        >
          <Typography sx={{ fontSize: '1.5rem', fontWeight: 700, lineHeight: 1, color: 'grey.900' }}>
            {total}
          </Typography>
          <Typography sx={{ fontSize: '0.7rem', color: 'grey.500', mt: 0.25 }}>
            Total
          </Typography>
        </Box>
      </Box>
      
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
        {slices.map((item, i) => (
          <Box key={i} sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Box sx={{ width: 12, height: 12, bgcolor: item.color, borderRadius: '50%' }} />
            <Box>
              <Typography sx={{ fontSize: '0.875rem', fontWeight: 600, color: 'grey.800' }}>
                {item.label}
              </Typography>
              <Typography sx={{ fontSize: '0.75rem', color: 'grey.500' }}>
                {item.value} ({(item.percentage * 100).toFixed(1)}%)
              </Typography>
            </Box>
          </Box>
        ))}
      </Box>
    </Box>
  );
}

// Mini sparkline component
function Sparkline({ 
  data, 
  color = '#3b82f6',
  height = 32,
  width = 80 
}: { 
  data: number[]; 
  color?: string;
  height?: number;
  width?: number;
}) {
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  
  const points = data.map((value, index) => {
    const x = (index / (data.length - 1)) * width;
    const y = height - ((value - min) / range) * (height - 4) - 2;
    return `${x},${y}`;
  }).join(' ');
  
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      <polyline
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={points}
      />
    </svg>
  );
}

// Compact KPI Tile component
function KPITile({
  title,
  value,
  unit = '',
  trend,
  trendDirection,
  sparklineData,
  sparklineColor,
  icon: Icon,
  iconBgColor,
  iconColor,
}: {
  title: string;
  value: number | string;
  unit?: string;
  trend?: string;
  trendDirection?: 'up' | 'down' | 'neutral';
  sparklineData?: number[];
  sparklineColor?: string;
  icon: React.ElementType;
  iconBgColor: string;
  iconColor: string;
}) {
  const getTrendColor = () => {
    if (trendDirection === 'up') return '#ef4444';
    if (trendDirection === 'down') return '#22c55e';
    return '#6b7280';
  };

  const TrendIcon = trendDirection === 'up' ? TrendingUp : 
                    trendDirection === 'down' ? TrendingDown : 
                    Activity;

  return (
    <Paper
      elevation={0}
      sx={{
        p: 2.5,
        height: '100%',
        border: '1px solid',
        borderColor: 'grey.200',
        borderRadius: 2,
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', mb: 1.5 }}>
        <Box
          sx={{
            width: 40,
            height: 40,
            bgcolor: iconBgColor,
            borderRadius: 1.5,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Icon style={{ width: 20, height: 20, color: iconColor }} />
        </Box>
        {sparklineData && (
          <Sparkline data={sparklineData} color={sparklineColor || '#3b82f6'} />
        )}
      </Box>

      <Box>
        <Typography
          sx={{
            color: 'grey.500',
            fontSize: '0.75rem',
            fontWeight: 500,
            mb: 0.5,
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
          }}
        >
          {title}
        </Typography>
        <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 0.5 }}>
          <Typography sx={{ fontSize: '1.75rem', fontWeight: 700, lineHeight: 1, color: 'grey.900' }}>
            {value}
          </Typography>
          {unit && (
            <Typography sx={{ fontSize: '1rem', color: 'grey.500', fontWeight: 500 }}>
              {unit}
            </Typography>
          )}
        </Box>
        {trend && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.75 }}>
            <TrendIcon style={{ width: 14, height: 14, color: getTrendColor() }} />
            <Typography sx={{ fontSize: '0.75rem', color: getTrendColor(), fontWeight: 500 }}>
              {trend}
            </Typography>
          </Box>
        )}
      </Box>
    </Paper>
  );
}

// Net change indicator component
function NetChangeIndicator() {
  const isIncrease = dashboardData.netChange > 0;
  
  return (
    <Paper
      elevation={0}
      sx={{
        p: 2.5,
        height: '100%',
        border: '1px solid',
        borderColor: isIncrease ? '#fecaca' : '#bbf7d0',
        borderRadius: 2,
        bgcolor: isIncrease ? '#fef2f2' : '#f0fdf4',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
      }}
    >
      <Typography
        sx={{
          color: 'grey.500',
          fontSize: '0.75rem',
          fontWeight: 500,
          mb: 1,
          textTransform: 'uppercase',
          letterSpacing: '0.05em',
        }}
      >
        Net Change (Week)
      </Typography>
      
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
        <Box
          sx={{
            width: 48,
            height: 48,
            bgcolor: isIncrease ? '#ef4444' : '#22c55e',
            borderRadius: 1.5,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          {isIncrease ? (
            <Plus style={{ width: 24, height: 24, color: 'white' }} />
          ) : (
            <Minus style={{ width: 24, height: 24, color: 'white' }} />
          )}
        </Box>
        <Box>
          <Typography
            sx={{
              fontSize: '2rem',
              fontWeight: 700,
              lineHeight: 1,
              color: isIncrease ? '#dc2626' : '#16a34a',
            }}
          >
            {isIncrease ? '+' : ''}{dashboardData.netChange}
          </Typography>
          <Typography sx={{ fontSize: '0.7rem', color: 'grey.600', mt: 0.25 }}>
            {isIncrease ? 'More than resolved' : 'More resolved'}
          </Typography>
        </Box>
      </Box>
    </Paper>
  );
}

interface ExecutiveOverviewProps {
  onNavigateToMap: () => void;
  onNavigateToHealth: () => void;
}

export function ExecutiveOverview({ onNavigateToMap, onNavigateToHealth }: ExecutiveOverviewProps) {
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
              <Route style={{ width: 24, height: 24, color: 'white' }} />
            </Box>
            <Box>
              <Typography variant="h1" component="h1" sx={{ fontSize: '1.25rem' }}>
                Road Condition Executive Summary
              </Typography>
              <Typography sx={{ color: 'grey.500', fontSize: '0.8rem' }}>
                Ho Chi Minh City • {new Date().toLocaleDateString()}
              </Typography>
            </Box>
          </Box>
          
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Box
              component="button"
              onClick={onNavigateToMap}
              sx={{
                px: 3,
                py: 1,
                bgcolor: '#84cc16',
                color: 'white',
                border: 'none',
                borderRadius: 1.5,
                fontWeight: 600,
                fontSize: '0.875rem',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                '&:hover': {
                  bgcolor: '#65a30d',
                },
              }}
            >
              Map View →
            </Box>
            
            <Box
              component="button"
              onClick={onNavigateToHealth}
              sx={{
                px: 3,
                py: 1,
                bgcolor: '#facc15',
                color: '#1f2937',
                border: 'none',
                borderRadius: 1.5,
                fontWeight: 600,
                fontSize: '0.875rem',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                '&:hover': {
                  bgcolor: '#eab308',
                },
              }}
            >
              <Activity style={{ width: 16, height: 16 }} />
              System Health
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
      </Box>

      {/* Main Content */}
      <Box
        sx={{
          flex: 1,
          p: 3,
          display: 'grid',
          gridTemplateColumns: '1fr 1fr 1fr 1fr 1fr',
          gridTemplateRows: 'auto 1fr',
          gap: 2.5,
          minHeight: 0,
        }}
      >
        {/* Row 1: KPI Cards */}
        <KPITile
          title="Total Active"
          value={dashboardData.totalActivePotholes}
          icon={AlertTriangle}
          iconBgColor="#fef2f2"
          iconColor="#ef4444"
        />
        
        <KPITile
          title="New Today"
          value={dashboardData.newToday}
          trend="+3 vs yesterday"
          trendDirection="up"
          icon={Plus}
          iconBgColor="#fff7ed"
          iconColor="#f97316"
        />
        
        <KPITile
          title="Resolved Today"
          value={dashboardData.resolvedToday}
          trend="+2 vs yesterday"
          trendDirection="down"
          icon={CheckCircle}
          iconBgColor="#f0fdf4"
          iconColor="#22c55e"
        />
        
        <KPITile
          title="Avg Severity"
          value={dashboardData.averageSeverity.toFixed(1)}
          unit="/ 10"
          sparklineData={dashboardData.severityTrend}
          sparklineColor="#f97316"
          icon={Activity}
          iconBgColor="#fff7ed"
          iconColor="#f97316"
        />
        
        <KPITile
          title="Roads Affected"
          value={dashboardData.roadsAffectedPercent}
          unit="%"
          trend="+0.2% this week"
          trendDirection="up"
          icon={Route}
          iconBgColor="#f5f5f5"
          iconColor="#6b7280"
        />

        {/* Row 2: Charts and Weekly Stats */}
        {/* Active Potholes Chart - spans 3 columns */}
        <Paper
          elevation={0}
          sx={{
            gridColumn: 'span 3',
            p: 3,
            border: '1px solid',
            borderColor: 'grey.200',
            borderRadius: 2,
            display: 'flex',
            flexDirection: 'column',
          }}
        >
          <Typography
            sx={{
              color: 'grey.500',
              fontSize: '0.75rem',
              fontWeight: 500,
              mb: 2,
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
            }}
          >
            Active Potholes Over Time (30 Days)
          </Typography>
          <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <ActivePotholesChart data={dashboardData.activePotholesHistory} />
          </Box>
        </Paper>

        {/* Right side panel - spans 2 columns */}
        <Box sx={{ gridColumn: 'span 2', display: 'flex', flexDirection: 'column', gap: 2.5 }}>
          {/* Severity Distribution Pie Chart */}
          <Paper
            elevation={0}
            sx={{
              flex: 1,
              p: 3,
              border: '1px solid',
              borderColor: 'grey.200',
              borderRadius: 2,
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            <Typography
              sx={{
                color: 'grey.500',
                fontSize: '0.75rem',
                fontWeight: 500,
                mb: 2,
                textTransform: 'uppercase',
                letterSpacing: '0.05em',
              }}
            >
              Severity Distribution
            </Typography>
            <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <SeverityPieChart />
            </Box>
          </Paper>

          {/* Weekly Summary Row */}
          <Box sx={{ display: 'flex', gap: 2.5 }}>
            <KPITile
              title="New This Week"
              value={dashboardData.newThisWeek}
              trend="+8 vs last week"
              trendDirection="up"
              icon={Plus}
              iconBgColor="#fefce8"
              iconColor="#eab308"
            />
            <NetChangeIndicator />
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
