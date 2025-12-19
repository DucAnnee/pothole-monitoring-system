'use client';

import { useState } from "react";
import { ExecutiveOverview } from "@/components/ExecutiveOverview";
import { MapViewDashboard } from "@/components/MapViewDashboard";
import { SystemHealthPage } from "@/components/SystemHealthPage";
import { LoginPage } from "@/components/LoginPage";
import { useAuth } from "@/components/auth/AuthContext";
import { Box, CircularProgress } from "@mui/material";
import "@/app/globals.css";

type View = 'overview' | 'map' | 'health';

function LoadingScreen() {
  return (
    <Box
      sx={{
        width: '100vw',
        height: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        bgcolor: 'grey.100',
      }}
    >
      <CircularProgress sx={{ color: '#84cc16' }} />
    </Box>
  );
}

export default function Page() {
  const { isAuthenticated, isLoading } = useAuth();
  const [currentView, setCurrentView] = useState<View>('overview');

  // Show loading while checking auth state
  if (isLoading) {
    return <LoadingScreen />;
  }

  // Redirect to login if not authenticated
  if (!isAuthenticated) {
    return <LoginPage />;
  }

  // Authenticated - show dashboard
  if (currentView === 'overview') {
    return (
      <ExecutiveOverview
        onNavigateToMap={() => setCurrentView('map')}
        onNavigateToHealth={() => setCurrentView('health')}
      />
    );
  }

  if (currentView === 'map') {
    return <MapViewDashboard onNavigateToOverview={() => setCurrentView('overview')} />;
  }

  return <SystemHealthPage onNavigateToOverview={() => setCurrentView('overview')} />;
}
