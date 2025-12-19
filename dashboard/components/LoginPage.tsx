'use client';

import { useState } from 'react';
import { Box, Typography, Paper, TextField, Button, Alert, CircularProgress, InputAdornment, IconButton } from '@mui/material';
import { Route, Lock, Eye, EyeOff } from 'lucide-react';
import { useAuth } from './auth/AuthContext';

export function LoginPage() {
  const { login } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);

    const success = await login(username, password);
    
    if (!success) {
      setError('Invalid username or password');
    }
    setIsLoading(false);
  };

  return (
    <Box
      sx={{
        width: '100vw',
        height: '100vh',
        bgcolor: 'grey.100',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      <Paper
        elevation={0}
        sx={{
          width: '100%',
          maxWidth: 400,
          p: 4,
          border: '1px solid',
          borderColor: 'grey.200',
          borderRadius: 2,
        }}
      >
        {/* Header */}
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', mb: 4 }}>
          <Box
            sx={{
              width: 56,
              height: 56,
              bgcolor: '#84cc16',
              borderRadius: 2,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              mb: 2,
            }}
          >
            <Route style={{ width: 32, height: 32, color: 'white' }} />
          </Box>
          <Typography variant="h1" component="h1" sx={{ fontSize: '1.5rem', textAlign: 'center' }}>
            Pothole Monitoring
          </Typography>
          <Typography sx={{ color: 'grey.500', textAlign: 'center', mt: 0.5 }}>
            Sign in to access the dashboard
          </Typography>
        </Box>

        {/* Error Alert */}
        {error && (
          <Alert severity="error" sx={{ mb: 3 }}>
            {error}
          </Alert>
        )}

        {/* Login Form */}
        <Box component="form" onSubmit={handleSubmit}>
          <Box sx={{ mb: 2.5 }}>
            <Typography sx={{ fontSize: '0.875rem', fontWeight: 500, color: 'grey.700', mb: 0.75 }}>
              Username
            </Typography>
            <TextField
              fullWidth
              size="small"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter username"
              disabled={isLoading}
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: 1.5,
                },
              }}
            />
          </Box>

          <Box sx={{ mb: 3 }}>
            <Typography sx={{ fontSize: '0.875rem', fontWeight: 500, color: 'grey.700', mb: 0.75 }}>
              Password
            </Typography>
            <TextField
              fullWidth
              size="small"
              type={showPassword ? 'text' : 'password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={isLoading}
              slotProps={{
                input: {
                  endAdornment: (
                    <InputAdornment position="end">
                      <IconButton
                        onClick={() => setShowPassword(!showPassword)}
                        edge="end"
                        size="small"
                        disabled={isLoading}
                        sx={{ color: 'grey.500' }}
                      >
                        {showPassword ? (
                          <EyeOff style={{ width: 18, height: 18 }} />
                        ) : (
                          <Eye style={{ width: 18, height: 18 }} />
                        )}
                      </IconButton>
                    </InputAdornment>
                  ),
                },
              }}
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: 1.5,
                },
              }}
            />
          </Box>

          <Button
            type="submit"
            fullWidth
            variant="contained"
            disabled={isLoading || !username || !password}
            sx={{
              py: 1.25,
              bgcolor: '#84cc16',
              borderRadius: 1.5,
              textTransform: 'none',
              fontWeight: 600,
              fontSize: '0.9375rem',
              '&:hover': {
                bgcolor: '#65a30d',
              },
              '&:disabled': {
                bgcolor: '#bef264',
              },
            }}
          >
            {isLoading ? (
              <CircularProgress size={24} sx={{ color: 'white' }} />
            ) : (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Lock style={{ width: 18, height: 18 }} />
                Sign In
              </Box>
            )}
          </Button>
        </Box>

        {/* Demo credentials hint */}
        <Box sx={{ mt: 3, pt: 3, borderTop: '1px solid', borderColor: 'grey.200' }}>
          <Typography sx={{ fontSize: '0.75rem', color: 'grey.500', textAlign: 'center' }}>
            Demo credentials: <strong>admin</strong> / <strong>admin123</strong>
          </Typography>
        </Box>
      </Paper>
    </Box>
  );
}
