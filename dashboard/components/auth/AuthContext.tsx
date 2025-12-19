'use client';

import { createContext, useContext, useState, useEffect, ReactNode } from 'react';

interface AuthContextType {
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (username: string, password: string) => Promise<boolean>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | null>(null);

// Hardcoded credentials for demo
const VALID_CREDENTIALS = {
  username: 'admin',
  password: 'admin123',
};

// Simple JWT-like token generation (for demo purposes)
function generateToken(): string {
  const payload = {
    sub: VALID_CREDENTIALS.username,
    iat: Date.now(),
    exp: Date.now() + 24 * 60 * 60 * 1000, // 24 hours
  };
  return btoa(JSON.stringify(payload));
}

function isTokenValid(token: string | null): boolean {
  if (!token) return false;
  try {
    const payload = JSON.parse(atob(token));
    return payload.exp > Date.now();
  } catch {
    return false;
  }
}

const TOKEN_KEY = 'pothole_dashboard_token';

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // Check for existing valid token on mount
    const token = localStorage.getItem(TOKEN_KEY);
    if (isTokenValid(token)) {
      setIsAuthenticated(true);
    } else {
      localStorage.removeItem(TOKEN_KEY);
    }
    setIsLoading(false);
  }, []);

  const login = async (username: string, password: string): Promise<boolean> => {
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 500));
    
    if (username === VALID_CREDENTIALS.username && password === VALID_CREDENTIALS.password) {
      const token = generateToken();
      localStorage.setItem(TOKEN_KEY, token);
      setIsAuthenticated(true);
      return true;
    }
    return false;
  };

  const logout = () => {
    localStorage.removeItem(TOKEN_KEY);
    setIsAuthenticated(false);
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, isLoading, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
