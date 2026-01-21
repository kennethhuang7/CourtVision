import { useState, useEffect, useRef } from 'react';
import { Link, Navigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Checkbox } from '@/components/ui/checkbox';
import { Label } from '@/components/ui/label';
import { Eye, EyeOff } from 'lucide-react';
import { toast } from 'sonner';
import { AuthTitleBar } from '@/components/layout/AuthTitleBar';
import { SupabaseConnectionStatus } from '@/components/ui/supabase-connection-status';
import { supabase } from '@/lib/supabase';
import { logger } from '@/lib/logger';

export default function Login() {
  const { login, isAuthenticated } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [rememberMe, setRememberMe] = useState(false);
  const [error, setError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const oauthCleanupRef = useRef<(() => void) | null>(null);
  const logoUrl = `${import.meta.env.BASE_URL}courtvision.png`;

  // Listen for OAuth callbacks from Electron main process
  useEffect(() => {
    const setupElectronOAuthListener = async () => {
      if (!window.electron?.onOAuthCallback) return;

      try {
        // Set up listener for OAuth callback from main process
        // This works in both dev and production Electron builds
        oauthCleanupRef.current = window.electron.onOAuthCallback(async (response: any) => {
          try {
            logger.debug('Received OAuth callback in Login');

            // Check for error response
            if (response.error) {
              logger.error('OAuth callback error', new Error(response.errorDescription || response.error));
              if (response.error === 'access_denied') {
                setError('Sign in was cancelled.');
              } else {
                setError(response.errorDescription || 'Authentication failed. Please try again.');
              }
              return;
            }

            // Set the session with the received tokens
            const { data, error: sessionError } = await supabase.auth.setSession({
              access_token: response.access_token,
              refresh_token: response.refresh_token || '',
            });

            if (sessionError) {
              logger.error('Failed to set session from OAuth callback', sessionError);
              setError('Authentication failed. Please try again.');
              return;
            }

            if (data.session) {
              toast.success('Welcome back!');
              // Navigation will happen automatically via isAuthenticated check
            }
          } catch (err) {
            logger.error('Error processing OAuth callback', err as Error);
            setError('Authentication failed. Please try again.');
          }
        });
      } catch (err) {
        logger.error('Error setting up Electron OAuth listener', err as Error);
      }
    };

    setupElectronOAuthListener();

    return () => {
      if (oauthCleanupRef.current) {
        oauthCleanupRef.current();
      }
    };
  }, []);

  // Check for OAuth errors in URL params
  useEffect(() => {
    const errorParam = searchParams.get('error');
    const errorDescription = searchParams.get('error_description');

    if (errorParam) {
      // Map common OAuth errors to user-friendly messages
      let message = 'Sign in failed. Please try again.';

      if (errorParam === 'access_denied') {
        message = 'Sign in was cancelled.';
      } else if (errorDescription?.includes('redirect_uri')) {
        message = 'OAuth configuration error. Please try again or use email/password.';
      } else if (errorDescription) {
        message = errorDescription.replace(/_/g, ' ');
      }

      setError(message);

      // Clear the error params from URL
      searchParams.delete('error');
      searchParams.delete('error_description');
      searchParams.delete('error_code');
      setSearchParams(searchParams, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  if (isAuthenticated) {
    return <Navigate to="/dashboard" replace />;
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!email || !password) {
      setError('Please fill in all fields');
      return;
    }

    try {
      setIsSubmitting(true);
      await login(email, password, rememberMe);
      toast.success('Welcome back!');
    } catch (err: any) {
      const errorMessage = err?.message || String(err);
      if (
        errorMessage.includes('timeout') ||
        errorMessage.includes('522') ||
        errorMessage.includes('Failed to fetch') ||
        errorMessage.includes('NetworkError') ||
        errorMessage.includes('Unexpected token')
      ) {
        setError('Unable to connect to the server. Please check your connection and try again.');
      } else {
        setError('Invalid email or password. Please try again.');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleOAuthLogin = async (provider: 'google' | 'discord') => {
    try {
      // Get the appropriate redirect URL (custom protocol for production Electron)
      let redirectUrl = `${window.location.origin}/dashboard`;

      if (window.electron?.getOAuthRedirectUrl) {
        try {
          const electronRedirectUrl = await window.electron.getOAuthRedirectUrl();
          if (electronRedirectUrl) {
            redirectUrl = electronRedirectUrl;
            logger.debug('Using Electron OAuth redirect URL:', redirectUrl);
          }
        } catch (err) {
          logger.error('Error getting Electron OAuth redirect URL', err as Error);
          // Fall back to web redirect URL
        }
      }

      const { error } = await supabase.auth.signInWithOAuth({
        provider,
        options: {
          redirectTo: redirectUrl,
          skipBrowserRedirect: false,
        },
      });
      if (error) throw error;
    } catch (err: any) {
      setError(`Failed to sign in with ${provider}. Please try again.`);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <AuthTitleBar />
      <div className="flex h-screen items-center justify-center pt-10 px-4">
        <div className="w-full max-w-sm rounded-lg border border-border bg-card px-6 py-10 shadow-lg">
          {/* Logo */}
          <div className="flex flex-col items-center gap-2 mb-8">
            <div className="flex items-center gap-2">
              <img
                src={logoUrl}
                alt="CourtVision"
                className="h-10 w-10"
              />
              <span className="text-xl font-semibold text-foreground">CourtVision</span>
            </div>
          </div>

          {/* Connection Status */}
          <SupabaseConnectionStatus />

          {/* Error Message */}
          {error && (
            <div className="mb-4 rounded-md bg-destructive/10 border border-destructive/20 p-3 text-sm text-destructive">
              {error}
            </div>
          )}

          {/* Form */}
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <Input
                id="email"
                type="email"
                placeholder="Email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoComplete="email"
                className="h-11"
              />
            </div>

            <div className="relative">
              <Input
                id="password"
                type={showPassword ? 'text' : 'password'}
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="current-password"
                className="h-11 pr-10"
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
              >
                {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
              </button>
            </div>

            <div className="flex items-center gap-2">
              <Checkbox
                id="remember"
                checked={rememberMe}
                onCheckedChange={(checked) => setRememberMe(checked as boolean)}
              />
              <Label htmlFor="remember" className="text-sm text-muted-foreground cursor-pointer">
                Stay signed in
              </Label>
            </div>

            <Button type="submit" className="w-full h-11" disabled={isSubmitting}>
              {isSubmitting ? (
                <div className="h-5 w-5 animate-spin rounded-full border-2 border-current border-t-transparent" />
              ) : (
                'Sign in'
              )}
            </Button>
          </form>

          {/* Divider */}
          <div className="relative my-6">
            <div className="absolute inset-0 flex items-center">
              <span className="w-full border-t border-border" />
            </div>
            <div className="relative flex justify-center text-xs uppercase">
              <span className="bg-card px-2 text-muted-foreground">or continue with</span>
            </div>
          </div>

          {/* OAuth Buttons */}
          <div className="space-y-3">
            <Button
              type="button"
              variant="outline"
              className="w-full h-11"
              onClick={() => handleOAuthLogin('google')}
            >
              <svg className="mr-2 h-5 w-5" viewBox="0 0 24 24">
                <path
                  fill="#4285F4"
                  d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"
                />
                <path
                  fill="#34A853"
                  d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"
                />
                <path
                  fill="#FBBC05"
                  d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"
                />
                <path
                  fill="#EA4335"
                  d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"
                />
              </svg>
              Sign in with Google
            </Button>

            <Button
              type="button"
              variant="outline"
              className="w-full h-11"
              onClick={() => handleOAuthLogin('discord')}
            >
              <svg className="mr-2 h-5 w-5" viewBox="0 0 24 24" fill="#5865F2">
                <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028 14.09 14.09 0 0 0 1.226-1.994.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z" />
              </svg>
              Sign in with Discord
            </Button>
          </div>

          {/* Footer Link */}
          <p className="mt-6 text-center text-sm text-muted-foreground">
            Don't have an account?{' '}
            <Link to="/register" className="font-medium text-primary hover:underline">
              Sign up
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
