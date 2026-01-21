import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { supabase } from '@/lib/supabase';
import { Button } from '@/components/ui/button';
import { MailCheck, RefreshCw, LogOut } from 'lucide-react';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';
import { AuthTitleBar } from '@/components/layout/AuthTitleBar';

export default function VerifyEmail() {
  const navigate = useNavigate();
  const location = useLocation() as { state?: { email?: string } };
  const { user, logout } = useAuth();
  const [isResending, setIsResending] = useState(false);
  const logoUrl = `${import.meta.env.BASE_URL}courtvision.png`;
  const [isChecking, setIsChecking] = useState(false);

  const email = location.state?.email || user?.email;

  useEffect(() => {
    if (!user) return;

    const checkEmailVerification = async () => {
      setIsChecking(true);
      try {
        const { data, error } = await supabase.auth.refreshSession();

        if (error) {
          logger.warn('Failed to refresh session for email verification check', { error });
          return;
        }

        if (data.session?.user?.email_confirmed_at) {
          toast.success('Email verified! Redirecting to dashboard...');
          navigate('/dashboard', { replace: true });
        }
      } catch (error) {
        logger.error('Error checking email verification', error as Error);
      } finally {
        setIsChecking(false);
      }
    };

    checkEmailVerification();
    const interval = setInterval(checkEmailVerification, 5000);

    return () => clearInterval(interval);
  }, [user, navigate]);

  const handleResendEmail = async () => {
    if (!email) {
      toast.error('Email address not found. Please try logging in again.');
      return;
    }

    setIsResending(true);
    try {
      const { error } = await supabase.auth.resend({
        type: 'signup',
        email: email,
      });

      if (error) throw error;

      toast.success('Verification email sent! Check your inbox.');
      logger.info('Verification email resent', { email: email.substring(0, 3) + '***' });
    } catch (error) {
      logger.error('Failed to resend verification email', error as Error);
      toast.error('Failed to resend email. Please try again.');
    } finally {
      setIsResending(false);
    }
  };

  const handleLogout = async () => {
    try {
      await logout();
      toast.success('Logged out successfully');
      navigate('/login', { replace: true });
    } catch (error) {
      logger.error('Logout failed', error as Error);
      toast.error('Failed to log out');
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <AuthTitleBar />
      <div className="flex h-screen items-center justify-center pt-10 px-4">
        <div className="w-full max-w-sm rounded-lg border border-border bg-card px-6 py-10 shadow-lg text-center">
          {/* Logo */}
          <div className="flex flex-col items-center gap-2 mb-6">
            <div className="flex items-center gap-2">
              <img
                src={logoUrl}
                alt="CourtVision"
                className="h-10 w-10"
              />
              <span className="text-xl font-semibold text-foreground">CourtVision</span>
            </div>
          </div>

          {/* Icon */}
          <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-primary/10 mb-4">
            <MailCheck className="h-7 w-7 text-primary" />
          </div>

          {/* Title */}
          <h2 className="text-xl font-semibold text-foreground mb-3">
            {user ? 'Verify your email' : 'Check your email'}
          </h2>

          {/* Description */}
          <div className="space-y-3 mb-6">
            <p className="text-sm text-muted-foreground">
              {user ? (
                <>
                  Please verify your email address to access CourtVision.
                  {email && (
                    <>
                      {' '}We've sent a verification link to{' '}
                      <span className="font-medium text-foreground">{email}</span>.
                    </>
                  )}
                </>
              ) : (
                <>
                  We've sent a verification link
                  {email && (
                    <>
                      {' '}to <span className="font-medium text-foreground">{email}</span>
                    </>
                  )}
                  . Please confirm your email address before signing in.
                </>
              )}
            </p>

            {user && isChecking && (
              <div className="flex items-center justify-center gap-2 text-xs text-muted-foreground">
                <RefreshCw className="h-3 w-3 animate-spin" />
                <span>Checking verification status...</span>
              </div>
            )}

            <p className="text-xs text-muted-foreground">
              Click the link in your email to verify, then you'll be automatically redirected.
            </p>
          </div>

          {/* Actions */}
          <div className="space-y-2">
            {user ? (
              <>
                <Button
                  className="w-full h-10"
                  variant="outline"
                  onClick={handleResendEmail}
                  disabled={isResending}
                >
                  {isResending ? (
                    <>
                      <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      Sending...
                    </>
                  ) : (
                    'Resend verification email'
                  )}
                </Button>

                <Button
                  className="w-full h-10"
                  variant="ghost"
                  onClick={handleLogout}
                >
                  <LogOut className="mr-2 h-4 w-4" />
                  Log out
                </Button>
              </>
            ) : (
              <Button
                className="w-full h-10"
                onClick={() => navigate('/login', { replace: true })}
              >
                Back to sign in
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
