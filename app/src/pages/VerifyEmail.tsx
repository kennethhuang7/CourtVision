import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { supabase } from '@/lib/supabase';
import { Button } from '@/components/ui/button';
import { MailCheck, RefreshCw, LogOut } from 'lucide-react';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

export default function VerifyEmail() {
  const navigate = useNavigate();
  const location = useLocation() as { state?: { email?: string } };
  const { user, logout } = useAuth();
  const [isResending, setIsResending] = useState(false);
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
    <div className="min-h-screen flex items-center justify-center bg-background">
      <div className="w-full max-w-md space-y-8 text-center animate-fade-in px-4">
        <div className="flex justify-center gap-3 items-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl overflow-hidden">
            <img src="/courtvision.png" alt="CourtVision" className="h-full w-full object-contain" />
          </div>
          <h1 className="text-2xl font-bold text-foreground">CourtVision</h1>
        </div>

        <div className="space-y-4">
          <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-primary/10">
            <MailCheck className="h-8 w-8 text-primary" />
          </div>

          <h2 className="text-2xl font-bold text-foreground">
            {user ? 'Verify your email' : 'Check your email'}
          </h2>

          <div className="space-y-3">
            <p className="text-muted-foreground">
              {user ? (
                <>
                  Please verify your email address to access CourtVision.
                  {email && (
                    <>
                      {' '}We&apos;ve sent a verification link to{' '}
                      <span className="font-semibold text-foreground">{email}</span>.
                    </>
                  )}
                </>
              ) : (
                <>
                  We&apos;ve sent a verification link
                  {email && (
                    <>
                      {' '}to <span className="font-semibold text-foreground">{email}</span>
                    </>
                  )}
                  . Please confirm your email address before signing in.
                </>
              )}
            </p>

            {user && (
              <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground">
                {isChecking && (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    <span>Checking verification status...</span>
                  </>
                )}
              </div>
            )}

            <p className="text-xs text-muted-foreground">
              Click the link in your email to verify, then you&apos;ll be automatically redirected to the dashboard.
            </p>
          </div>
        </div>

        <div className="space-y-3">
          {user ? (
            <>
              <Button
                size="lg"
                className="w-full"
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
                size="lg"
                className="w-full"
                variant="ghost"
                onClick={handleLogout}
              >
                <LogOut className="mr-2 h-4 w-4" />
                Log out
              </Button>
            </>
          ) : (
            <Button
              size="lg"
              className="w-full"
              variant="hero"
              onClick={() => navigate('/login', { replace: true })}
            >
              Back to sign in
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}
