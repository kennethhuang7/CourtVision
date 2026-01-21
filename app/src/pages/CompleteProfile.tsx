import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Check, X } from 'lucide-react';
import { toast } from 'sonner';
import { cn } from '@/lib/utils';
import { AuthTitleBar } from '@/components/layout/AuthTitleBar';
import { supabase } from '@/lib/supabase';
import { logger } from '@/lib/logger';
import { useUserProfile } from '@/hooks/useUserProfile';
import { OAUTH_PENDING_PREFIX } from '@/hooks/useEnsureUserProfile';

export default function CompleteProfile() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const { data: userProfile, isLoading: isProfileLoading } = useUserProfile();
  const [username, setUsername] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [error, setError] = useState('');
  const logoUrl = `${import.meta.env.BASE_URL}courtvision.png`;
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isCheckingUsername, setIsCheckingUsername] = useState(false);
  const [isUsernameAvailable, setIsUsernameAvailable] = useState<boolean | null>(null);

  // Check if user needs to complete profile
  const needsCompletion = userProfile?.username?.startsWith(OAUTH_PENDING_PREFIX) ?? false;

  // Redirect if profile is already complete or user is not OAuth
  useEffect(() => {
    if (isProfileLoading || !userProfile) return;

    if (!needsCompletion) {
      navigate('/dashboard', { replace: true });
      return;
    }

    // Pre-fill display name from OAuth provider if available
    const prefillDisplayName = async () => {
      try {
        const { data: { user: authUser } } = await supabase.auth.getUser();
        const providerName = authUser?.user_metadata?.full_name ||
                            authUser?.user_metadata?.name ||
                            authUser?.user_metadata?.preferred_username || '';
        if (providerName && !displayName) {
          setDisplayName(providerName);
        }
      } catch (err) {
        logger.error('Error fetching OAuth user data', err as Error);
      }
    };

    prefillDisplayName();
  }, [userProfile, isProfileLoading, needsCompletion, navigate, displayName]);

  // Check username availability with debounce
  useEffect(() => {
    if (!username || username.length < 3) {
      setIsUsernameAvailable(null);
      return;
    }

    const timer = setTimeout(async () => {
      setIsCheckingUsername(true);
      try {
        const { data, error } = await supabase.rpc('check_username_availability', {
          p_username: username.trim(),
          p_current_user_id: user?.id || '',
        });

        if (error) throw error;
        setIsUsernameAvailable(data);
      } catch (err) {
        logger.error('Error checking username', err as Error);
        setIsUsernameAvailable(null);
      } finally {
        setIsCheckingUsername(false);
      }
    }, 500);

    return () => clearTimeout(timer);
  }, [username, user?.id]);

  const usernameValidation = {
    length: username.length >= 3 && username.length <= 30,
    format: /^[a-z0-9_]+$/.test(username),
    available: isUsernameAvailable === true,
  };

  const isValidUsername = usernameValidation.length && usernameValidation.format && usernameValidation.available;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!username || !displayName) {
      setError('Please fill in all fields');
      return;
    }

    if (!isValidUsername) {
      setError('Please choose a valid, available username');
      return;
    }

    if (displayName.length > 50) {
      setError('Display name must be 50 characters or less');
      return;
    }

    try {
      setIsSubmitting(true);

      // Update user profile
      const { error: updateError } = await supabase
        .from('user_profiles')
        .update({
          username: username.trim(),
          display_name: displayName.trim(),
          updated_at: new Date().toISOString(),
        })
        .eq('user_id', user?.id);

      if (updateError) {
        if (updateError.code === '23505') {
          setError('This username is already taken. Please choose another.');
          return;
        }
        throw updateError;
      }

      // Also update user_metadata for consistency
      await supabase.auth.updateUser({
        data: {
          username: username.trim(),
          display_name: displayName.trim(),
        },
      });

      toast.success('Profile completed! Welcome to CourtVision.');
      navigate('/dashboard', { replace: true });
    } catch (err: any) {
      logger.error('Error completing profile', err as Error);
      setError('Failed to complete profile. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  const ValidationCheck = ({ met, label, loading }: { met: boolean | null; label: string; loading?: boolean }) => (
    <div className="flex items-center gap-2 text-xs">
      {loading ? (
        <div className="h-3 w-3 animate-spin rounded-full border border-muted-foreground border-t-transparent" />
      ) : met === true ? (
        <Check className="h-3 w-3 text-green-500" />
      ) : met === false ? (
        <X className="h-3 w-3 text-destructive" />
      ) : (
        <X className="h-3 w-3 text-muted-foreground" />
      )}
      <span className={cn(
        met === true ? 'text-green-500' : met === false ? 'text-destructive' : 'text-muted-foreground'
      )}>{label}</span>
    </div>
  );

  // Show loading while checking profile
  if (isProfileLoading || !userProfile) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    );
  }

  // If profile is complete, redirect happens in useEffect
  if (!needsCompletion) {
    return null;
  }

  return (
    <div className="min-h-screen bg-background">
      <AuthTitleBar />
      <div className="flex h-screen items-center justify-center pt-10 px-4">
        <div className="w-full max-w-sm rounded-lg border border-border bg-card px-6 py-8 shadow-lg">
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

          {/* Title */}
          <div className="text-center mb-6">
            <h2 className="text-lg font-semibold text-foreground">Complete Your Profile</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Choose a username and display name
            </p>
          </div>

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
                id="displayName"
                type="text"
                placeholder="Display Name"
                value={displayName}
                onChange={(e) => setDisplayName(e.target.value)}
                maxLength={50}
                className="h-10"
              />
              <p className="mt-1 text-xs text-muted-foreground">
                This is how others will see you
              </p>
            </div>

            <div>
              <Input
                id="username"
                type="text"
                placeholder="Username"
                value={username}
                onChange={(e) => setUsername(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ''))}
                maxLength={30}
                className="h-10"
              />
              {username && (
                <div className="mt-2 space-y-1">
                  <ValidationCheck
                    met={usernameValidation.length}
                    label="3-30 characters"
                  />
                  <ValidationCheck
                    met={usernameValidation.format}
                    label="Lowercase letters, numbers, underscores only"
                  />
                  <ValidationCheck
                    met={isUsernameAvailable}
                    label={isUsernameAvailable ? "Username available" : "Username taken"}
                    loading={isCheckingUsername}
                  />
                </div>
              )}
            </div>

            <Button type="submit" className="w-full h-10 mt-2" disabled={isSubmitting || !isValidUsername}>
              {isSubmitting ? (
                <div className="h-5 w-5 animate-spin rounded-full border-2 border-current border-t-transparent" />
              ) : (
                'Continue to Dashboard'
              )}
            </Button>
          </form>
        </div>
      </div>
    </div>
  );
}
