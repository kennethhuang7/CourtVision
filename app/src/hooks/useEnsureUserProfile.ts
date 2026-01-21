import { useEffect, useState } from 'react';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId } from '@/lib/security';
import { logger } from '@/lib/logger';

// Prefix for OAuth users who haven't completed their profile
export const OAUTH_PENDING_PREFIX = '__oauth_pending_';

export function useEnsureUserProfile() {
  const { user } = useAuth();
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    if (!user || checked) return;

    let isMounted = true;

    const ensureProfile = async () => {
      try {
        const validatedUserId = validateUserId(user.id);

        // Check if profile already exists
        const { data, error } = await supabase
          .from('user_profiles')
          .select('*')
          .eq('user_id', validatedUserId)
          .limit(1)
          .maybeSingle();

        if (error && error.code !== 'PGRST116') {
          logger.error('Error checking user profile', error as Error);
          return;
        }

        if (data) {
          return;
        }

        // Check if user signed in via OAuth
        const { data: { user: authUser } } = await supabase.auth.getUser();
        const identities = authUser?.identities || [];
        const isOAuthUser = identities.some(
          (id: any) => id.provider === 'google' || id.provider === 'discord'
        );

        let username: string;
        let displayName: string;

        if (isOAuthUser) {
          // OAuth users get a pending username - they'll set their real one in CompleteProfile
          username = `${OAUTH_PENDING_PREFIX}${validatedUserId.slice(0, 8)}`;
          // Use their OAuth display name if available, otherwise pending
          displayName = authUser?.user_metadata?.full_name ||
                        authUser?.user_metadata?.name ||
                        authUser?.user_metadata?.preferred_username ||
                        username;
        } else {
          // Regular users - use their chosen username from registration
          const baseUsername = (
            user.username ||
            user.email.split('@')[0] ||
            `user_${validatedUserId.slice(0, 8)}`
          ).toLowerCase().replace(/[^a-z0-9_]/g, '').slice(0, 25);

          username = baseUsername;
          displayName = username;

          // Try to create profile, retry with suffix if username taken
          let attempts = 0;
          const maxAttempts = 5;

          while (attempts < maxAttempts) {
            const { error: insertError } = await supabase
              .from('user_profiles')
              .upsert(
                {
                  user_id: validatedUserId,
                  username,
                  display_name: displayName,
                },
                { onConflict: 'user_id' }
              );

            if (!insertError) {
              return; // Success
            }

            if (insertError.code === '23505' && insertError.message?.includes('username')) {
              const randomSuffix = Math.floor(Math.random() * 10000).toString().padStart(4, '0');
              username = `${baseUsername.slice(0, 25)}_${randomSuffix}`;
              displayName = username;
              attempts++;
              logger.warn('Username taken, retrying with suffix', { attempt: attempts, username });
            } else {
              logger.error('Error creating user profile', insertError as Error);
              return;
            }
          }

          // Fallback
          username = `user_${validatedUserId.slice(0, 8)}`;
          displayName = username;
        }

        // Create the profile
        const { error: insertError } = await supabase
          .from('user_profiles')
          .upsert(
            {
              user_id: validatedUserId,
              username,
              display_name: displayName,
            },
            { onConflict: 'user_id' }
          );

        if (insertError) {
          logger.error('Error creating user profile', insertError as Error);
        }
      } finally {
        if (isMounted) {
          setChecked(true);
        }
      }
    };

    ensureProfile();

    return () => {
      isMounted = false;
    };
  }, [user, checked]);
}


