import { useEffect, useState } from 'react';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId } from '@/lib/security';
import { logger } from '@/lib/logger';


export function useEnsureUserProfile() {
  const { user } = useAuth();
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    if (!user || checked) return;

    let isMounted = true;

    const ensureProfile = async () => {
      try {
        
        const validatedUserId = validateUserId(user.id);

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

        const username =
          user.username ||
          user.email.split('@')[0] ||
          `user_${validatedUserId.slice(0, 8)}`;

        const { error: insertError } = await supabase
          .from('user_profiles')
          .upsert(
            {
              user_id: validatedUserId,
              username,
              display_name: username, 
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


