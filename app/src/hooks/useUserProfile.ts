import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId } from '@/lib/security';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

export interface UserProfile {
  user_id: string;
  username: string;
  display_name: string | null;
  profile_picture_url: string | null;
  banner_url: string | null;
  about_me: string | null;
  discord_rich_presence_enabled: boolean | null;
  error_logging_enabled: boolean | null;
  created_at: string;
  updated_at: string;
}

export function useUserProfile() {
  const { user } = useAuth();

  return useQuery<UserProfile | null, Error>({
    queryKey: ['user-profile', user?.id],
    queryFn: async () => {
      if (!user?.id) return null;

      
      const validatedUserId = validateUserId(user.id);

      const { data, error } = await supabase
        .from('user_profiles')
        .select('*')
        .eq('user_id', validatedUserId)
        .single();

      if (error) {
        if (error.code === 'PGRST116') {
          
          return null;
        }
        throw error;
      }

      return data as UserProfile;
    },
    enabled: !!user?.id,
  });
}

export function useUpdateUserProfile() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (updates: Partial<UserProfile>) => {
      if (!user?.id) throw new Error('User not authenticated');

      
      const validatedUserId = validateUserId(user.id);

      
      const { user_id, created_at, ...allowedUpdates } = updates;
      
      
      const updateableFields: (keyof UserProfile)[] = [
        'username',
        'display_name',
        'profile_picture_url',
        'banner_url',
        'about_me',
        'discord_rich_presence_enabled',
        'default_time_window',
        'default_stat',
        'default_confidence_filter',
        'auto_refresh_interval',
        'error_logging_enabled',
      ];
      
      const sanitizedUpdates: Partial<UserProfile> = {};
      for (const field of updateableFields) {
        if (field in allowedUpdates) {
          sanitizedUpdates[field] = allowedUpdates[field];
        }
      }

      const { data, error } = await supabase
        .from('user_profiles')
        .update({
          ...sanitizedUpdates,
          updated_at: new Date().toISOString(),
        })
        .eq('user_id', validatedUserId)
        .select()
        .single();

      if (error) throw error;
      return data as UserProfile;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['user-profile', user?.id] });
      toast.success('Profile updated successfully');
    },
    onError: (error: any) => {
      logger.error('Error updating profile', error as Error);
      if (error.code === '23505') {
        
        if (error.message.includes('username')) {
          toast.error('Username is already taken');
        } else {
          toast.error('A unique constraint was violated');
        }
      } else {
        toast.error('Failed to update profile');
      }
    },
  });
}

export function useCheckUsernameAvailability() {
  const { user } = useAuth();
  
  return useMutation({
    mutationFn: async (username: string): Promise<boolean> => {
      if (!user?.id) throw new Error('User not authenticated');
      
      
      const validatedUserId = validateUserId(user.id);
      
      
      if (!username || typeof username !== 'string' || username.trim().length === 0) {
        throw new Error('Username cannot be empty');
      }
      if (username.length > 50) {
        throw new Error('Username is too long');
      }
      
      const { data, error } = await supabase.rpc('check_username_availability', {
        p_username: username.trim(),
        p_current_user_id: validatedUserId,
      });

      if (error) throw error;
      return data;
    },
  });
}

