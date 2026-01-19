import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { logger } from '@/lib/logger';

export interface Notification {
  id: string;
  user_id: string;
  type: string;
  title: string;
  message: string;
  read: boolean;
  created_at: string;
  related_id: string | null;
  related_type: string | null;
  action_url: string | null;
  icon: string | null;
}


export function useRecentNotifications(limit = 15) {
  const { user } = useAuth();

  return useQuery({
    queryKey: ['notifications', 'recent', user?.id, limit],
    queryFn: async () => {
      if (!user?.id) return [];

      const { data, error } = await supabase
        .from('user_notifications')
        .select('*')
        .eq('user_id', user.id)
        .order('created_at', { ascending: false })
        .limit(limit);

      if (error) {
        logger.error('Error fetching recent notifications', error);
        throw error;
      }

      return data as Notification[];
    },
    enabled: !!user?.id,
    staleTime: 30000, 
    refetchInterval: 30000,
  });
}


export function useUnreadNotificationCount() {
  const { user } = useAuth();

  return useQuery({
    queryKey: ['notifications', 'unread-count', user?.id],
    queryFn: async () => {
      if (!user?.id) return 0;

      const { count, error } = await supabase
        .from('user_notifications')
        .select('*', { count: 'exact', head: true })
        .eq('user_id', user.id)
        .eq('read', false);

      if (error) {
        logger.error('Error fetching unread notification count', error);
        throw error;
      }

      return count || 0;
    },
    enabled: !!user?.id,
    staleTime: 10000, 
    refetchInterval: 10000,
  });
}


export function useMarkNotificationAsRead() {
  const queryClient = useQueryClient();
  const { user } = useAuth();

  return useMutation({
    mutationFn: async (notificationId: string) => {
      const { error } = await supabase
        .from('user_notifications')
        .update({ read: true })
        .eq('id', notificationId);

      if (error) throw error;

      return notificationId;
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['notifications', 'recent', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'unread-count', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'all', user?.id] });
    },
    onError: (error) => {
      logger.error('Error marking notification as read', error);
    },
  });
}


export function useMarkAllNotificationsAsRead() {
  const queryClient = useQueryClient();
  const { user } = useAuth();

  return useMutation({
    mutationFn: async () => {
      if (!user?.id) throw new Error('User not authenticated');

      const { error } = await supabase
        .from('user_notifications')
        .update({ read: true })
        .eq('user_id', user.id)
        .eq('read', false);

      if (error) throw error;
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['notifications', 'recent', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'unread-count', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'all', user?.id] });
    },
    onError: (error) => {
      logger.error('Error marking all notifications as read', error);
    },
  });
}


export function useDeleteNotification() {
  const queryClient = useQueryClient();
  const { user } = useAuth();

  return useMutation({
    mutationFn: async (notificationId: string) => {
      const { error } = await supabase
        .from('user_notifications')
        .delete()
        .eq('id', notificationId);

      if (error) throw error;

      return notificationId;
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['notifications', 'recent', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'unread-count', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'all', user?.id] });
    },
    onError: (error) => {
      logger.error('Error deleting notification', error);
    },
  });
}


export function useDeleteAllNotifications() {
  const queryClient = useQueryClient();
  const { user } = useAuth();

  return useMutation({
    mutationFn: async () => {
      if (!user?.id) throw new Error('User not authenticated');

      const { error } = await supabase
        .from('user_notifications')
        .delete()
        .eq('user_id', user.id);

      if (error) throw error;
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['notifications', 'recent', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'unread-count', user?.id] });
      queryClient.invalidateQueries({ queryKey: ['notifications', 'all', user?.id] });
    },
    onError: (error) => {
      logger.error('Error deleting all notifications', error);
    },
  });
}


export function useAllNotifications(page = 0, pageSize = 50) {
  const { user } = useAuth();

  return useQuery({
    queryKey: ['notifications', 'all', user?.id, page, pageSize],
    queryFn: async () => {
      if (!user?.id) return { data: [], count: 0 };

      const from = page * pageSize;
      const to = from + pageSize - 1;

      const { data, error, count } = await supabase
        .from('user_notifications')
        .select('*', { count: 'exact' })
        .eq('user_id', user.id)
        .order('created_at', { ascending: false })
        .range(from, to);

      if (error) {
        logger.error('Error fetching all notifications', error);
        throw error;
      }

      return {
        data: data as Notification[],
        count: count || 0,
      };
    },
    enabled: !!user?.id,
    staleTime: 30000,
  });
}
