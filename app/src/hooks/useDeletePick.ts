import { useMutation, useQueryClient } from '@tanstack/react-query';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { validateUserId, validateUUID } from '@/lib/security';

export function useDeletePick() {
  const { user } = useAuth();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (pickId: string) => {
      if (!user) {
        throw new Error('You must be logged in to delete a pick.');
      }

      
      const validatedUserId = validateUserId(user.id);
      const validatedPickId = validateUUID(pickId, 'Pick ID');

      
      const { error } = await supabase
        .from('user_picks')
        .update({ is_active: false })
        .eq('id', validatedPickId)
        .eq('owner_id', validatedUserId); 

      if (error) {
        throw error;
      }
    },
    onSuccess: () => {
      
      queryClient.invalidateQueries({ queryKey: ['userPicks', user?.id] });
    },
  });
}

