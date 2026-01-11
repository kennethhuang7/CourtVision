import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Download, Loader2, CheckCircle2 } from 'lucide-react';
import { supabase } from '@/lib/supabase';
import { useAuth } from '@/contexts/AuthContext';
import { toast } from 'sonner';
import { logger } from '@/lib/logger';

export function ExportDataSection() {
  const { user } = useAuth();
  const [isExporting, setIsExporting] = useState(false);
  const [exportComplete, setExportComplete] = useState(false);

  const handleExportData = async () => {
    if (!user?.id) {
      toast.error('You must be logged in to export data');
      return;
    }

    setIsExporting(true);
    setExportComplete(false);

    try {
      
      const userPicks = await supabase.from('user_picks').select('id').eq('owner_id', user.id);
      const userPickIds = userPicks.data?.map(p => p.id) || [];

      const [
        profileData,
        picksData,
        messagesData,
        conversationsData,
        friendshipsData,
        groupMembershipsData,
        groupsData,
        groupMessagesData,
        notificationsData,
        sessionsData,
        groupInvitesData,
        messageReactionsData,
        pickGroupSharesData,
        pickUserSharesData,
      ] = await Promise.all([
        supabase.from('user_profiles').select('*').eq('user_id', user.id).single(),
        Promise.resolve({ data: userPicks.data }),
        supabase.from('user_messages').select('*').eq('sender_id', user.id),
        supabase.from('user_conversations').select('*').eq('user_id', user.id),
        supabase.from('user_friendships').select('*').or(`user_id.eq.${user.id},friend_id.eq.${user.id}`),
        supabase.from('user_group_members').select('*').eq('user_id', user.id),
        supabase.from('user_groups').select('*').eq('owner_id', user.id),
        supabase.from('group_messages').select('*').eq('sender_id', user.id),
        supabase.from('user_notifications').select('*').eq('user_id', user.id),
        supabase.from('user_sessions').select('*').eq('user_id', user.id),
        supabase.from('user_group_invites').select('*').or(`inviter_id.eq.${user.id},invitee_id.eq.${user.id}`),
        supabase.from('message_reactions').select('*').eq('user_id', user.id),
        userPickIds.length > 0
          ? supabase.from('pick_group_shares').select('*').in('pick_id', userPickIds)
          : Promise.resolve({ data: [] }),
        userPickIds.length > 0
          ? supabase.from('pick_user_shares').select('*').or(`pick_id.in.(${userPickIds.join(',')}),shared_with_user_id.eq.${user.id}`)
          : Promise.resolve({ data: [] }),
      ]);

      
      const exportData = {
        export_info: {
          exported_at: new Date().toISOString(),
          user_id: user.id,
          export_version: '2.0',
        },
        profile: profileData.data,
        picks: picksData.data || [],
        messages: messagesData.data || [],
        conversations: conversationsData.data || [],
        friendships: friendshipsData.data || [],
        group_memberships: groupMembershipsData.data || [],
        owned_groups: groupsData.data || [],
        group_messages: groupMessagesData.data || [],
        notifications: notificationsData.data || [],
        sessions: sessionsData.data || [],
        group_invites: groupInvitesData.data || [],
        message_reactions: messageReactionsData.data || [],
        pick_group_shares: pickGroupSharesData.data || [],
        pick_user_shares: pickUserSharesData.data || [],
      };

      
      const jsonString = JSON.stringify(exportData, null, 2);
      const blob = new Blob([jsonString], { type: 'application/json' });

      
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `courtvision-data-export-${new Date().toISOString().split('T')[0]}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);

      setExportComplete(true);
      toast.success('Your data has been exported successfully');

      
      setTimeout(() => {
        setExportComplete(false);
      }, 3000);
    } catch (error) {
      logger.error('Error exporting user data', error);
      toast.error('Failed to export data');
    } finally {
      setIsExporting(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-border bg-card p-4">
        <div className="space-y-3">
          <div>
            <p className="text-sm text-foreground font-medium mb-1">Data Included in Export:</p>
            <ul className="text-sm text-muted-foreground space-y-1 list-disc list-inside">
              <li>Profile information (username, display name, bio, avatar, banner, settings)</li>
              <li>All your picks (saved and active)</li>
              <li>Direct messages, group messages, conversations, and message reactions</li>
              <li>Friends and friendships</li>
              <li>Group memberships, owned groups, and group invites</li>
              <li>Pick shares (shared to groups and other users)</li>
              <li>Notification history</li>
              <li>Active sessions</li>
            </ul>
          </div>

          <div className="pt-2">
            <Button
              onClick={handleExportData}
              disabled={isExporting}
              className="w-full gap-2"
            >
              {isExporting ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Preparing Export...
                </>
              ) : exportComplete ? (
                <>
                  <CheckCircle2 className="h-4 w-4" />
                  Export Complete
                </>
              ) : (
                <>
                  <Download className="h-4 w-4" />
                  Download My Data
                </>
              )}
            </Button>
          </div>
        </div>
      </div>

      <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 p-4">
        <p className="text-sm text-blue-700 dark:text-blue-400">
          <strong>Note:</strong> Your data will be exported as a JSON file. This file contains all your personal information and should be stored securely.
        </p>
      </div>
    </div>
  );
}
