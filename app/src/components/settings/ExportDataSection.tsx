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

      const userMessages = await supabase.from('user_messages').select('id').eq('sender_id', user.id);
      const userMessageIds = userMessages.data?.map(m => m.id) || [];

      const userGroupMessages = await supabase.from('group_messages').select('id').eq('sender_id', user.id);
      const userGroupMessageIds = userGroupMessages.data?.map(m => m.id) || [];

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
        messageReactionsByUserData,
        messageReactionsToUserMessagesData,
        pickGroupSharesData,
        pickUserSharesData,
      ] = await Promise.all([
        supabase.from('user_profiles').select('*').eq('user_id', user.id).single(),
        supabase.from('user_picks').select('*').eq('owner_id', user.id),
        supabase.from('user_messages').select('*').eq('sender_id', user.id),
        supabase.from('user_conversations').select('*').eq('user_id', user.id),
        supabase.from('user_friendships').select('*').or(`requester_id.eq.${user.id},addressee_id.eq.${user.id}`),
        supabase.from('user_group_members').select('*').eq('user_id', user.id),
        supabase.from('user_groups').select('*').eq('owner_id', user.id),
        supabase.from('group_messages').select('*').eq('sender_id', user.id),
        supabase.from('user_notifications').select('*').eq('user_id', user.id),
        supabase.from('user_sessions').select('*').eq('user_id', user.id),
        supabase.from('user_group_invites').select('*').or(`inviter_id.eq.${user.id},invitee_id.eq.${user.id}`),
        supabase.from('message_reactions').select('*').eq('user_id', user.id),
        [...userMessageIds, ...userGroupMessageIds].length > 0
          ? supabase.from('message_reactions').select('*').in('message_id', [...userMessageIds, ...userGroupMessageIds])
          : Promise.resolve({ data: [] }),
        userPickIds.length > 0
          ? supabase.from('pick_group_shares').select('*').in('pick_id', userPickIds)
          : Promise.resolve({ data: [] }),
        userPickIds.length > 0
          ? supabase.from('pick_user_shares').select('*').or(`pick_id.in.(${userPickIds.join(',')}),shared_with_user_id.eq.${user.id}`)
          : Promise.resolve({ data: [] }),
      ]);

      
      const allReactions = [
        ...(messageReactionsByUserData.data || []),
        ...(messageReactionsToUserMessagesData.data || []),
      ];
      const uniqueReactions = Array.from(
        new Map(allReactions.map(r => [r.id, r])).values()
      );

      const exportData = {
        export_info: {
          exported_at: new Date().toISOString(),
          user_id: user.id,
          export_version: '2.1',
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
        message_reactions: uniqueReactions,
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
              <li>Profile information (username, display name, bio, avatar, banner, all settings)</li>
              <li>All your picks (all picks you created, including inactive ones)</li>
              <li>All direct messages and group messages you sent</li>
              <li>All conversations you participated in</li>
              <li>All message reactions you created, plus reactions to your messages</li>
              <li>All friendships (both requests you sent and received)</li>
              <li>All group memberships and groups you own</li>
              <li>All group invites you sent or received</li>
              <li>All pick shares (picks you shared and picks shared with you)</li>
              <li>All notification history</li>
              <li>All active sessions</li>
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

      <div className="rounded-lg border border-primary/20 bg-primary/5 p-4">
        <p className="text-sm text-primary">
          <strong>Note:</strong> Your data will be exported as a JSON file. This file contains all your personal information and should be stored securely.
        </p>
      </div>
    </div>
  );
}
