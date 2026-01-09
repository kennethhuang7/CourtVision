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
      
      const [
        profileData,
        picksData,
        messagesData,
        conversationsData,
        friendshipsData,
        groupMembershipsData,
        groupsData,
        notificationsData,
        sessionsData,
      ] = await Promise.all([
        supabase.from('user_profiles').select('*').eq('id', user.id).single(),
        supabase.from('user_picks').select('*').eq('owner_id', user.id),
        supabase.from('user_messages').select('*').eq('sender_id', user.id),
        supabase.from('user_conversations').select('*').eq('user_id', user.id),
        supabase.from('user_friendships').select('*').or(`user_id.eq.${user.id},friend_id.eq.${user.id}`),
        supabase.from('user_group_members').select('*').eq('user_id', user.id),
        supabase.from('user_groups').select('*').eq('owner_id', user.id),
        supabase.from('user_notifications').select('*').eq('user_id', user.id),
        supabase.from('user_sessions').select('*').eq('user_id', user.id),
      ]);

      
      const exportData = {
        export_info: {
          exported_at: new Date().toISOString(),
          user_id: user.id,
          export_version: '1.0',
        },
        profile: profileData.data,
        picks: picksData.data || [],
        messages: messagesData.data || [],
        conversations: conversationsData.data || [],
        friendships: friendshipsData.data || [],
        group_memberships: groupMembershipsData.data || [],
        owned_groups: groupsData.data || [],
        notifications: notificationsData.data || [],
        sessions: sessionsData.data || [],
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
              <li>Profile information (username, bio, avatar, banner)</li>
              <li>All your picks (saved and active)</li>
              <li>Messages and conversations</li>
              <li>Friends and friendships</li>
              <li>Group memberships and owned groups</li>
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
