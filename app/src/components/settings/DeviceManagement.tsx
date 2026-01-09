import { useUserSessions, useDeleteSession } from '@/hooks/useSessions';
import { Button } from '@/components/ui/button';
import { Monitor, Smartphone, Globe, Loader2, X } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { cn } from '@/lib/utils';
import { toast } from 'sonner';

export function DeviceManagement() {
  const { data: sessions = [], isLoading } = useUserSessions();
  const deleteSession = useDeleteSession();

  const getDeviceIcon = (deviceType: string) => {
    if (deviceType.toLowerCase().includes('windows') || deviceType.toLowerCase().includes('mac') || deviceType.toLowerCase().includes('linux')) {
      return Monitor;
    } else if (deviceType.toLowerCase().includes('ios') || deviceType.toLowerCase().includes('android')) {
      return Smartphone;
    }
    return Globe;
  };

  const handleLogout = async (sessionId: string, deviceInfo: string) => {
    try {
      await deleteSession.mutateAsync(sessionId);
      toast.success(`Logged out from ${deviceInfo}`);
    } catch (error) {
      toast.error('Failed to logout from device');
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const currentSession = sessions.find(s => s.is_current);
  const otherSessions = sessions.filter(s => !s.is_current);

  return (
    <div className="space-y-6">
      {currentSession && (
        <div>
          <h3 className="text-sm font-medium text-muted-foreground mb-3">Current Device</h3>
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="flex items-start gap-4">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-primary/10 shrink-0">
                {(() => {
                  const Icon = getDeviceIcon(currentSession.device_type);
                  return <Icon className="h-5 w-5 text-primary" />;
                })()}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <p className="text-sm font-semibold text-foreground">
                    {currentSession.device_type} - {currentSession.client_type}
                  </p>
                  <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-primary/10 text-primary">
                    Current
                  </span>
                </div>
                <p className="text-sm text-muted-foreground">
                  {[currentSession.city, currentSession.region, currentSession.country]
                    .filter(Boolean)
                    .join(', ') || 'Location unavailable'}
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  Active now
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {otherSessions.length > 0 && (
        <div>
          <h3 className="text-sm font-medium text-muted-foreground mb-3">Other Devices</h3>
          <div className="space-y-2">
            {otherSessions.map((session) => {
              const Icon = getDeviceIcon(session.device_type);
              return (
                <div
                  key={session.id}
                  className="rounded-lg border border-border bg-card p-4 hover:bg-accent/5 transition-colors"
                >
                  <div className="flex items-start gap-4">
                    <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted shrink-0">
                      <Icon className="h-5 w-5 text-muted-foreground" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-semibold text-foreground mb-1">
                        {session.device_type} - {session.client_type}
                      </p>
                      <p className="text-sm text-muted-foreground">
                        {[session.city, session.region, session.country]
                          .filter(Boolean)
                          .join(', ') || 'Location unavailable'}
                      </p>
                      <p className="text-xs text-muted-foreground mt-1">
                        {formatDistanceToNow(new Date(session.last_active), { addSuffix: true })}
                      </p>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => handleLogout(session.id, `${session.device_type} - ${session.client_type}`)}
                      disabled={deleteSession.isPending}
                      className="shrink-0"
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {sessions.length === 0 && (
        <div className="text-center py-12 text-muted-foreground border border-dashed rounded-lg bg-secondary/30">
          <p className="text-sm">No active sessions found</p>
        </div>
      )}

      {otherSessions.length === 0 && sessions.length > 0 && (
        <div className="text-center py-8 text-muted-foreground border border-dashed rounded-lg bg-secondary/30">
          <p className="text-sm">No other devices logged in</p>
        </div>
      )}

      <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 p-4">
        <p className="text-sm text-yellow-700 dark:text-yellow-400">
          <strong>Security tip:</strong> If you see a device you don't recognize, log it out immediately and change your password.
        </p>
      </div>
    </div>
  );
}
