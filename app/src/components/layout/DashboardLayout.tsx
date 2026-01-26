import { Outlet, Navigate } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import { Sidebar } from './Sidebar';
import { TitleBar } from './TitleBar';
import { useEnsureUserProfile } from '@/hooks/useEnsureUserProfile';
import { DraggableChatWindow } from '@/components/messages/DraggableChatWindow';
import { useChatWindow } from '@/contexts/ChatWindowContext';
import { useNewPredictions } from '@/hooks/useNewPredictions';
import { useGameResultsUpdates } from '@/hooks/useGameResultsUpdates';
import { useUserPicks } from '@/hooks/useUserPicks';
import { useConversations } from '@/hooks/useConversations';
import { useFriends } from '@/hooks/useFriends';
import { useFriendRequests } from '@/hooks/useFriends';
import { useGroupInvites } from '@/hooks/useGroups';
import { SupabaseConnectionStatus } from '@/components/ui/supabase-connection-status';
import { OfflineIndicator } from '@/components/ui/offline-indicator';
import { useAutoRefreshInit } from '@/hooks/useAutoRefreshInit';
import { useErrorLoggingInit } from '@/hooks/useErrorLoggingInit';
import { useTrackCurrentSession } from '@/hooks/useSessions';
import { DiscordPresence } from '@/components/DiscordPresence';
import { cn } from '@/lib/utils';
import { useUserProfile } from '@/hooks/useUserProfile';
import { OAUTH_PENDING_PREFIX } from '@/hooks/useEnsureUserProfile';
import { SupabaseErrorPage } from '@/components/ui/supabase-error-page';

export function DashboardLayout() {
  const { isAuthenticated, isLoading, isSupabaseError, retryAuth, user } = useAuth();
  const { isVisible, close } = useChatWindow();
  const { data: userProfile, isLoading: isProfileLoading } = useUserProfile();
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    return localStorage.getItem('sidebar-collapsed') === 'true';
  });
  useEnsureUserProfile();

  const needsProfileCompletion = userProfile?.username?.startsWith(OAUTH_PENDING_PREFIX) ?? false;

  useEffect(() => {
    const handleSidebarCollapse = (e: CustomEvent<{ isCollapsed: boolean }>) => {
      setSidebarCollapsed(e.detail.isCollapsed);
    };
    window.addEventListener('sidebar-collapse', handleSidebarCollapse as EventListener);
    return () => {
      window.removeEventListener('sidebar-collapse', handleSidebarCollapse as EventListener);
    };
  }, []);
  useAutoRefreshInit(); 
  useErrorLoggingInit(); 
  useTrackCurrentSession(); 

  
  
  useNewPredictions();
  useGameResultsUpdates();
  useUserPicks(); 
  useConversations(); 
  useFriends(); 
  useFriendRequests(); 
  useGroupInvites(); 

  if (isSupabaseError) {
    return <SupabaseErrorPage onRetry={retryAuth} />;
  }

  if (isLoading || isProfileLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="flex flex-col items-center gap-4">
          <div className="h-12 w-12 shrink-0 animate-spin rounded-full border-4 border-primary border-t-transparent" />
          <p className="text-muted-foreground whitespace-nowrap">Loading...</p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }


  if (user && !user.emailConfirmed) {
    return <Navigate to="/verify-email" replace />;
  }

  if (needsProfileCompletion === true) {
    return <Navigate to="/complete-profile" replace />;
  }

  return (
    <div className="flex h-screen flex-col bg-background overflow-hidden">
      <DiscordPresence />
      <TitleBar />
      <div className="flex flex-1 pt-10 overflow-hidden">
        <Sidebar />
        <main className={cn(
          "flex-1 density-padding overflow-y-auto transition-all duration-300 ease-in-out",
          sidebarCollapsed ? "ml-[4.5rem]" : "ml-64"
        )}>
          <SupabaseConnectionStatus />
          <OfflineIndicator />
          <Outlet />
        </main>
      </div>
      <DraggableChatWindow isVisible={isVisible} onClose={close} />
    </div>
  );
}
